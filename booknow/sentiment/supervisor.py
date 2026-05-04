"""
supervisor.py
─────────────────────────────────────────────────────────────────────────────
Async supervisor for the binance-sentiment-engine subprocesses.

Boots every entry from ``SENTIMENT_TASKS``:

  - Setup tasks run sequentially and we await their exit before
    spawning anything else (so e.g. fee_calculator_util.py finishes
    seeding Redis before the analyzers start reading).
  - Persistent tasks run in parallel; on any non-zero exit, sleep
    ``restart_delay_s`` and respawn.
  - Scheduled tasks run, exit, sleep ``interval_s``, repeat.

stdout/stderr from each child is teed line-by-line into the engine's
own logger with a ``[<task name>]`` prefix so everything ends up in
one log stream.

Graceful shutdown: SIGINT first; if a child doesn't exit within 5 s,
SIGTERM; another 3 s and SIGKILL. The supervisor itself awaits all
children before returning from ``stop()`` so the engine's main
shutdown sequence stays orderly.
"""

from __future__ import annotations

import asyncio
import logging
import re
import signal
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Sequence, TextIO

from booknow.sentiment.tasks import SubprocessTask


logger = logging.getLogger("booknow.sentiment")

# Grace windows for shutdown signal escalation. asyncio-based scripts
# (volume_price_analyzer --daemon, profit_020_trend_analyzer, etc.) can
# take a few seconds to drain their event loop on SIGINT, so we give
# them generous time before escalating to SIGTERM/SIGKILL.
_SIGINT_GRACE_S  = 8.0
_SIGTERM_GRACE_S = 4.0


def _sanitize_name(name: str) -> str:
    """Map a human task name to a safe filename stem.

    "Fee Intelligence" -> "fee_intelligence"
    "Fast Move Analyzer" -> "fast_move_analyzer"
    """
    s = name.strip().lower()
    s = re.sub(r"[^\w]+", "_", s)
    return s.strip("_") or "task"


class SentimentSupervisor:
    """Boot + supervise every subprocess in the given task list."""

    def __init__(
        self,
        *,
        tasks: Sequence[SubprocessTask],
        sentiment_dir: Path,
        python_executable: Optional[str] = None,
        log_dir: Optional[Path] = None,
    ):
        self._tasks = list(tasks)
        self._dir = sentiment_dir
        self._python = python_executable or sys.executable
        self._supervisors: List[asyncio.Task] = []
        self._processes: Dict[str, asyncio.subprocess.Process] = {}
        # Per-task log files. Opened lazily on first run, kept open across
        # restarts so a tail -f keeps showing data through respawns.
        self._log_dir: Optional[Path] = log_dir
        self._log_files: Dict[str, TextIO] = {}
        if self._log_dir is not None:
            self._log_dir.mkdir(parents=True, exist_ok=True)
        self._running = False

    # ── Lifecycle ────────────────────────────────────────────────────────

    async def start(self) -> None:
        if self._running:
            return
        if not self._dir.exists():
            logger.warning(
                "[sentiment] dir %s does not exist — skipping supervisor",
                self._dir,
            )
            return

        self._running = True
        logger.info(
            "[sentiment] supervisor starting from %s with %d tasks",
            self._dir, len(self._tasks),
        )

        # Setup tasks first — sequential.
        setups = [t for t in self._tasks if t.kind == "setup"]
        for task in setups:
            if not self._running:
                return
            try:
                rc = await self._run_once(task)
                if rc == 0:
                    logger.info("[sentiment:%s] setup complete", task.name)
                else:
                    logger.error("[sentiment:%s] setup exited rc=%d (continuing)", task.name, rc)
            except asyncio.CancelledError:
                return

        # Persistent + scheduled in parallel.
        for task in self._tasks:
            if task.kind == "persistent":
                self._supervisors.append(
                    asyncio.create_task(self._supervise_persistent(task), name=f"sent-{task.name}")
                )
            elif task.kind == "scheduled":
                self._supervisors.append(
                    asyncio.create_task(self._supervise_scheduled(task), name=f"sent-{task.name}")
                )

        names = [t.name for t in self._tasks if t.kind in ("persistent", "scheduled")]
        logger.info("[sentiment] supervising %d running tasks: %s", len(names), ", ".join(names))

    async def stop(self) -> None:
        self._running = False
        # Cancel supervisor coroutines first so they stop respawning.
        for s in self._supervisors:
            s.cancel()
        # Send SIGINT to every live child for graceful exit.
        for name, proc in list(self._processes.items()):
            if proc.returncode is None:
                logger.info("[sentiment:%s] stopping (SIGINT)", name)
                try:
                    proc.send_signal(signal.SIGINT)
                except ProcessLookupError:
                    continue
        # Drain everything with a deadline.
        await self._await_or_kill(grace_s=_SIGINT_GRACE_S, signal_to_send=signal.SIGTERM)
        await self._await_or_kill(grace_s=_SIGTERM_GRACE_S, signal_to_send=signal.SIGKILL)
        # Final reap of supervisor tasks.
        for s in self._supervisors:
            try:
                await s
            except (asyncio.CancelledError, Exception):
                pass
        self._supervisors.clear()
        self._processes.clear()
        # Close all per-task log files so the OS flushes their buffers.
        for name, f in list(self._log_files.items()):
            try:
                f.close()
            except Exception:
                pass
        self._log_files.clear()
        logger.info("[sentiment] supervisor stopped")

    # ── Task supervision strategies ──────────────────────────────────────

    async def _supervise_persistent(self, task: SubprocessTask) -> None:
        while self._running:
            try:
                rc = await self._run_once(task)
                if not self._running:
                    return
                logger.warning(
                    "[sentiment:%s] exited rc=%d — restarting in %.1fs",
                    task.name, rc, task.restart_delay_s,
                )
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error("[sentiment:%s] supervisor error: %s", task.name, e, exc_info=True)
            try:
                await asyncio.sleep(task.restart_delay_s)
            except asyncio.CancelledError:
                return

    async def _supervise_scheduled(self, task: SubprocessTask) -> None:
        while self._running:
            try:
                rc = await self._run_once(task)
                if rc != 0 and self._running:
                    logger.warning("[sentiment:%s] scheduled run rc=%d", task.name, rc)
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error("[sentiment:%s] supervisor error: %s", task.name, e, exc_info=True)
            try:
                await asyncio.sleep(task.interval_s)
            except asyncio.CancelledError:
                return

    # ── Single-run + tee ──────────────────────────────────────────────────

    async def _run_once(self, task: SubprocessTask) -> int:
        cmd = [self._python] + list(task.cmd_argv)
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=str(self._dir),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
        except FileNotFoundError as e:
            logger.error(
                "[sentiment:%s] cannot spawn (%s) — is %s present?",
                task.name, e, " ".join(cmd),
            )
            return -1
        self._processes[task.name] = proc
        log_file = self._open_log_file(task.name)
        if log_file is not None:
            log_file.write(
                f"\n──── {time.strftime('%Y-%m-%d %H:%M:%S')} "
                f"launched pid={proc.pid} ────\n"
            )
            log_file.flush()
        logger.info(
            "[sentiment:%s] launched (pid=%d%s)",
            task.name, proc.pid,
            f", log={self._log_path_for(task.name)}" if self._log_dir else "",
        )

        # Tee output line-by-line.
        reader_task = asyncio.create_task(
            self._tee(task.name, proc, log_file),
            name=f"sent-out-{task.name}",
        )
        try:
            rc = await proc.wait()
        finally:
            # Make sure we drain any remaining output before returning.
            try:
                await asyncio.wait_for(reader_task, timeout=2.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                reader_task.cancel()
            self._processes.pop(task.name, None)
            if log_file is not None:
                log_file.write(
                    f"──── {time.strftime('%Y-%m-%d %H:%M:%S')} "
                    f"exited rc={rc} ────\n"
                )
                log_file.flush()
        return rc

    async def _tee(
        self,
        name: str,
        proc: asyncio.subprocess.Process,
        log_file: Optional[TextIO],
    ) -> None:
        if proc.stdout is None:
            return
        while True:
            try:
                line = await proc.stdout.readline()
            except asyncio.CancelledError:
                return
            if not line:
                return
            text = line.decode("utf-8", errors="replace").rstrip()
            if not text:
                continue
            # Main engine log: one prefixed line per child line, same as before.
            logger.info("[%s] %s", name, text)
            # Per-task file: timestamp + raw line so it grep-friendly.
            if log_file is not None:
                try:
                    log_file.write(f"{time.strftime('%H:%M:%S')} {text}\n")
                    log_file.flush()
                except Exception as e:
                    logger.warning(
                        "[sentiment:%s] log file write failed: %s — disabling for this run",
                        name, e,
                    )
                    log_file = None

    def _open_log_file(self, name: str) -> Optional[TextIO]:
        """Return the per-task log handle, opening it the first time we see ``name``.

        Reusing the same handle across respawns means a ``tail -f`` keeps
        showing data even when the supervisor restarts a crashed child.
        """
        if self._log_dir is None:
            return None
        existing = self._log_files.get(name)
        if existing is not None and not existing.closed:
            return existing
        path = self._log_path_for(name)
        try:
            f = open(path, "a", encoding="utf-8", buffering=1)  # line-buffered
        except Exception as e:
            logger.warning(
                "[sentiment:%s] cannot open per-task log %s: %s — main log only",
                name, path, e,
            )
            return None
        self._log_files[name] = f
        return f

    def _log_path_for(self, name: str) -> Path:
        assert self._log_dir is not None
        return self._log_dir / f"{_sanitize_name(name)}.log"

    async def _await_or_kill(self, grace_s: float, signal_to_send: signal.Signals) -> None:
        """Wait `grace_s` for live children to exit; escalate the rest."""
        deadline = asyncio.get_event_loop().time() + grace_s
        while self._processes:
            now = asyncio.get_event_loop().time()
            remaining = max(0.0, deadline - now)
            if remaining <= 0:
                break
            for name, proc in list(self._processes.items()):
                if proc.returncode is not None:
                    self._processes.pop(name, None)
            if not self._processes:
                return
            try:
                await asyncio.sleep(min(0.25, remaining))
            except asyncio.CancelledError:
                break

        # Anyone still running gets the next signal.
        for name, proc in list(self._processes.items()):
            if proc.returncode is None:
                logger.warning(
                    "[sentiment:%s] still alive after grace — sending %s",
                    name, signal_to_send.name,
                )
                try:
                    proc.send_signal(signal_to_send)
                except ProcessLookupError:
                    self._processes.pop(name, None)
