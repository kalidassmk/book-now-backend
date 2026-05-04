"""
tasks.py
─────────────────────────────────────────────────────────────────────────────
Definitions of the sentiment-engine subprocesses the python-engine
should supervise.

Mirrors ``binance-sentiment-engine/start_utilities.py``'s ``UTILITIES``
list. Three task kinds:

  - SETUP        run-once at boot, await completion before persistent
                 tasks start (e.g. fee_calculator_util.py seeds a
                 Redis key the others read).
  - PERSISTENT   long-running daemon; supervisor restarts on death
                 with a configurable backoff.
  - SCHEDULED    fire-and-await, sleep ``interval_s``, repeat. Used
                 for hourly refreshers like sync_symbols.py.

These run as subprocesses (matching the existing layout) so we don't
have to refactor 3,388 lines of analyzer code on this branch. Any
analyzer that wants to migrate in-process can drop its module under
``booknow.sentiment`` and we delete its entry here in a follow-up.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple


@dataclass(frozen=True)
class SubprocessTask:
    """One sentiment-engine subprocess.

    ``cmd_argv`` is ``shlex.split``-ready and includes the script name
    plus any flags. The supervisor prepends the project's Python
    interpreter and runs with the sentiment-engine directory as cwd.
    """

    name: str
    cmd_argv: Tuple[str, ...]
    kind: str  # "setup" | "persistent" | "scheduled"
    interval_s: float = 0.0          # only for scheduled
    restart_delay_s: float = 5.0     # for persistent

    def __post_init__(self) -> None:
        if self.kind not in ("setup", "persistent", "scheduled"):
            raise ValueError(f"unknown task kind: {self.kind}")
        if self.kind == "scheduled" and self.interval_s <= 0:
            raise ValueError(f"{self.name}: scheduled task needs interval_s > 0")


# Mirrors start_utilities.py exactly so we don't lose any behaviour.
# Tuple form so subclassing/mutation accidents don't bite us.
SENTIMENT_TASKS: List[SubprocessTask] = [
    # ── Setup (sequential, run before persistent tasks) ──────────────
    SubprocessTask(
        name="Fee Intelligence",
        cmd_argv=("fee_calculator_util.py",),
        kind="setup",
    ),

    # ── Scheduled refreshers ─────────────────────────────────────────
    SubprocessTask(
        name="Symbol Sync",
        cmd_argv=("sync_symbols.py",),
        kind="scheduled",
        interval_s=3600,  # every 1 hour
    ),

    # ── Persistent analyzers (parallel, restart on death) ────────────
    SubprocessTask(
        name="Market Scanner",
        cmd_argv=("market_sentiment_engine.py",),
        kind="persistent",
    ),
    SubprocessTask(
        name="Fast Move Analyzer",
        cmd_argv=("volume_price_analyzer.py", "--daemon"),
        kind="persistent",
    ),
    SubprocessTask(
        name="Fast Scalper",
        cmd_argv=("ultra_fast_scalper.py",),
        kind="persistent",
    ),
    SubprocessTask(
        name="Profit Analyzer",
        cmd_argv=("profit_reached_analyzer.py",),
        kind="persistent",
    ),
    SubprocessTask(
        name="Pattern Recorder",
        cmd_argv=("success_pattern_recorder.py",),
        kind="persistent",
    ),
    SubprocessTask(
        name="Pattern Matcher",
        cmd_argv=("pattern_matching_engine.py",),
        kind="persistent",
    ),
    SubprocessTask(
        name="Profit Trend",
        cmd_argv=("profit_020_trend_analyzer.py",),
        kind="persistent",
    ),
    SubprocessTask(
        name="Virtual Scalper",
        cmd_argv=("virtual_scalp_executor.py",),
        kind="persistent",
    ),
]
