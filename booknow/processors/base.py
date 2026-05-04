"""
base.py
─────────────────────────────────────────────────────────────────────────────
Async base class for the four processor loops.

Each Java processor was a ``Runnable`` thread with a ``while
!interrupted: try { tick(); sleep(N) } catch …``. The Python version
collapses that boilerplate into one shared coroutine so each subclass
only has to implement ``async def _tick(self)``.

Key differences from the Java thread:
  - asyncio task instead of OS thread (cheap, no pool exhaustion)
  - graceful cancellation via ``asyncio.Task.cancel()``
  - errors don't kill the loop; they sleep ``error_sleep_s`` and retry
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional


class AsyncProcessor:
    """Base class for booknow.processors.* tasks.

    Subclasses implement ``_tick()`` (one iteration of the loop).
    The base class supervises lifecycle, logging, and error recovery.
    """

    name: str = "processor"
    sleep_s: float = 0.5
    error_sleep_s: float = 1.0

    def __init__(self):
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self.log = logging.getLogger(f"booknow.{self.name}")

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_forever(), name=self.name)
        self.log.info("[%s] task spawned (tick %.2fs)", self.name, self.sleep_s)

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

    async def _run_forever(self) -> None:
        while self._running:
            try:
                await self._tick()
                await asyncio.sleep(self.sleep_s)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log.error("[%s] tick error: %s", self.name, e, exc_info=True)
                try:
                    await asyncio.sleep(self.error_sleep_s)
                except asyncio.CancelledError:
                    break

    async def _tick(self) -> None:  # pragma: no cover - abstract
        raise NotImplementedError
