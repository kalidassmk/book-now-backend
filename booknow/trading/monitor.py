"""
monitor.py
─────────────────────────────────────────────────────────────────────────────
Unified position monitor. Direct port of TrailingStopLossProcessor.java.

Runs every second and, for every open position:

  1. Reads the latest ``CURRENT_PRICE`` for that symbol from Redis.
  2. Updates the trailing-stop-loss high-water mark; if price has
     dropped past the configured TSL %, fires
     ``executor.force_market_exit(reason="TSL")``.
  3. Otherwise, checks the max-hold timer; if the position has been
     open longer than ``max_hold_seconds``, fires
     ``executor.force_market_exit(reason="MAX_HOLD")``.

The executor protocol is just two methods (``force_market_exit`` is
async); Phase 10's TradeExecutor implements it. Until then the engine
can run with a ``LoggingExecutor`` stub that just logs intended exits.
"""

from __future__ import annotations

import asyncio
import json
import logging
from decimal import Decimal
from time import time
from typing import Any, Dict, Optional, Protocol

import redis.asyncio as aioredis

from booknow.processors.base import AsyncProcessor
from booknow.repository import redis_keys
from booknow.trading.state import Position, TradeState
from booknow.trading.tsl import TrailingStopLoss


logger = logging.getLogger("booknow.position_monitor")


class ExitExecutor(Protocol):
    """Subset of TradeExecutor that the monitor needs.

    Phase 10's full TradeExecutor will satisfy this. Phase 9 ships
    with a ``LoggingExecutor`` stub so the surveillance plumbing runs
    end-to-end before live order plumbing arrives.
    """

    async def force_market_exit(
        self, symbol: str, current_price: Decimal, reason: str,
    ) -> None: ...


class LoggingExecutor:
    """Drop-in :class:`ExitExecutor` that just logs.

    Useful for paper trading and for running the rest of the engine
    while Phase 10 (the real TradeExecutor) is still in flight.
    """

    async def force_market_exit(
        self, symbol: str, current_price: Decimal, reason: str,
    ) -> None:
        logger.warning(
            "[paper-exit] %s reason=%s current_price=%s — would force market exit",
            symbol, reason, current_price,
        )


class PositionMonitor(AsyncProcessor):
    """Async port of TrailingStopLossProcessor.

    Wires together: TradeState (open positions), TrailingStopLoss
    (per-symbol high-water tracker), ExitExecutor (Phase 10), and
    Redis (for the live CURRENT_PRICE feed written by Phase 5).
    """

    name = "position_monitor"
    sleep_s = 1.0

    def __init__(
        self,
        redis_client: aioredis.Redis,
        trade_state: TradeState,
        tsl: TrailingStopLoss,
        executor: ExitExecutor,
        max_hold_seconds: int = 300,  # 5 min default; matches Java fast-scalp config
    ):
        super().__init__()
        self._redis = redis_client
        self._state = trade_state
        self._tsl = tsl
        self._executor = executor
        self.max_hold_seconds = max_hold_seconds

    async def _tick(self) -> None:
        positions = self._state.snapshot()
        if not positions:
            return

        prices = await self._read_current_prices(list(positions.keys()))
        now_ts = time()

        for symbol, pos in positions.items():
            cp = prices.get(symbol)
            if cp is None:
                continue

            try:
                price = Decimal(str(cp.get("price", "0")))
            except Exception:
                continue
            if price <= 0:
                continue

            # 1) Trailing stop-loss
            if self._tsl.check_and_track(symbol, price):
                self.log.info("[Monitor] TSL triggered for %s — forcing market exit", symbol)
                await self._safe_exit(symbol, price, "TSL")
                continue

            # 2) Max-hold timer
            if self.max_hold_seconds > 0:
                held = now_ts - pos.entry_time
                if held >= self.max_hold_seconds:
                    self.log.info(
                        "[Monitor] Max-hold %ds exceeded for %s (held %.0fs) — forcing market exit",
                        self.max_hold_seconds, symbol, held,
                    )
                    await self._safe_exit(symbol, price, "MAX_HOLD")

    async def _safe_exit(self, symbol: str, price: Decimal, reason: str) -> None:
        try:
            await self._executor.force_market_exit(symbol, price, reason)
        except Exception as e:
            self.log.error(
                "[Monitor] force_market_exit(%s, %s) failed: %s",
                symbol, reason, e, exc_info=True,
            )

    async def _read_current_prices(self, symbols: list[str]) -> Dict[str, Dict[str, Any]]:
        """Pull only the symbols we care about from CURRENT_PRICE."""
        if not symbols:
            return {}
        async with self._redis.pipeline(transaction=False) as pipe:
            for sym in symbols:
                pipe.hget(redis_keys.CURRENT_PRICE, sym)
            results = await pipe.execute()
        out: Dict[str, Dict[str, Any]] = {}
        for sym, raw in zip(symbols, results):
            if not raw:
                continue
            try:
                out[sym] = json.loads(raw)
            except json.JSONDecodeError:
                continue
        return out
