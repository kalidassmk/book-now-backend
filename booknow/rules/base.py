"""
base.py
─────────────────────────────────────────────────────────────────────────────
Shared scaffolding for the three rule loops (R1 / R2 / R3).

Each rule reads a fixed set of timing fields from the ``ST0/ST1/ST2/ST3``
Redis hashes (written by Phase 8's TimeAnalyser), matches a small
pattern, and calls :meth:`TradeExecutor.try_buy` when the pattern fires.

This base class collapses the duplicated boilerplate:

  - The async run loop (inherited from :class:`AsyncProcessor`).
  - The per-symbol "triggered" guard so a single ladder pattern doesn't
    fire repeatedly while the symbol is in flight.
  - Sell-listener registration on :class:`TradeState` so the guard
    re-arms once the position closes (the same coin can be scalped
    again on the next signal).
  - Helpers for reading the timing hashes, the CURRENT_PRICE map, and
    persisting rule-result JSON.

Rules differ only in the pattern logic + the ``sell_pct`` they hand
to ``try_buy``, so the subclasses stay tiny.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Mapping, Set

import redis.asyncio as aioredis

from booknow.config.trading_config import TradingConfigService
from booknow.processors.base import AsyncProcessor
from booknow.repository import redis_keys
from booknow.trading.executor import TradeExecutor
from booknow.trading.state import TradeState


class RuleBase(AsyncProcessor):
    """Common helpers + lifecycle for R1 / R2 / R3."""

    sleep_s = 0.5

    def __init__(
        self,
        *,
        redis_client: aioredis.Redis,
        trade_state: TradeState,
        trade_executor: TradeExecutor,
        config_service: TradingConfigService,
    ):
        super().__init__()
        self._redis = redis_client
        self._executor = trade_executor
        self._config = config_service
        self._triggered: Set[str] = set()
        # Re-arm the per-symbol guard once the position closes.
        trade_state.add_sell_listener(self._on_sold)

    # ── Sell-listener callback ───────────────────────────────────────────

    def _on_sold(self, symbol: str) -> None:
        """Called from TradeState.mark_sold (synchronous)."""
        self._triggered.discard(symbol)

    # ── Redis helpers ────────────────────────────────────────────────────

    async def _read_timing(self, store_key: str, label: str) -> Dict[str, float]:
        """Pull one ``ST*`` field and return ``{symbol → shortest seconds}``.

        TimeAnalyser writes:
            store_key = ST0 | ST1 | ST2 | ST3   (the outer Redis hash)
            label     = "0T1" | "1T2" | "2T5" | …   (the field on it)
            value     = JSON {"name": label,
                              "shortestTimeList": [{"symbol": …, "timeTook": …}]}

        Multiple entries with the same symbol are min-merged so the
        rule sees the fastest crossing on record.
        """
        raw = await self._redis.hget(store_key, label)
        if not raw:
            return {}
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        out: Dict[str, float] = {}
        for st in data.get("shortestTimeList") or []:
            sym = st.get("symbol")
            time_took = st.get("timeTook") or 0
            try:
                seconds = float(time_took)
            except (TypeError, ValueError):
                continue
            if sym and seconds > 0:
                cur = out.get(sym)
                if cur is None or seconds < cur:
                    out[sym] = seconds
        return out

    async def _read_current_prices(self) -> Dict[str, Dict[str, Any]]:
        """Snapshot of ``CURRENT_PRICE`` parsed into ``{symbol → dict}``."""
        raw = await self._redis.hgetall(redis_keys.CURRENT_PRICE)
        out: Dict[str, Dict[str, Any]] = {}
        for sym, value in raw.items():
            try:
                out[sym] = json.loads(value)
            except json.JSONDecodeError:
                continue
        return out

    async def _save_rule_result(
        self, hash_key: str, symbol: str, payload: Mapping[str, Any],
    ) -> None:
        await self._redis.hset(hash_key, symbol, json.dumps(dict(payload)))

    # ── Fire the buy ────────────────────────────────────────────────────

    async def _fire(
        self,
        symbol: str,
        prices: Dict[str, Dict[str, Any]],
        sell_pct: float,
        label: str,
    ) -> None:
        """Hand a confirmed pattern off to :class:`TradeExecutor`.

        Mirrors the Java ``fire()`` method's fast-scalp gate. When
        ``fastScalpMode`` is off the Java code routes through the
        multi-agent ConsensusCoordinator; that's not yet ported, so
        we skip with a warning. Default config has fast-scalp ON.
        """
        cp = prices.get(symbol)
        if cp is None:
            return
        cfg = await self._config.get()
        if not cfg.fastScalpMode:
            self.log.warning(
                "[%s] fast-scalp mode OFF and ConsensusCoordinator not yet "
                "ported in the Python engine — skipping fire for %s. "
                "Toggle fastScalpMode back on, or wait for the consensus "
                "port in a future phase.",
                self.name, symbol,
            )
            return
        await self._executor.try_buy(symbol, cp, sell_pct, label)
