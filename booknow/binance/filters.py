"""
filters.py
─────────────────────────────────────────────────────────────────────────────
Async port of BinanceFilterService.java.

Caches Binance Spot exchange-info filters (LOT_SIZE / PRICE_FILTER /
MIN_NOTIONAL) in Redis and exposes the price/qty rounding helpers the
trade executor calls before sending an order. ``getExchangeInfo`` is
the only Binance endpoint that has no websocket equivalent, so this
module is REST-bound by definition. We mitigate by:

  - One full refresh per hour via a background task.
  - 24-hour Redis TTL on the per-symbol entries so a missed refresh
    doesn't break trading.
  - On-demand fetch on cache miss (rate-limit-guarded).

Schema (matches Java's ``BINANCE:SYMBOL:<symbol>``):
    {
        "symbol":      "BTCUSDT",
        "stepSize":    "0.00001",
        "minQty":      "0.00001",
        "maxQty":      "9000",
        "tickSize":    "0.01",
        "minNotional": "10"
    }
"""

from __future__ import annotations

import asyncio
import json
import logging
from decimal import Decimal, ROUND_FLOOR
from typing import Any, Dict, Optional

import redis.asyncio as aioredis

from booknow.binance.rate_limit import get_default as _get_rate_limit_guard
from booknow.binance.rest_api import BinanceIpBannedException, RestApiClient
from booknow.repository import redis_keys


logger = logging.getLogger("booknow.filters")

REFRESH_INTERVAL_S = 60 * 60       # 1 hour
ENTRY_TTL_S = 24 * 60 * 60         # 24 hours per-symbol


class FilterService:
    """Owner of the ``BINANCE:SYMBOL:<symbol>`` Redis cache.

    Pass an :class:`RestApiClient` (typically the engine-wide one) so
    we share the same ban-aware HTTP layer.
    """

    def __init__(self, redis_client: aioredis.Redis, rest: RestApiClient):
        self._redis = redis_client
        self._rest = rest
        self._guard = _get_rate_limit_guard()
        self._refresh_task: Optional[asyncio.Task] = None
        self._running = False
        self._refresh_lock = asyncio.Lock()

    # ── Lifecycle ────────────────────────────────────────────────────────

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        # Best-effort initial refresh: if we're banned right now this is
        # a no-op and the background task will retry. Cache survives
        # restarts so most boots still trade fine.
        await self.refresh_cache()
        self._refresh_task = asyncio.create_task(self._refresh_loop(), name="filter-refresh")
        logger.info("[FilterService] task spawned (hourly refresh)")

    async def stop(self) -> None:
        self._running = False
        if self._refresh_task is not None:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except (asyncio.CancelledError, Exception):
                pass
            self._refresh_task = None

    # ── Public API ───────────────────────────────────────────────────────

    async def refresh_cache(self) -> int:
        """Pull all USDT symbols from exchangeInfo. Returns count cached."""
        if self._guard.is_banned():
            logger.warning(
                "[FilterService] refresh skipped — Binance ban active for %ds",
                self._guard.ban_remaining_seconds(),
            )
            return 0
        async with self._refresh_lock:
            try:
                info = await self._rest.get_exchange_info()
            except BinanceIpBannedException as e:
                logger.error("[FilterService] refresh blocked by ban: %s", e)
                return 0
            except Exception as e:
                if self._guard.report_if_banned(e):
                    return 0
                logger.error("[FilterService] refresh failed: %s", e)
                return 0

            count = 0
            symbols = info.get("symbols") or []
            async with self._redis.pipeline(transaction=False) as pipe:
                for s in symbols:
                    name = s.get("symbol")
                    if not name or not name.endswith("USDT"):
                        continue
                    rule = _extract_rule(s)
                    pipe.set(
                        f"{redis_keys.SYMBOL_PREFIX}{name}",
                        json.dumps(rule),
                        ex=ENTRY_TTL_S,
                    )
                    count += 1
                await pipe.execute()
            logger.info("[FilterService] cache refreshed — %d USDT symbols", count)
            return count

    async def get_or_fetch(self, symbol: str) -> Dict[str, str]:
        """Return cached rule for a symbol, fetching one-shot on miss."""
        sym = symbol.upper()
        if not sym.endswith("USDT"):
            raise RuntimeError(f"Only USDT pairs are supported for trading (got {sym!r})")

        key = f"{redis_keys.SYMBOL_PREFIX}{sym}"
        raw = await self._redis.get(key)
        if raw:
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("[FilterService] corrupted cache for %s — refetching", sym)

        # Cache miss. Honour ban guard before touching REST.
        if self._guard.is_banned():
            raise RuntimeError(
                f"Binance ban active ({self._guard.ban_remaining_seconds()}s remaining); "
                f"cannot fetch filter data for {sym}. Trade aborted for safety."
            )

        try:
            info = await self._rest.get_exchange_info()
        except BinanceIpBannedException as e:
            raise RuntimeError(
                f"Binance ban triggered while fetching {sym}; cool-down "
                f"{self._guard.ban_remaining_seconds()}s remaining"
            ) from e

        for s in info.get("symbols") or []:
            if s.get("symbol") == sym:
                rule = _extract_rule(s)
                await self._redis.set(key, json.dumps(rule), ex=ENTRY_TTL_S)
                return rule

        raise RuntimeError(f"No Binance filter data found for symbol: {sym}. Trade aborted for safety.")

    async def round_price(self, symbol: str, price) -> Decimal:
        """Round price down to symbol's tickSize."""
        rule = await self.get_or_fetch(symbol)
        tick = Decimal(rule["tickSize"])
        return _floor_to_step(Decimal(str(price)), tick)

    async def round_quantity(self, symbol: str, qty) -> Decimal:
        """Round qty down to symbol's stepSize."""
        rule = await self.get_or_fetch(symbol)
        step = Decimal(rule["stepSize"])
        return _floor_to_step(Decimal(str(qty)), step)

    async def validate_notional(self, symbol: str, qty, price) -> None:
        """Raise if qty * price < minNotional. Mirrors the Java contract."""
        rule = await self.get_or_fetch(symbol)
        min_notional = Decimal(rule.get("minNotional") or "5.0")
        notional = Decimal(str(qty)) * Decimal(str(price))
        if notional < min_notional:
            raise RuntimeError(
                f"Trade size too small! Notional {notional} < minNotional {min_notional}"
            )

    # ── Background refresh ──────────────────────────────────────────────

    async def _refresh_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(REFRESH_INTERVAL_S)
            except asyncio.CancelledError:
                break
            if not self._running:
                break
            await self.refresh_cache()


# ── Helpers ────────────────────────────────────────────────────────────


def _extract_rule(symbol_info: Dict[str, Any]) -> Dict[str, str]:
    """Translate a single Binance symbolInfo into our compact rule dict."""
    name = symbol_info.get("symbol", "")
    filters_by_type = {f.get("filterType"): f for f in symbol_info.get("filters") or []}

    lot_size = filters_by_type.get("LOT_SIZE") or {}
    price_filter = filters_by_type.get("PRICE_FILTER") or {}
    notional = filters_by_type.get("MIN_NOTIONAL") or filters_by_type.get("NOTIONAL") or {}

    return {
        "symbol":      name,
        "stepSize":    lot_size.get("stepSize")     or "0.00000001",
        "minQty":      lot_size.get("minQty")       or "0.00000001",
        "maxQty":      lot_size.get("maxQty")       or "999999999",
        "tickSize":    price_filter.get("tickSize") or "0.00000001",
        "minNotional": notional.get("minNotional")  or "5.0",
    }


def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    """Round-down (floor) `value` to the nearest multiple of `step`.

    Mirrors the Java helper:
        value.divide(step, 0, RoundingMode.FLOOR).multiply(step)
    """
    if step <= 0:
        return value
    units = (value / step).to_integral_value(rounding=ROUND_FLOOR)
    return units * step
