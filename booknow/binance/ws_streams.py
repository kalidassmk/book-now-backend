"""
ws_streams.py
─────────────────────────────────────────────────────────────────────────────
Async port of MessageConsumer.java — the market data fan-in.

Connects to Binance's ``!ticker_1h@arr`` stream and, for each tick:

  1. Filters to USDT pairs, drops delisted symbols.
  2. Records the tick-to-tick momentum score (+/- weighted).
  3. Updates the per-symbol price+percentage in Redis (CURRENT_PRICE).
  4. On first sight of a symbol, stores the baseline (RW_BASE_PRICE).
  5. Computes base→current % gain and price delta.
  6. Pushes a WatchList row into the global WATCH_ALL hash.
  7. Maps the gain into a bucket (>0<1 .. >10) and updates the
     per-symbol FAST_MOVE momentum counter.
  8. On first time a symbol enters a bucket, pins a copy of the
     watchlist row under ``BS_TO_<bucket>_INC_%``.
  9. Persists a Percentage entry (with single-tick history) under
     the bucket hash so processors and rules downstream can read it.

The Redis JSON shapes match the Java models exactly so the existing
dashboard and Python sentiment engine read what they were reading
yesterday — no schema migration required.

Lifecycle: ``start()`` spawns one async task; ``stop()`` cancels it
and closes the websocket. Auto-reconnects on disconnect with
exponential backoff. Honours :class:`RateLimitGuard` even though the
stream is a public one (Binance's IP-ban applies to all endpoints
including streams).
"""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
from dataclasses import asdict
from decimal import Decimal
from time import time
from typing import Any, Dict, List, Optional, Set

import websockets
import websockets.exceptions

from booknow.binance.rate_limit import get_default as _get_rate_limit_guard
from booknow.repository import redis_keys
from booknow.util.momentum import (
    DEFAULT_DELIST_SEED,
    FastMove,
    get_bucket,
    get_hms,
    get_percentage,
    get_price,
    momentum_score,
    to_decimal,
)


logger = logging.getLogger("booknow.ws_streams")

_USDT = "USDT"
_USDT_LEN = 4
_STREAM_URL = "wss://stream.binance.com:9443/ws/!ticker_1h@arr"
_RECONNECT_BACKOFF_MAX_S = 30
_BAN_DEFAULT_COOLDOWN_S = 120

# Match other modules' SSL convention on this host (corporate proxies /
# missing CA bundles). Production deployments should plug certifi in.
_SSL_CTX = ssl._create_unverified_context()


class MarketStreamService:
    """Owner of the ``!ticker_1h@arr`` subscription.

    Wire one instance into the engine and ``await service.start()``.
    The Redis writes happen inside the per-frame handler — there's no
    queue between the websocket and Redis, so a Redis stall will
    backpressure the consumer (which is the right behaviour: better to
    drop ticks than to OOM on a buffered queue).
    """

    def __init__(self, redis_client, delist: Optional[Set[str]] = None):
        self._redis = redis_client
        self._delist: Set[str] = set(delist) if delist is not None else set(DEFAULT_DELIST_SEED)
        self._guard = _get_rate_limit_guard()

        # In-memory view of RW_BASE_PRICE so we don't fetch every tick.
        self._baseline: Dict[str, Dict[str, Any]] = {}
        # In-memory view of FAST_MOVE so we don't read-modify-write Redis.
        self._fast_move: Dict[str, FastMove] = {}
        # Tracks which symbols have already been logged as crossing a
        # given bucket — one log line per (symbol, bucket).
        self._band_crossed: Set[str] = set()

        self._task: Optional[asyncio.Task] = None
        self._running = False

    # ── Lifecycle ────────────────────────────────────────────────────────

    async def start(self) -> None:
        if self._running:
            return
        await self._load_baseline_from_redis()
        await self._load_fast_move_from_redis()
        self._running = True
        self._task = asyncio.create_task(self._run_forever(), name="market-stream")
        logger.info(
            "[MarketStream] task spawned (baseline=%d cached, delist=%d entries)",
            len(self._baseline), len(self._delist),
        )

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

    # ── Connection loop ──────────────────────────────────────────────────

    async def _run_forever(self) -> None:
        backoff = 2
        while self._running:
            if self._guard.is_banned():
                secs = self._guard.ban_remaining_seconds()
                logger.warning(
                    "[MarketStream] connect deferred — Binance ban for %ds", secs,
                )
                await asyncio.sleep(min(secs + 1, _RECONNECT_BACKOFF_MAX_S))
                continue
            try:
                async with websockets.connect(
                    _STREAM_URL,
                    ping_interval=20,
                    ping_timeout=15,
                    ssl=_SSL_CTX,
                    max_size=8 * 1024 * 1024,
                ) as ws:
                    logger.info("[MarketStream] connected to %s", _STREAM_URL)
                    backoff = 2
                    async for raw in ws:
                        if not self._running:
                            break
                        await self._on_market_event(raw)
            except (websockets.exceptions.ConnectionClosed, OSError) as e:
                logger.warning("[MarketStream] dropped: %s — reconnecting in %ds", e, backoff)
            except Exception as e:
                if self._guard.report_if_banned(e):
                    logger.error("[MarketStream] ban detected — pausing %ds", _BAN_DEFAULT_COOLDOWN_S)
                    await asyncio.sleep(_BAN_DEFAULT_COOLDOWN_S)
                else:
                    logger.error("[MarketStream] error: %s", e, exc_info=True)

            if not self._running:
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, _RECONNECT_BACKOFF_MAX_S)

    # ── Per-frame pipeline ───────────────────────────────────────────────

    async def _on_market_event(self, raw) -> None:
        try:
            tickers = json.loads(raw)
        except json.JSONDecodeError:
            return
        if not isinstance(tickers, list):
            return

        # Filter to USDT pairs, drop delisted, sort highest-percentage first
        # (matches the Java consumer's ordering for downstream consumers
        # that grab a "top movers" slice without a sort).
        valid: List[Dict[str, Any]] = [
            t for t in tickers
            if isinstance(t.get("s"), str)
            and t["s"].endswith(_USDT)
            and t["s"] not in self._delist
        ]
        try:
            valid.sort(key=lambda t: float(t.get("P", 0) or 0), reverse=True)
        except (TypeError, ValueError):
            pass

        if not valid:
            return

        # Pull the prior CURRENT_PRICE map ONCE per event so the
        # tick-to-tick deltas are computed against a consistent
        # snapshot (Java does the same).
        try:
            prior_raw = await self._redis.hgetall(redis_keys.CURRENT_PRICE)
        except Exception as e:
            if self._guard.report_if_banned(e):
                return
            logger.error("[MarketStream] redis hgetall CURRENT_PRICE failed: %s", e)
            return

        prior: Dict[str, Dict[str, Any]] = {}
        for sym, json_str in prior_raw.items():
            try:
                prior[sym] = json.loads(json_str)
            except json.JSONDecodeError:
                continue

        try:
            for t in valid:
                await self._process_ticker(t, prior)
        except Exception as e:
            if self._guard.report_if_banned(e):
                return
            logger.error("[MarketStream] process error: %s", e, exc_info=True)

    async def _process_ticker(
        self,
        ticker: Dict[str, Any],
        prior: Dict[str, Dict[str, Any]],
    ) -> None:
        symbol: str = ticker["s"]
        cur_pct = _f(ticker.get("P"))
        cur_price = to_decimal(ticker.get("c"))
        cur_ts = int(ticker.get("E") or time() * 1000)

        # 1) momentum delta (against the prior CURRENT_PRICE entry).
        prior_entry = prior.get(symbol) or {}
        prev_pct = _f(prior_entry.get("percentage", cur_pct))
        prev_price = to_decimal(prior_entry.get("price", cur_price))

        tick_delta = get_percentage(prev_pct, cur_pct)
        score = momentum_score(tick_delta)

        # 2) write CURRENT_PRICE row (per-symbol field on the hash).
        cp_payload = {
            "symbol": symbol,
            "percentage": cur_pct,
            "price": _decimal_to_jsonable(cur_price),
            "timestamp": cur_ts,
            "hms": get_hms(),
            "healthIndex": 0.0,
        }
        await self._redis.hset(redis_keys.CURRENT_PRICE, symbol, json.dumps(cp_payload))

        # 3) initialise baseline on first sight.
        baseline = self._baseline.get(symbol)
        if baseline is None:
            baseline = {
                "Symbol": symbol,
                "price": _decimal_to_jsonable(cur_price),
                "percentage": cur_pct,
                "timestamp": cur_ts,
            }
            self._baseline[symbol] = baseline
            await self._redis.hset(
                redis_keys.RW_BASE_PRICE, symbol, json.dumps(baseline),
            )

        base_pct = _f(baseline.get("percentage", cur_pct))
        base_price = to_decimal(baseline.get("price", cur_price))
        base_ts = int(baseline.get("timestamp") or cur_ts)

        # 4) base→current gain.
        gain_pct = get_percentage(base_pct, cur_pct)
        gain_price = get_price(base_price, cur_price)
        interval_min = (cur_ts - base_ts) / 60_000.0

        # 5) WatchList row under WATCH_ALL.
        watchlist_payload = {
            "symbol": symbol,
            "basePercentage": base_pct,
            "currentPercentage": cur_pct,
            "increasedPercentage": gain_pct,
            "basePrice": _decimal_to_jsonable(base_price),
            "currentPrice": _decimal_to_jsonable(cur_price),
            "increasedPrice": _decimal_to_jsonable(gain_price),
            "count": 0,
            "timestamp": cur_ts,
            "hms": get_hms(),
        }
        watchlist_json = json.dumps(watchlist_payload)
        await self._redis.hset(redis_keys.WATCH_ALL, symbol, watchlist_json)

        # 6) FastMove counter + bucket assignment.
        fast_move = self._fast_move.get(symbol) or FastMove(symbol=symbol)
        if not fast_move.symbol:
            fast_move.symbol = symbol
        bucket = get_bucket(gain_pct, fast_move, score)
        if bucket is None:
            # Below 0.30 % — nothing more to record.
            self._fast_move[symbol] = fast_move
            return
        self._fast_move[symbol] = fast_move
        await self._redis.hset(
            redis_keys.FAST_MOVE, symbol, json.dumps(asdict(fast_move)),
        )

        # 7) record first time a symbol enters a bucket.
        band_key = f"{symbol}::{bucket}"
        if band_key not in self._band_crossed:
            self._band_crossed.add(band_key)
            watch_key = f"{redis_keys.WATCH_PREFIX}{bucket}{redis_keys.WATCH_SUFFIX}"
            await self._redis.hset(watch_key, symbol, watchlist_json)
            logger.debug(
                "%s first crossed bucket %s at %s | gain=%.2f%%",
                symbol, bucket, get_hms(), gain_pct,
            )

        # 8) Percentage entry under the bucket hash (single-tick history,
        # matching the Java consumer's behaviour).
        prev_entry = {
            "lPercentage": prev_pct,
            "cPercentage": cur_pct,
            "iPercentage": tick_delta,
            "basePrice": _decimal_to_jsonable(prev_price),
            "currentPrice": _decimal_to_jsonable(cur_price),
            "increasedPrice": _decimal_to_jsonable(get_price(prev_price, cur_price)),
            "timestamp": cur_ts,
        }
        percentage_payload = {
            "symbol": symbol,
            "basePercentage": base_pct,
            "currentPercentage": cur_pct,
            "increasedPercentage": gain_pct,
            "basePrice": _decimal_to_jsonable(base_price),
            "currentPrice": _decimal_to_jsonable(cur_price),
            "increasedPrice": _decimal_to_jsonable(gain_price),
            "baseTimeStamp": base_ts,
            "currentTimeStamp": cur_ts,
            "baseToCurrentInterval": interval_min,
            "previousCountList": [prev_entry],
            "hms": get_hms(),
        }
        await self._redis.hset(bucket, symbol, json.dumps(percentage_payload))

    # ── Boot-time cache loaders ──────────────────────────────────────────

    async def _load_baseline_from_redis(self) -> None:
        """Populate the in-memory baseline cache from Redis on startup."""
        try:
            raw = await self._redis.hgetall(redis_keys.RW_BASE_PRICE)
        except Exception as e:
            logger.warning("[MarketStream] couldn't load baselines: %s", e)
            return
        for sym, value in raw.items():
            try:
                self._baseline[sym] = json.loads(value)
            except json.JSONDecodeError:
                continue

    async def _load_fast_move_from_redis(self) -> None:
        """Populate the FastMove counter cache from Redis on startup."""
        try:
            raw = await self._redis.hgetall(redis_keys.FAST_MOVE)
        except Exception as e:
            logger.warning("[MarketStream] couldn't load FastMove: %s", e)
            return
        for sym, value in raw.items():
            try:
                obj = json.loads(value)
                fm = FastMove(symbol=sym)
                for attr in ("countG0L1", "countG1L2", "countG2L3", "countG3L5",
                             "countG5L7", "countG7L10", "countG10", "overAllCount"):
                    setattr(fm, attr, _f(obj.get(attr, 0)))
                self._fast_move[sym] = fm
            except json.JSONDecodeError:
                continue


# ── Helpers ────────────────────────────────────────────────────────────────


def _f(value, default: float = 0.0) -> float:
    """Forgiving float() — tolerates None, bytes, str numerics, etc."""
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _decimal_to_jsonable(d: Decimal):
    """Emit Decimal as a JSON number (matches Jackson's default)."""
    if d is None:
        return 0.0
    # Use float for compactness; precision is sufficient for spot prices.
    # If a future symbol needs >15 digits we can switch to str trivially.
    return float(d)
