"""
klines_cache.py
─────────────────────────────────────────────────────────────────────────────
Async WebSocket-backed kline cache for Binance Spot.

Migrated from binance-sentiment-engine/klines_ws_cache.py during the
python-engine consolidation. Public API unchanged so callers in the
sentiment engine keep working via the compatibility shim at the old path.

Design (unchanged from origin):
1. Single combined-stream connection (`wss://stream.binance.com:9443/stream`)
   multiplexes up to 1024 streams.
2. Live SUBSCRIBE / UNSUBSCRIBE per (symbol, interval).
3. Single REST seed per (symbol, interval); WS keeps it warm afterwards.
4. `get_klines()` returns a DataFrame matching the legacy
   `fetch_klines()` shape so call sites barely change.

Phase-2 additions on top of the original:
- The REST seed path now consults the shared `RateLimitGuard` so a
  Binance ban anywhere in the engine pauses cache seeding too. Cache
  reads remain free.
- Logger name is `booknow.klines_cache` (was `KlinesCache`).
"""

from __future__ import annotations

import asyncio
import collections
import json
import logging
import ssl
import time
from typing import Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd
import requests
import urllib3
import websockets

from booknow.binance.rate_limit import get_default as _get_rate_limit_guard

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger("booknow.klines_cache")

BINANCE_REST = "https://api.binance.com"
BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream"

# Binance docs: max 1,024 streams per single connection. Stay well under.
MAX_STREAMS_PER_CONN = 900

# Subscribe rate: 5 messages/sec. We send one SUBSCRIBE per tick — be safe.
_SUBSCRIBE_MIN_GAP_S = 0.25


def _stream_name(symbol: str, interval: str) -> str:
    """Binance stream id, e.g. 'btcusdt@kline_5m'."""
    return f"{symbol.replace('/', '').lower()}@kline_{interval}"


class KlinesCache:
    """
    Async cache. Spawn one per process and ``await cache.start()`` once.

    Then either:
      - ``await cache.ensure(symbol, intervals)`` to subscribe, or
      - ``cache.get_klines(symbol, interval, limit)`` for a non-blocking
        read from the in-memory buffer.

    All mutations happen on the asyncio event loop that calls ``start()``.
    ``get_klines()`` is safe to call from the same loop.
    """

    def __init__(self, intervals: Iterable[str] = ("1m", "5m", "15m", "1h", "4h"),
                 buffer_size: int = 200,
                 rest_timeout: int = 10):
        self.live_intervals: Tuple[str, ...] = tuple(intervals)
        self.buffer_size = buffer_size
        self.rest_timeout = rest_timeout

        # buffer[symbol][interval] -> deque of dict rows
        self._buffer: Dict[str, Dict[str, collections.deque]] = {}
        # set of (symbol, interval) currently subscribed
        self._subscribed: Set[Tuple[str, str]] = set()
        # set of (symbol, interval) seeded via REST at least once
        self._seeded: Set[Tuple[str, str]] = set()

        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._ws_lock = asyncio.Lock()
        self._running = False
        self._next_msg_id = 1
        self._last_send = 0.0
        self._guard = _get_rate_limit_guard()

    # ── Lifecycle ────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Open the WS and spawn the read loop. Idempotent."""
        if self._running:
            return
        self._running = True
        asyncio.create_task(self._run_forever(), name="klines-cache-ws")

    async def stop(self) -> None:
        self._running = False
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

    async def _run_forever(self) -> None:
        """Auto-reconnecting WS read loop. Re-subscribes on every reconnect."""
        ssl_ctx = ssl._create_unverified_context()
        backoff = 2
        while self._running:
            try:
                async with websockets.connect(
                    BINANCE_WS_BASE,
                    ping_interval=30,
                    ping_timeout=15,
                    ssl=ssl_ctx,
                    max_size=4 * 1024 * 1024,
                ) as ws:
                    self._ws = ws
                    logger.info("[KlinesCache] WS connected (%d previously subscribed streams)",
                                len(self._subscribed))
                    # Resubscribe everything (after a reconnect, server forgot us)
                    if self._subscribed:
                        await self._send_subscribe([_stream_name(s, i) for s, i in self._subscribed])
                    backoff = 2
                    async for raw in ws:
                        self._handle_raw(raw)
            except (websockets.ConnectionClosed, OSError) as e:
                logger.warning("[KlinesCache] WS dropped: %s — reconnecting in %ds", e, backoff)
            except Exception as e:
                logger.error("[KlinesCache] WS error: %s — reconnecting in %ds", e, backoff)
            finally:
                self._ws = None

            if self._running:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    # ── Subscription management ──────────────────────────────────────────

    async def ensure(self, symbol: str, intervals: Optional[Iterable[str]] = None) -> None:
        """
        Make sure ``(symbol, interval)`` is both seeded (REST once) and
        streaming (WS). Safe to call repeatedly — it's a no-op once warm.
        """
        sym = symbol.replace("/", "").upper()
        ivs = tuple(intervals) if intervals is not None else self.live_intervals
        # Seed any missing buffers (one REST call each, async-friendly)
        for iv in ivs:
            if (sym, iv) not in self._seeded:
                await asyncio.get_event_loop().run_in_executor(None, self._seed_one, sym, iv)
        # Subscribe to anything not yet on the wire
        new_streams = [
            _stream_name(sym, iv) for iv in ivs if (sym, iv) not in self._subscribed
        ]
        if new_streams:
            for iv in ivs:
                self._subscribed.add((sym, iv))
            await self._send_subscribe(new_streams)

    async def release(self, symbols_to_keep: Set[str]) -> None:
        """
        Unsubscribe symbols not in ``symbols_to_keep``. Lets the cache
        shrink as the FAST_MOVE list rotates so we don't hit the 1024-stream cap.
        """
        keep = {s.replace("/", "").upper() for s in symbols_to_keep}
        drop_pairs = [(s, i) for (s, i) in self._subscribed if s not in keep]
        if not drop_pairs:
            return
        for pair in drop_pairs:
            self._subscribed.discard(pair)
        await self._send_unsubscribe([_stream_name(s, i) for (s, i) in drop_pairs])
        # Free memory for fully released symbols
        dropped_symbols = {s for (s, _) in drop_pairs}
        for sym in dropped_symbols:
            if sym not in {s for (s, _) in self._subscribed}:
                self._buffer.pop(sym, None)
                # leave _seeded set as a "we've seen this" marker; cheap.

    # ── Reads ────────────────────────────────────────────────────────────

    def get_klines(self, symbol: str, interval: str, limit: int) -> pd.DataFrame:
        """
        Return up to ``limit`` most recent candles as a DataFrame matching
        the shape that the legacy ``fetch_klines()`` returned. Empty
        DataFrame if the cache hasn't been warmed yet.
        """
        sym = symbol.replace("/", "").upper()
        rows = self._buffer.get(sym, {}).get(interval)
        if not rows:
            return pd.DataFrame()
        slice_ = list(rows)[-limit:] if limit else list(rows)
        return pd.DataFrame(slice_, columns=["timestamp", "open", "high", "low", "close", "volume"])

    def has(self, symbol: str, interval: str) -> bool:
        sym = symbol.replace("/", "").upper()
        return bool(self._buffer.get(sym, {}).get(interval))

    # ── Internal: REST seed + WS write/read ──────────────────────────────

    def _seed_one(self, symbol: str, interval: str) -> None:
        """Single REST fetch to seed the buffer. Runs off the event loop."""
        # Honour the shared cool-down: a Binance ban anywhere in the
        # engine pauses fresh seeds too.
        if self._guard.is_banned():
            logger.warning(
                "[KlinesCache] Skipping seed for %s %s — Binance ban active for %ds",
                symbol, interval, self._guard.ban_remaining_seconds(),
            )
            return
        try:
            r = requests.get(
                f"{BINANCE_REST}/api/v3/klines",
                params={"symbol": symbol, "interval": interval, "limit": self.buffer_size},
                timeout=self.rest_timeout,
                verify=False,
            )
            if r.status_code == 429:
                logger.warning("[KlinesCache] seed rate-limited %s %s", symbol, interval)
                return
            if r.status_code in (418, 429) or (r.status_code >= 400 and "banned" in r.text.lower()):
                # Surface to the shared guard. r.text typically contains
                # "Way too much request weight used; IP banned until N".
                self._guard.report_if_banned(RuntimeError(r.text))
                return
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            if self._guard.report_if_banned(e):
                return
            logger.warning("[KlinesCache] seed failed %s %s: %s", symbol, interval, e)
            return

        dq = collections.deque(maxlen=self.buffer_size)
        for k in data:
            dq.append({
                "timestamp": pd.to_datetime(k[0], unit="ms"),
                "open":   float(k[1]),
                "high":   float(k[2]),
                "low":    float(k[3]),
                "close":  float(k[4]),
                "volume": float(k[5]),
            })
        self._buffer.setdefault(symbol, {})[interval] = dq
        self._seeded.add((symbol, interval))
        logger.debug("[KlinesCache] seeded %s %s (%d candles)", symbol, interval, len(dq))

    async def _send_subscribe(self, streams: List[str]) -> None:
        await self._send({"method": "SUBSCRIBE", "params": streams, "id": self._allocate_id()})

    async def _send_unsubscribe(self, streams: List[str]) -> None:
        await self._send({"method": "UNSUBSCRIBE", "params": streams, "id": self._allocate_id()})

    async def _send(self, payload: dict) -> None:
        async with self._ws_lock:
            ws = self._ws
            if ws is None:
                # Will be replayed on reconnect via self._subscribed
                logger.debug("[KlinesCache] WS not connected, deferring send: %s", payload.get("method"))
                return
            now = time.monotonic()
            gap = now - self._last_send
            if gap < _SUBSCRIBE_MIN_GAP_S:
                await asyncio.sleep(_SUBSCRIBE_MIN_GAP_S - gap)
            try:
                await ws.send(json.dumps(payload))
                self._last_send = time.monotonic()
            except Exception as e:
                logger.warning("[KlinesCache] send failed (%s): %s", payload.get("method"), e)

    def _allocate_id(self) -> int:
        self._next_msg_id += 1
        return self._next_msg_id

    def _handle_raw(self, raw) -> None:
        """Parse one WS frame. Combined-stream payloads wrap data in {stream, data}."""
        try:
            msg = json.loads(raw)
        except Exception:
            return
        # Subscribe/unsubscribe ACKs come back as {result: null, id: N} — ignore.
        data = msg.get("data") if isinstance(msg, dict) else None
        if not data or data.get("e") != "kline":
            return
        kline = data.get("k") or {}
        symbol = data.get("s") or kline.get("s")
        interval = kline.get("i")
        if not symbol or not interval:
            return
        is_closed = bool(kline.get("x"))

        row = {
            "timestamp": pd.to_datetime(int(kline["t"]), unit="ms"),
            "open":   float(kline["o"]),
            "high":   float(kline["h"]),
            "low":    float(kline["l"]),
            "close":  float(kline["c"]),
            "volume": float(kline["v"]),
        }

        symbol_buf = self._buffer.setdefault(symbol, {})
        dq = symbol_buf.get(interval)
        if dq is None:
            # Stream arrived before seed completed — start a new buffer; the
            # in-flight seed will overwrite once it lands.
            dq = collections.deque(maxlen=self.buffer_size)
            symbol_buf[interval] = dq

        if is_closed:
            # Final candle: append (or replace last if same timestamp)
            if dq and dq[-1]["timestamp"] == row["timestamp"]:
                dq[-1] = row
            else:
                dq.append(row)
        else:
            # In-progress candle: keep the buffer "live" by overwriting last
            # if it matches the same open_time, else append. This means
            # ``get_klines()`` always reflects the latest tick for the most
            # recent bar — which is what fetch_klines() did via REST.
            if dq and dq[-1]["timestamp"] == row["timestamp"]:
                dq[-1] = row
            else:
                dq.append(row)
