"""
ws_klines_cache.py
─────────────────────────────────────────────────────────────────────────────
Sync facade over the async ``booknow.binance.klines_cache.KlinesCache`` so
sentiment subprocess scripts (sync ``while True:`` workers) can read
WebSocket-backed kline buffers instead of hitting Binance REST per call.

Design
------
- Lazily spawns one daemon thread per process running its own asyncio loop.
- The thread owns a ``KlinesCache`` instance. Streams auto-resubscribe on
  WS reconnect (handled inside ``KlinesCache._run_forever``).
- ``ensure(symbol, intervals)`` blocks until the one-time REST seed
  completes so the caller can immediately read.
- ``get_ohlcv(symbol, interval, limit)`` returns CCXT-style rows
  ``[[ts_ms, open, high, low, close, volume], ...]`` for drop-in
  compatibility with the rest of the sentiment scripts and ``klines_router``.

Cost model
----------
- First call for a (symbol, interval): one REST seed (1 weight on Binance)
  + a SUBSCRIBE message. Then the buffer is kept warm by the WS stream.
- All subsequent calls for the same (symbol, interval): zero REST cost.

Per-process scope
-----------------
Each sentiment subprocess runs in its own Python interpreter so it spawns
its own loop + WS connection. Two extra WS connections per backend
container is fine; Binance allows hundreds per IP.
"""
from __future__ import annotations

import asyncio
import logging
import threading
from concurrent.futures import TimeoutError as FuturesTimeoutError
from typing import Iterable, List, Optional, Set, Tuple

log = logging.getLogger("booknow.ws_klines_cache")

# Pre-warmed intervals. Kept narrow because each (symbol, interval) pair
# consumes a stream slot and Binance caps at 1,024 streams per connection.
DEFAULT_INTERVALS: Tuple[str, ...] = ("1s", "1m", "1h")
DEFAULT_BUFFER_SIZE = 200
ENSURE_TIMEOUT_S = 15


class WSKlinesCache:
    def __init__(
        self,
        intervals: Iterable[str] = DEFAULT_INTERVALS,
        buffer_size: int = DEFAULT_BUFFER_SIZE,
    ):
        # Defer the import so the consumer file can be imported in
        # environments where booknow isn't on the path yet.
        from booknow.binance.klines_cache import KlinesCache

        self._KlinesCache = KlinesCache
        self._intervals = tuple(intervals)
        self._buffer_size = buffer_size

        self._cache: Optional[KlinesCache] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._ready = threading.Event()
        self._stopped = False

        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="ws-klines-loop",
        )
        self._thread.start()
        if not self._ready.wait(timeout=10):
            log.error("[WSKlinesCache] async loop didn't start within 10s")

    def _run_loop(self):
        try:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            self._cache = self._KlinesCache(
                intervals=self._intervals,
                buffer_size=self._buffer_size,
            )
            self._loop.create_task(self._cache.start())
            self._ready.set()
            log.info(
                "[WSKlinesCache] loop started — intervals=%s, buffer=%d",
                list(self._intervals), self._buffer_size,
            )
            self._loop.run_forever()
        except Exception as e:
            log.exception("[WSKlinesCache] loop crashed: %s", e)
        finally:
            self._stopped = True

    # ── Public API ────────────────────────────────────────────────────────

    def ensure(
        self,
        symbol: str,
        intervals: Optional[Iterable[str]] = None,
        timeout: float = ENSURE_TIMEOUT_S,
    ) -> bool:
        """Subscribe to live updates AND complete the REST seed.
        Blocks (with timeout) so that on success the buffer is immediately readable.
        Returns False on timeout / error / loop-not-ready."""
        if self._stopped or not self._cache or not self._loop:
            return False
        ivs = tuple(intervals) if intervals else self._intervals
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._cache.ensure(symbol, ivs), self._loop,
            )
            future.result(timeout=timeout)
            return True
        except FuturesTimeoutError:
            log.warning("[WSKlinesCache] ensure timeout %ss for %s %s", timeout, symbol, ivs)
            return False
        except Exception as e:
            log.warning("[WSKlinesCache] ensure failed for %s %s: %s", symbol, ivs, e)
            return False

    def get_ohlcv(self, symbol: str, interval: str, limit: int) -> List[list]:
        """CCXT-compatible read: ``[[ts_ms, o, h, l, c, v], ...]``.
        Empty list if buffer not warmed."""
        if not self._cache:
            return []
        df = self._cache.get_klines(symbol, interval, limit)
        if df.empty:
            return []
        out: List[list] = []
        for _, row in df.iterrows():
            ts = row["timestamp"]
            ts_ms = int(ts.timestamp() * 1000) if hasattr(ts, "timestamp") else int(ts)
            out.append([
                ts_ms,
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                float(row["volume"]),
            ])
        return out

    def has(self, symbol: str, interval: str) -> bool:
        return self._cache.has(symbol, interval) if self._cache else False

    def release(self, symbols_to_keep: Set[str]) -> None:
        """Drop subscriptions for symbols not in ``symbols_to_keep``.
        Frees stream slots so we don't hit the 1,024-stream Binance cap
        as the active universe rotates."""
        if self._stopped or not self._cache or not self._loop:
            return
        asyncio.run_coroutine_threadsafe(
            self._cache.release(set(symbols_to_keep)), self._loop,
        )


# ── Singleton accessor ────────────────────────────────────────────────────

_default: Optional[WSKlinesCache] = None
_default_lock = threading.Lock()


def get_default_ws_cache(
    intervals: Iterable[str] = DEFAULT_INTERVALS,
    buffer_size: int = DEFAULT_BUFFER_SIZE,
) -> Optional[WSKlinesCache]:
    """Return a process-singleton WS klines cache, or None if it cannot be
    constructed (e.g. ``booknow`` package not on path). Callers should
    treat None as "WS unavailable, fall back to REST"."""
    global _default
    if _default is not None:
        return _default
    with _default_lock:
        if _default is None:
            try:
                _default = WSKlinesCache(intervals=intervals, buffer_size=buffer_size)
            except Exception as e:
                log.warning("[WSKlinesCache] unavailable, callers should fall back to REST: %s", e)
                return None
        return _default
