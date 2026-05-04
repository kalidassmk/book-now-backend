"""
tickers_cache.py
─────────────────────────────────────────────────────────────────────────────
Sync-friendly cache of Binance Spot 24-hour mini-tickers.

Migrated from binance-sentiment-engine/tickers_ws_cache.py during the
python-engine consolidation. Public API unchanged so callers in the
sentiment engine keep working via the compatibility shim at the old
path.

Subscribes once to the public ``!miniTicker@arr`` stream and keeps a
per-symbol dict updated every second. Reads are O(1) and don't hit
Binance.

Designed for synchronous Python daemons that previously called
``ccxt.fetch_tickers()`` in a tight loop. The WebSocket runs in a
daemon thread with its own asyncio event loop; consumers call
``start()`` once and read.

Cached fields per symbol (mirroring the !miniTicker@arr payload):
    last         — close (last) price
    open         — 24h open price
    high         — 24h high
    low          — 24h low
    volume       — 24h base-asset volume
    quoteVolume  — 24h quote-asset (USDT) volume
    ts           — last update wall-clock time (epoch seconds)
"""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
import threading
import time
from typing import Dict, Optional

import websockets

logger = logging.getLogger("booknow.tickers_cache")

WS_URL = "wss://stream.binance.com:9443/ws/!miniTicker@arr"


class TickersCache:
    """Thread-backed singleton-ish cache. ``start()`` is idempotent."""

    def __init__(self):
        self._cache: Dict[str, dict] = {}
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._running = False
        self._first_frame_event = threading.Event()

    # ── Lifecycle ────────────────────────────────────────────────────────

    def start(self, wait_for_first_frame_seconds: float = 5.0) -> None:
        """
        Spawn the WS thread (idempotent) and optionally block until the
        first miniTicker frame lands. Pass 0 to return immediately.
        """
        if self._thread and self._thread.is_alive():
            return
        self._running = True
        self._first_frame_event.clear()
        self._thread = threading.Thread(
            target=self._thread_target, daemon=True, name="tickers-cache"
        )
        self._thread.start()
        if wait_for_first_frame_seconds > 0:
            self._first_frame_event.wait(timeout=wait_for_first_frame_seconds)

    def stop(self) -> None:
        self._running = False
        if self._loop and self._loop.is_running():
            try:
                self._loop.call_soon_threadsafe(self._loop.stop)
            except Exception:
                pass

    # ── Public reads (sync, thread-safe) ─────────────────────────────────

    def get_ticker(self, symbol: str) -> Optional[dict]:
        """Latest ticker for a symbol like 'BTCUSDT', or None if not seen yet."""
        if not symbol:
            return None
        return self._cache.get(symbol.upper())

    def get_all(self) -> Dict[str, dict]:
        """Snapshot of every symbol seen so far. Cheap to call."""
        with self._lock:
            return dict(self._cache)

    def has(self, symbol: str) -> bool:
        return bool(symbol) and symbol.upper() in self._cache

    # ── Internals ────────────────────────────────────────────────────────

    def _thread_target(self) -> None:
        # Each thread needs its own event loop because asyncio is not
        # thread-safe in general — and we don't want to interfere with
        # any caller that already has a main-thread event loop.
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._run_forever())
        except Exception as e:
            logger.error("[TickersCache] thread crashed: %s", e)
        finally:
            try:
                self._loop.close()
            except Exception:
                pass

    async def _run_forever(self) -> None:
        ssl_ctx = ssl._create_unverified_context()
        backoff = 2
        while self._running:
            try:
                async with websockets.connect(
                    WS_URL,
                    ping_interval=30,
                    ping_timeout=15,
                    ssl=ssl_ctx,
                    max_size=8 * 1024 * 1024,
                ) as ws:
                    logger.info("[TickersCache] WS connected to !miniTicker@arr")
                    backoff = 2
                    async for raw in ws:
                        if not self._running:
                            break
                        self._handle(raw)
            except (websockets.ConnectionClosed, OSError) as e:
                logger.warning("[TickersCache] WS dropped: %s — reconnecting in %ds", e, backoff)
            except Exception as e:
                logger.error("[TickersCache] WS error: %s — reconnecting in %ds", e, backoff)

            if self._running:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def _handle(self, raw) -> None:
        try:
            arr = json.loads(raw)
        except Exception:
            return
        if not isinstance(arr, list):
            return
        now = time.time()
        with self._lock:
            for t in arr:
                sym = t.get("s")
                if not sym:
                    continue
                try:
                    self._cache[sym] = {
                        "last":         float(t.get("c", 0)),
                        "open":         float(t.get("o", 0)),
                        "high":         float(t.get("h", 0)),
                        "low":          float(t.get("l", 0)),
                        "volume":       float(t.get("v", 0)),  # base-asset volume
                        "quoteVolume":  float(t.get("q", 0)),  # quote-asset volume (USDT for *USDT pairs)
                        "ts": now,
                    }
                except (TypeError, ValueError):
                    continue
        if not self._first_frame_event.is_set():
            self._first_frame_event.set()


# Convenience module-level singleton — most daemons just want one shared cache.
_default: Optional[TickersCache] = None


def get_default_cache(start: bool = True) -> TickersCache:
    """Lazily-built process-wide TickersCache. Safe to call from anywhere."""
    global _default
    if _default is None:
        _default = TickersCache()
        if start:
            _default.start()
    return _default
