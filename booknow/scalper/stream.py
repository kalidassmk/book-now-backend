"""
stream.py
─────────────────────────────────────────────────────────────────────────────
Order-flow market-data fan-in — a sibling of :class:`MarketStreamService`.

Connects to one Binance *combined* stream carrying ``aggTrade`` and partial
``depth`` events for every configured scalper symbol, and dispatches each frame
to a callback ``(symbol, event_type, data)``. Auto-reconnects with exponential
backoff and honours the shared :class:`RateLimitGuard` (Binance IP-bans apply
to public streams too), matching the conventions in ``ws_streams.py``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
from time import time
from typing import Any, Callable, Dict

import websockets
import websockets.exceptions

from booknow.binance.rate_limit import get_default as _get_rate_limit_guard
from booknow.scalper.config import ScalperConfig


logger = logging.getLogger("booknow.scalper.stream")

_RECONNECT_BACKOFF_MAX_S = 30
_BAN_DEFAULT_COOLDOWN_S = 120

# Match the host's SSL convention used elsewhere in the engine (corporate
# proxies / missing CA bundles). Production should plug certifi in.
_SSL_CTX = ssl._create_unverified_context()


class OrderFlowStreamService:
    """Owner of the combined ``aggTrade`` + ``depth`` subscription."""

    def __init__(
        self,
        config: ScalperConfig,
        on_event: Callable[[str, str, Dict[str, Any]], None],
    ):
        """
        Args:
            config: scalper configuration (symbols, depth levels, URL).
            on_event: callback ``(symbol, event_type, data)`` where
                ``event_type`` is ``"aggTrade"`` or ``"depth"``.
        """
        self._config = config
        self._on_event = on_event
        self._guard = _get_rate_limit_guard()

        self._task: asyncio.Task | None = None
        self._running = False
        self.connected = False
        self.last_message_ts = 0.0

    # ── Lifecycle ────────────────────────────────────────────────────────

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_forever(), name="scalper-stream")
        logger.info(
            "[ScalperStream] task spawned (%d symbols: %s)",
            len(self._config.symbols), ",".join(self._config.symbols),
        )

    async def stop(self) -> None:
        self._running = False
        self.connected = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

    # ── Connection loop ──────────────────────────────────────────────────

    async def _run_forever(self) -> None:
        url = self._config.ws_url()
        backoff = 2
        while self._running:
            if self._guard.is_banned():
                secs = self._guard.ban_remaining_seconds()
                logger.warning("[ScalperStream] connect deferred — Binance ban for %ds", secs)
                await asyncio.sleep(min(secs + 1, _RECONNECT_BACKOFF_MAX_S))
                continue
            try:
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=15,
                    ssl=_SSL_CTX,
                    max_size=8 * 1024 * 1024,
                ) as ws:
                    self.connected = True
                    backoff = 2
                    logger.info("[ScalperStream] connected (%d symbols)", len(self._config.symbols))
                    async for raw in ws:
                        if not self._running:
                            break
                        self._on_frame(raw)
            except (websockets.exceptions.ConnectionClosed, OSError) as e:
                self.connected = False
                logger.warning("[ScalperStream] dropped: %s — reconnecting in %ds", e, backoff)
            except Exception as e:
                self.connected = False
                if self._guard.report_if_banned(e):
                    logger.error("[ScalperStream] ban detected — pausing %ds", _BAN_DEFAULT_COOLDOWN_S)
                    await asyncio.sleep(_BAN_DEFAULT_COOLDOWN_S)
                else:
                    logger.error("[ScalperStream] error: %s", e, exc_info=True)

            self.connected = False
            if not self._running:
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, _RECONNECT_BACKOFF_MAX_S)

        logger.info("[ScalperStream] stopped")

    # ── Per-frame dispatch ───────────────────────────────────────────────

    def _on_frame(self, raw) -> None:
        self.last_message_ts = time()
        try:
            msg = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return

        stream = msg.get("stream", "")
        data = msg.get("data", msg)
        if not stream:
            return

        # stream looks like "btcusdt@aggTrade" or "btcusdt@depth20@100ms".
        symbol_part, _, kind = stream.partition("@")
        symbol = symbol_part.upper()

        if kind.startswith("aggTrade"):
            self._on_event(symbol, "aggTrade", data)
        elif kind.startswith("depth"):
            self._on_event(symbol, "depth", data)
