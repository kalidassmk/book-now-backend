"""
user_data.py
─────────────────────────────────────────────────────────────────────────────
Async port of ``BinanceUserDataStreamService.java``.

Subscribes to the Binance Spot user-data-stream and dispatches every
event to the rest of the engine. Replaces what used to be REST polling:

  - 5-minute ``getAccount()`` poll  →  real-time
    ``outboundAccountPosition`` and ``balanceUpdate`` pushes (handled
    by :class:`BalanceService`).
  - On-demand ``getOrderStatus()`` polling for limit-sell fills →
    real-time ``executionReport`` pushes (will be wired into
    ``TradeState`` and the position monitor in later phases).

Lifecycle (mirrors the Java implementation):

  1. Engine boot → ``start()`` calls ``userDataStream.start`` over the
     WS-API to obtain a 60-minute ``listenKey``, then opens a stream
     subscription at ``wss://stream.binance.com:9443/ws/<listenKey>``.
  2. A keepalive task pings the listenKey every 25 minutes (Binance
     keeps it alive 60 min after the last ping; 25 min gives plenty of
     margin and survives ~30s of clock drift).
  3. On websocket disconnect → reconnect: fetch a new listenKey, open
     a fresh stream. Auto-resubscribe is a no-op since the listenKey
     IS the subscription.
  4. Engine shutdown → ``stop()`` closes the stream socket and calls
     ``userDataStream.stop`` so Binance can reclaim the slot
     (concurrent listenKeys per account are capped).

Rate-limit guard integration:
  - Pre-flight: skip listenKey calls when the shared
    :class:`RateLimitGuard` reports a ban.
  - Post-failure: parse ``BinanceIpBannedException`` from the WS-API
    layer and let the guard trip the global cool-down.
"""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
from typing import Any, Awaitable, Callable, Dict, Optional

import websockets
import websockets.exceptions

from booknow.binance.balances import BalanceService
from booknow.binance.rate_limit import get_default as _get_rate_limit_guard
from booknow.binance.ws_api import (
    BinanceIpBannedException,
    WsApiClient,
    extract_retry_after_seconds,
    is_ip_ban,
)


logger = logging.getLogger("booknow.user_data")

_STREAM_BASE = "wss://stream.binance.com:9443/ws"
_KEEPALIVE_INTERVAL_S = 25 * 60   # listenKey TTL is 60 min; refresh at 25 min
_RECONNECT_BACKOFF_MAX_S = 30
_BAN_DEFAULT_COOLDOWN_S = 120

# Same convention klines_cache / tickers_cache / ws_api use on this host.
_SSL_CTX = ssl._create_unverified_context()

# Optional callback to invoke for raw executionReport events. Phase 9
# (TradeState + position monitor) will plug a real handler in here; for
# now we only log.
ExecutionReportHandler = Callable[[Dict[str, Any]], Awaitable[None]]


class UserDataStreamService:
    """Owns the listenKey lifecycle + stream subscription.

    Wire one instance into the engine and ``await service.start()``.
    The service spawns two background tasks:

      - ``_run_stream``: connects to ``/ws/<listenKey>``, parses events,
        dispatches to handlers, reconnects on disconnect.
      - ``_run_keepalive``: pings the listenKey every 25 minutes.

    Both tasks honour ``self._running`` so ``stop()`` is graceful.
    """

    def __init__(
        self,
        ws_api: WsApiClient,
        balance_service: BalanceService,
        on_execution_report: Optional[ExecutionReportHandler] = None,
    ):
        self.ws_api = ws_api
        self.balances = balance_service
        self.on_execution_report = on_execution_report
        self._guard = _get_rate_limit_guard()

        self._listen_key: Optional[str] = None
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._stream_task: Optional[asyncio.Task] = None
        self._keepalive_task: Optional[asyncio.Task] = None
        self._running = False

    # ── Lifecycle ────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Spawn the background tasks. Idempotent."""
        if self._running:
            return
        self._running = True
        self._stream_task = asyncio.create_task(self._run_stream(), name="user-data-stream")
        self._keepalive_task = asyncio.create_task(self._run_keepalive(), name="user-data-keepalive")
        logger.info("[UserDataStream] tasks spawned (stream + keepalive)")

    async def stop(self) -> None:
        """Cancel tasks, close socket, release listenKey. Best-effort."""
        self._running = False

        for task in (self._stream_task, self._keepalive_task):
            if task is not None and not task.done():
                task.cancel()
        for task in (self._stream_task, self._keepalive_task):
            if task is not None:
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        self._stream_task = None
        self._keepalive_task = None

        await self._close_socket()

        key = self._listen_key
        self._listen_key = None
        if key is not None and not self._guard.is_banned():
            try:
                await self.ws_api.close_user_data_stream(key)
                logger.info("[UserDataStream] Closed listenKey on shutdown")
            except Exception as e:
                logger.warning("[UserDataStream] Error closing listenKey on shutdown: %s", e)

    # ── Background tasks ─────────────────────────────────────────────────

    async def _run_stream(self) -> None:
        """Connect → consume → reconnect, forever (or until stop())."""
        backoff = 2
        while self._running:
            # Acquire a listenKey if we don't have one (or the previous
            # connection failed). Honour the rate-limit guard.
            if self._listen_key is None:
                if self._guard.is_banned():
                    secs = self._guard.ban_remaining_seconds()
                    logger.warning(
                        "[UserDataStream] listenKey acquisition deferred — Binance ban for %ds", secs,
                    )
                    await asyncio.sleep(min(secs + 1, _RECONNECT_BACKOFF_MAX_S))
                    continue
                try:
                    self._listen_key = await self.ws_api.start_user_data_stream()
                    logger.info(
                        "[UserDataStream] Obtained listenKey=%s...%s",
                        self._listen_key[:4], self._listen_key[-4:],
                    )
                    backoff = 2
                except BinanceIpBannedException as e:
                    cool = extract_retry_after_seconds(e) or _BAN_DEFAULT_COOLDOWN_S
                    logger.error("[UserDataStream] Binance IP BAN — sleeping %ds", cool)
                    await asyncio.sleep(cool)
                    continue
                except Exception as e:
                    if self._guard.report_if_banned(e):
                        await asyncio.sleep(_BAN_DEFAULT_COOLDOWN_S)
                        continue
                    logger.error("[UserDataStream] listenKey start failed: %s", e)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, _RECONNECT_BACKOFF_MAX_S)
                    continue

            # Open the stream socket and pump frames.
            url = f"{_STREAM_BASE}/{self._listen_key}"
            try:
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=15,
                    ssl=_SSL_CTX,
                    max_size=4 * 1024 * 1024,
                ) as ws:
                    self._ws = ws
                    logger.info(
                        "[UserDataStream] Stream connected — balance + order updates are now push-driven",
                    )
                    backoff = 2
                    async for raw in ws:
                        if not self._running:
                            break
                        await self._handle_raw(raw)
            except (websockets.exceptions.ConnectionClosed, OSError) as e:
                logger.warning("[UserDataStream] Stream dropped: %s — reconnecting in %ds", e, backoff)
            except Exception as e:
                if is_ip_ban(e) or self._guard.report_if_banned(e):
                    logger.error("[UserDataStream] Ban detected on stream — pausing reconnects")
                    await asyncio.sleep(_BAN_DEFAULT_COOLDOWN_S)
                else:
                    logger.error("[UserDataStream] Stream error: %s", e)
            finally:
                self._ws = None

            if not self._running:
                break

            # Reconnect path. The listenKey could still be valid (Binance
            # gives 60 min total), so reuse it; on the next 25-min keepalive
            # tick we'll refresh it. If Binance kicked us with a 4xx, the
            # NEXT iteration of this loop will catch the 4xx on connect()
            # and we'll re-acquire.
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, _RECONNECT_BACKOFF_MAX_S)

    async def _run_keepalive(self) -> None:
        """Refresh the listenKey every 25 minutes."""
        while self._running:
            try:
                await asyncio.sleep(_KEEPALIVE_INTERVAL_S)
            except asyncio.CancelledError:
                break

            if not self._running:
                break

            if self._guard.is_banned():
                logger.warning(
                    "[UserDataStream] Skipping keepalive — Binance ban active for %ds",
                    self._guard.ban_remaining_seconds(),
                )
                continue

            key = self._listen_key
            if key is None:
                logger.info("[UserDataStream] keepalive: no listenKey, the stream loop will acquire one")
                continue

            try:
                await self.ws_api.keep_alive_user_data_stream(key)
                logger.debug("[UserDataStream] Keepalive ping OK")
            except BinanceIpBannedException as e:
                logger.error(
                    "[UserDataStream] Keepalive hit Binance ban (%ds). Forcing fresh listenKey on reconnect.",
                    extract_retry_after_seconds(e) or _BAN_DEFAULT_COOLDOWN_S,
                )
                await self._close_socket()
                self._listen_key = None
            except Exception as e:
                if self._guard.report_if_banned(e):
                    await self._close_socket()
                    self._listen_key = None
                    continue
                logger.warning("[UserDataStream] Keepalive failed (%s) — forcing reconnect", e)
                await self._close_socket()
                self._listen_key = None

    # ── Event dispatch ───────────────────────────────────────────────────

    async def _handle_raw(self, raw: str) -> None:
        try:
            event = json.loads(raw)
        except json.JSONDecodeError:
            return
        event_type = event.get("e")
        if not event_type:
            return
        try:
            if event_type in ("outboundAccountPosition", "outboundAccountInfo"):
                # B is the balance array on both event variants.
                await self.balances.apply_account_snapshot(event.get("B") or [])
            elif event_type == "balanceUpdate":
                await self.balances.apply_balance_delta(
                    event.get("a"), event.get("d") or "0",
                )
            elif event_type == "executionReport":
                await self._on_execution_report(event)
            else:
                logger.debug("[UserDataStream] Ignoring event type: %s", event_type)
        except Exception as e:
            logger.error("[UserDataStream] Handler error on %s: %s", event_type, e, exc_info=True)

    async def _on_execution_report(self, event: Dict[str, Any]) -> None:
        """Order lifecycle event.

        Phase 9 will plug in the TradeState + position monitor handler.
        For now we log every fill and forward the raw event to any
        externally-registered handler.
        """
        logger.info(
            "[UserDataStream] order %s %s %s status=%s executedQty=%s price=%s orderId=%s",
            event.get("s"), event.get("S"), event.get("o"),
            event.get("X"), event.get("z"), event.get("p"), event.get("i"),
        )
        if self.on_execution_report is not None:
            try:
                await self.on_execution_report(event)
            except Exception as e:
                logger.error("[UserDataStream] external execution handler failed: %s", e, exc_info=True)

    # ── Internals ────────────────────────────────────────────────────────

    async def _close_socket(self) -> None:
        ws = self._ws
        self._ws = None
        if ws is not None:
            try:
                await ws.close()
            except Exception:
                pass
