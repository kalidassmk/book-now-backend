"""
ws_api.py
─────────────────────────────────────────────────────────────────────────────
Async client for Binance Spot WebSocket API
(``wss://ws-api.binance.com/ws-api/v3``).

Direct port of ``BinanceWsApiClient.java`` plus the signed methods we
need for trading:

    Unsigned (apiKey only)
        userDataStream.start            → :py:meth:`start_user_data_stream`
        userDataStream.ping             → :py:meth:`keep_alive_user_data_stream`
        userDataStream.stop             → :py:meth:`close_user_data_stream`

    Signed (HMAC-SHA256 over sorted query)
        order.place                     → :py:meth:`place_order`
        order.cancel                    → :py:meth:`cancel_order`
        order.status                    → :py:meth:`get_order_status`
        openOrders.status               → :py:meth:`get_open_orders`
        account.status                  → :py:meth:`get_account`

Each call opens a short-lived WebSocket, sends one JSON-RPC frame,
awaits one response frame, and closes — matches the Java implementation
and avoids the complexity of session.logon (which would need Ed25519
keys). Latency per call is ~150-300 ms, fine for our trade frequency
and the listenKey keepalive cadence.

Why not use REST: Binance retired ``POST /api/v3/userDataStream``
(returns 410 Gone). The WS-API is the supported path. We also want
order placement here so the engine has a single transport for all
authenticated Binance operations.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import ssl
import time
import uuid
from typing import Any, Dict, List, Optional

import websockets
import websockets.exceptions

from booknow.binance.rate_limit import get_default as _get_rate_limit_guard


logger = logging.getLogger("booknow.ws_api")

WS_API_URL = "wss://ws-api.binance.com:443/ws-api/v3"
DEFAULT_TIMEOUT_S = 15.0

# Match the convention used by klines_cache / tickers_cache: relax cert
# verification because the host's CA bundle frequently lacks the chain
# Binance presents (especially behind corporate proxies). Cloud-hosted
# deployments should use a proper SSLContext built from certifi.
_SSL_CTX = ssl._create_unverified_context()


# ── Typed ban exception ────────────────────────────────────────────────────


class BinanceIpBannedException(RuntimeError):
    """Raised when Binance returns 418/429 to a WS-API request.

    Surfaces ``retry_after_seconds`` so callers can honour the cool-down
    instead of retrying every 25 minutes (which only deepens the ban).
    """

    def __init__(self, http_code: int, retry_after_seconds: int, message: str = ""):
        self.http_code = http_code
        self.retry_after_seconds = retry_after_seconds
        super().__init__(
            f"HTTP {http_code} from Binance — IP temporarily banned. "
            f"Retry-After: {retry_after_seconds}s. {message}"
        )


def is_ip_ban(exc: BaseException) -> bool:
    """Walk an exception chain looking for a ban marker."""
    cur: Optional[BaseException] = exc
    while cur is not None:
        if isinstance(cur, BinanceIpBannedException):
            return True
        cur = cur.__cause__ or cur.__context__
    return False


def extract_retry_after_seconds(exc: BaseException) -> int:
    cur: Optional[BaseException] = exc
    while cur is not None:
        if isinstance(cur, BinanceIpBannedException):
            return cur.retry_after_seconds
        cur = cur.__cause__ or cur.__context__
    return 0


# ── Client ─────────────────────────────────────────────────────────────────


class WsApiClient:
    """Short-lived-WS, request/response Binance WS-API client.

    Usage::

        client = WsApiClient(api_key, secret_key)
        listen_key = await client.start_user_data_stream()
        order = await client.place_order(
            symbol="BTCUSDT", side="BUY", order_type="MARKET", quote_order_qty="12"
        )

    All methods raise :class:`BinanceIpBannedException` when the IP is
    banned, and a generic :class:`RuntimeError` for other transport or
    server errors. The shared
    :func:`booknow.binance.rate_limit.get_default()` guard is updated on
    every detected ban so other engine tasks halt in sync.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        timeout_seconds: float = DEFAULT_TIMEOUT_S,
    ):
        self.api_key = api_key or ""
        self.secret_key = secret_key or ""
        self.timeout = timeout_seconds
        self._guard = _get_rate_limit_guard()

    # ── userDataStream lifecycle (unsigned) ──────────────────────────────

    async def start_user_data_stream(self) -> str:
        """Obtain a 60-minute spot user-data listenKey."""
        if not self.api_key:
            raise ValueError("api_key required for userDataStream.start")
        result = await self._call("userDataStream.start", {"apiKey": self.api_key})
        listen_key = (result or {}).get("listenKey") if isinstance(result, dict) else None
        if not listen_key:
            raise RuntimeError(f"WS-API userDataStream.start returned no listenKey: {result!r}")
        return listen_key

    async def keep_alive_user_data_stream(self, listen_key: str) -> None:
        """Refresh a listenKey's TTL (call every ~25 minutes)."""
        await self._call("userDataStream.ping",
                         {"apiKey": self.api_key, "listenKey": listen_key})

    async def close_user_data_stream(self, listen_key: str) -> None:
        """Best-effort close — lets Binance reclaim the listenKey slot."""
        await self._call("userDataStream.stop",
                         {"apiKey": self.api_key, "listenKey": listen_key})

    # ── Order lifecycle (HMAC-SHA256 signed) ─────────────────────────────

    async def place_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: Optional[str] = None,
        quote_order_qty: Optional[str] = None,
        price: Optional[str] = None,
        time_in_force: Optional[str] = None,
        new_client_order_id: Optional[str] = None,
        recv_window: int = 5000,
    ) -> Dict[str, Any]:
        """Place a spot order. Mirrors REST ``POST /api/v3/order`` params.

        For a +$0.20 fast-scalp MARKET buy::

            await client.place_order("BTCUSDT", "BUY", "MARKET",
                                     quote_order_qty="12")

        For a LIMIT sell at a fixed target::

            await client.place_order("BTCUSDT", "SELL", "LIMIT",
                                     quantity="0.0001",
                                     price="78950.00",
                                     time_in_force="GTC")
        """
        params: Dict[str, Any] = {
            "symbol": symbol.upper(),
            "side": side.upper(),
            "type": order_type.upper(),
        }
        if quantity is not None:
            params["quantity"] = quantity
        if quote_order_qty is not None:
            params["quoteOrderQty"] = quote_order_qty
        if price is not None:
            params["price"] = price
        if time_in_force is not None:
            params["timeInForce"] = time_in_force.upper()
        if new_client_order_id is not None:
            params["newClientOrderId"] = new_client_order_id
        return await self._signed_call("order.place", params, recv_window)

    async def cancel_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        orig_client_order_id: Optional[str] = None,
        recv_window: int = 5000,
    ) -> Dict[str, Any]:
        """Cancel an order. Provide ``order_id`` OR ``orig_client_order_id``."""
        if order_id is None and orig_client_order_id is None:
            raise ValueError("cancel_order: order_id or orig_client_order_id required")
        params: Dict[str, Any] = {"symbol": symbol.upper()}
        if order_id is not None:
            params["orderId"] = order_id
        if orig_client_order_id is not None:
            params["origClientOrderId"] = orig_client_order_id
        return await self._signed_call("order.cancel", params, recv_window)

    async def get_order_status(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        orig_client_order_id: Optional[str] = None,
        recv_window: int = 5000,
    ) -> Dict[str, Any]:
        """Single-order status query."""
        if order_id is None and orig_client_order_id is None:
            raise ValueError("get_order_status: order_id or orig_client_order_id required")
        params: Dict[str, Any] = {"symbol": symbol.upper()}
        if order_id is not None:
            params["orderId"] = order_id
        if orig_client_order_id is not None:
            params["origClientOrderId"] = orig_client_order_id
        return await self._signed_call("order.status", params, recv_window)

    async def get_open_orders(
        self, symbol: Optional[str] = None, recv_window: int = 5000,
    ) -> List[Dict[str, Any]]:
        """List open orders. Without ``symbol`` returns all pairs (heavy)."""
        params: Dict[str, Any] = {}
        if symbol is not None:
            params["symbol"] = symbol.upper()
        result = await self._signed_call("openOrders.status", params, recv_window)
        return result if isinstance(result, list) else []

    async def get_account(self, recv_window: int = 5000) -> Dict[str, Any]:
        """Account info incl. balances, fees, status flags."""
        return await self._signed_call("account.status", {}, recv_window)

    async def get_all_orders(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 500,
        recv_window: int = 5000,
    ) -> List[Dict[str, Any]]:
        """Historical order list for ``symbol``.

        Mirrors REST ``GET /api/v3/allOrders``. Used by the dashboard's
        order-history view.
        """
        params: Dict[str, Any] = {"symbol": symbol.upper(), "limit": limit}
        if order_id is not None:
            params["orderId"] = order_id
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        result = await self._signed_call("allOrders", params, recv_window)
        return result if isinstance(result, list) else []

    async def get_my_trades(
        self,
        symbol: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        from_id: Optional[int] = None,
        limit: int = 500,
        recv_window: int = 5000,
    ) -> List[Dict[str, Any]]:
        """Account trade list for ``symbol``.

        Mirrors REST ``GET /api/v3/myTrades``. Powers the dashboard's
        trade-history view.
        """
        params: Dict[str, Any] = {"symbol": symbol.upper(), "limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        if from_id is not None:
            params["fromId"] = from_id
        result = await self._signed_call("myTrades", params, recv_window)
        return result if isinstance(result, list) else []

    async def get_all_order_lists(
        self,
        from_id: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 500,
        recv_window: int = 5000,
    ) -> List[Dict[str, Any]]:
        """OCO/order-list history.

        Mirrors REST ``GET /api/v3/allOrderList``. Used by the
        dashboard's "Trade Pairs" view (matched buy + sell).
        """
        params: Dict[str, Any] = {"limit": limit}
        if from_id is not None:
            params["fromId"] = from_id
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        result = await self._signed_call("allOrderLists", params, recv_window)
        return result if isinstance(result, list) else []

    # ── Internals ────────────────────────────────────────────────────────

    async def _signed_call(
        self,
        method: str,
        params: Dict[str, Any],
        recv_window: int,
    ) -> Any:
        """Add timestamp + apiKey + HMAC signature, then dispatch."""
        if not self.api_key or not self.secret_key:
            raise ValueError(f"{method} requires api_key and secret_key")
        # Per WS-API docs: signature is HMAC-SHA256 over the alphabetically
        # sorted query string of all params (incl. apiKey + timestamp +
        # recvWindow). Computed AFTER all data fields are added.
        signed = dict(params)
        signed["apiKey"] = self.api_key
        signed["timestamp"] = int(time.time() * 1000)
        signed["recvWindow"] = recv_window
        signed["signature"] = self._sign(signed)
        return await self._call(method, signed)

    def _sign(self, params: Dict[str, Any]) -> str:
        """HMAC-SHA256 signature of sorted query string."""
        query = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        return hmac.new(
            self.secret_key.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    async def _call(self, method: str, params: Dict[str, Any]) -> Any:
        """Open WS, send one request, await one response, close.

        Translates server errors and 418/429 upgrade rejections into
        Python exceptions, and reports bans into the shared rate-limit
        guard so other engine tasks pause in sync.
        """
        # Don't bother knocking if we're known-banned. Let the caller
        # decide whether to wait or fail; we just surface the cool-down.
        if self._guard.is_banned():
            raise BinanceIpBannedException(
                418,
                self._guard.ban_remaining_seconds(),
                f"deferred {method}: cool-down active",
            )

        # Binance requires id ∈ ^[a-zA-Z0-9-_]{1,36}$. UUID4 is exactly
        # 36 chars [a-f0-9-] and matches.
        request_id = str(uuid.uuid4())
        payload = json.dumps({"id": request_id, "method": method, "params": params})

        try:
            raw = await asyncio.wait_for(
                self._send_recv(payload),
                timeout=self.timeout,
            )
        except asyncio.TimeoutError as e:
            raise RuntimeError(f"WS-API {method} timed out after {self.timeout}s") from e
        except BinanceIpBannedException:
            raise
        except Exception as e:
            # Catch the upgrade-time 418/429 exposed by the websockets lib
            # as InvalidStatus / InvalidStatusCode. The lib's __str__ on
            # those errors looks like:
            #   "server rejected WebSocket connection: HTTP 418"
            msg = str(e)
            if "HTTP 418" in msg or "HTTP 429" in msg:
                code = 418 if "418" in msg else 429
                self._guard.record_ban_until(
                    int(time.time() * 1000) + 120 * 1000,
                    f"WS-API {method} upgrade rejected: {msg[:160]}",
                )
                raise BinanceIpBannedException(code, 120, msg) from e
            # Other text-based ban indicators ("banned until N", "too much
            # request weight", "teapot") — let the guard parse + record.
            if self._guard.report_if_banned(e):
                raise BinanceIpBannedException(
                    418, self._guard.ban_remaining_seconds(),
                    f"WS-API {method}: {msg[:160]}",
                ) from e
            raise RuntimeError(f"WS-API {method}: {msg}") from e

        try:
            response = json.loads(raw)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"WS-API {method} non-JSON reply: {raw[:200]}") from e

        status = response.get("status", 0)
        if status >= 400:
            err = response.get("error") or {}
            err_msg = (
                err.get("msg") if isinstance(err, dict) else None
            ) or (
                err.get("message") if isinstance(err, dict) else None
            ) or json.dumps(err) or "?"
            # Some error messages embed "banned until N" — surface to guard.
            full_msg = f"{method} ({status}): {err_msg}"
            if self._guard.report_if_banned(RuntimeError(full_msg)):
                raise BinanceIpBannedException(
                    status, self._guard.ban_remaining_seconds(), full_msg,
                )
            raise RuntimeError(f"Binance WS-API {full_msg}")

        return response.get("result")

    async def _send_recv(self, payload: str) -> str:
        """Open one WS, send, await one frame, close. Coroutine for wait_for."""
        async with websockets.connect(
            WS_API_URL,
            ping_interval=20,
            ping_timeout=15,
            ssl=_SSL_CTX,
            max_size=4 * 1024 * 1024,
        ) as ws:
            await ws.send(payload)
            return await ws.recv()
