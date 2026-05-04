"""
routes_binance.py
─────────────────────────────────────────────────────────────────────────────
``/api/v1/binance/*`` — direct port of Java's ``BinanceDashboardController``.

These endpoints proxy Binance-side reads (account, orders, trades) for
the dashboard. We add a tiny 10-second TTL cache, same as the Java
controller did, to keep dashboard polling from blowing weight limits.

Endpoints:
    GET  /api/v1/binance/account
    GET  /api/v1/binance/open-orders[?symbol=]
    GET  /api/v1/binance/trade-history?symbol=
    DELETE /api/v1/binance/trade/cancel?symbol=&orderId=
    POST /api/v1/binance/trade/modify          (cancel + replace)
    GET  /api/v1/binance/trade/order-history?symbol=
    GET  /api/v1/binance/trade/trade-history?symbol=[&startTime=&endTime=]
    GET  /api/v1/binance/trade/order-list[?startTime=&endTime=]
    GET  /api/v1/binance/btc-price             (Redis-backed)
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query

from booknow.api.deps import get_state
from booknow.api.state import AppState
from booknow.repository import redis_keys


router = APIRouter(prefix="/api/v1/binance", tags=["binance-dashboard"])
logger = logging.getLogger("booknow.api.binance")


# ── 10-second TTL cache, same as Java's CachedData ───────────────────────


_CACHE_TTL_S = 10.0
_cache: Dict[str, tuple] = {}  # key → (timestamp, data)


def _cache_get(key: str) -> Optional[Any]:
    entry = _cache.get(key)
    if entry is None:
        return None
    ts, data = entry
    if (time.monotonic() - ts) > _CACHE_TTL_S:
        _cache.pop(key, None)
        return None
    return data


def _cache_put(key: str, data: Any) -> None:
    _cache[key] = (time.monotonic(), data)


# ── Read endpoints ───────────────────────────────────────────────────────


@router.get("/account")
async def get_account(state: AppState = Depends(get_state)) -> Any:
    cached = _cache_get("account")
    if cached is not None:
        return cached
    try:
        account = await state.ws_api.get_account()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch account info: {e}")
    _cache_put("account", account)
    return account


@router.get("/open-orders")
async def get_open_orders(
    symbol: Optional[str] = Query(default=None),
    state: AppState = Depends(get_state),
) -> Any:
    cache_key = f"open-orders-{symbol or 'all'}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    try:
        orders = await state.ws_api.get_open_orders(symbol=symbol)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch open orders: {e}")
    _cache_put(cache_key, orders)
    return orders


@router.get("/trade-history")
async def get_trade_history(
    symbol: str = Query(...),
    state: AppState = Depends(get_state),
) -> Any:
    try:
        return await state.ws_api.get_my_trades(symbol)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch trades for {symbol}: {e}",
        )


@router.get("/trade/order-history")
async def get_order_history(
    symbol: str = Query(...),
    state: AppState = Depends(get_state),
) -> Any:
    try:
        return await state.ws_api.get_all_orders(symbol)
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.get("/trade/trade-history")
async def get_trade_history_v2(
    symbol: str = Query(...),
    startTime: Optional[int] = Query(default=None),
    endTime: Optional[int] = Query(default=None),
    state: AppState = Depends(get_state),
) -> Any:
    try:
        trades = await state.ws_api.get_my_trades(
            symbol, start_time=startTime, end_time=endTime, limit=500,
        )
        logger.info("[BINANCE] trade-history fetched %d rows for %s", len(trades), symbol)
        return trades
    except Exception as e:
        logger.error("[BINANCE] trade-history %s failed: %s", symbol, e)
        return {"ok": False, "error": str(e)}


@router.get("/trade/order-list")
async def get_order_list(
    startTime: Optional[int] = Query(default=None),
    endTime: Optional[int] = Query(default=None),
    state: AppState = Depends(get_state),
) -> Any:
    try:
        return await state.ws_api.get_all_order_lists(
            start_time=startTime, end_time=endTime, limit=500,
        )
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.get("/btc-price")
async def get_btc_price(state: AppState = Depends(get_state)) -> Dict[str, Any]:
    """Live BTC price from the Redis ``CURRENT_PRICE`` hash.

    Replaces Spring's ``binance/btc-price`` which made a fresh REST call
    every time. We piggy-back on the WS ticker stream the engine
    already maintains.
    """
    raw = await state.redis.hget(redis_keys.CURRENT_PRICE, "BTCUSDT")
    if not raw:
        raise HTTPException(status_code=503, detail="BTC price not available yet")
    import json
    try:
        cp = json.loads(raw)
    except (TypeError, ValueError):
        raise HTTPException(status_code=503, detail="BTC price unparseable")
    return {"symbol": "BTCUSDT", "price": cp.get("price")}


# ── Write endpoints ──────────────────────────────────────────────────────


@router.delete("/trade/cancel")
async def cancel_order(
    symbol: str = Query(...),
    orderId: int = Query(...),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        await state.ws_api.cancel_order(symbol=symbol, order_id=orderId)
    except Exception as e:
        return {"ok": False, "error": str(e)}
    return {"ok": True, "symbol": symbol, "orderId": orderId}


@router.post("/trade/modify")
async def modify_order(
    payload: Dict[str, Any] = Body(...),
    state: AppState = Depends(get_state),
) -> Any:
    """Cancel + replace. Mirrors Java's two-step approach so the
    dashboard's "Edit Order" button keeps working.
    """
    try:
        symbol = str(payload["symbol"])
        old_order_id = int(payload["orderId"])
        price = str(payload["price"])
        quantity = str(payload["quantity"])
        side = str(payload["side"]).upper()
    except (KeyError, ValueError, TypeError) as e:
        raise HTTPException(status_code=400, detail=f"Invalid modify payload: {e}")

    try:
        # 1) cancel old
        await state.ws_api.cancel_order(symbol=symbol, order_id=old_order_id)
        # 2) place new
        resp = await state.ws_api.place_order(
            symbol=symbol,
            side=side,
            order_type="LIMIT",
            quantity=quantity,
            price=price,
            time_in_force="GTC",
        )
    except Exception as e:
        return {"ok": False, "error": str(e)}
    return resp
