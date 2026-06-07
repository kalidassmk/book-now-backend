"""
routes_trading.py
─────────────────────────────────────────────────────────────────────────────
``/api/v1/*`` — direct port of Java's ``BookNowController``.

Every route here mirrors the Spring contract the dashboard already
consumes, so the existing JS in ``dashboard/server.js`` keeps working
once we point ``SPRING_BASE`` at the python-engine port.

Endpoints:
    GET  /api/v1/start                          — pipeline already runs;
                                                  reports running state
    GET  /api/v1/stop                           — request graceful stop
    GET  /api/v1/health                         — ping
    GET  /api/v1/sell/{symbol}?qty=             — manual sell
    GET  /api/v1/order/buy/{symbol}?qty=        — manual market buy
    GET  /api/v1/order/limit-buy/{symbol}?...   — manual limit buy
    GET  /api/v1/analyze/{symbol}               — coin analysis
    GET  /api/v1/order/status/{symbol}/{id}     — single-order status
    GET  /api/v1/order/cancel/{symbol}/{id}     — cancel order
    GET  /api/v1/orders/open                    — all open orders
"""

from __future__ import annotations

import json
import logging
import os
import signal
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse, PlainTextResponse

from booknow.api.deps import get_state
from booknow.api.state import AppState
from booknow.repository import redis_keys


router = APIRouter(prefix="/api/v1", tags=["trading"])
logger = logging.getLogger("booknow.api.trading")


# ── Lifecycle ────────────────────────────────────────────────────────────


@router.get("/start", response_class=PlainTextResponse)
async def start(state: AppState = Depends(get_state)) -> str:
    """Java returned "started" / "already running". The Python engine
    boots the pipeline at process start so by the time this endpoint is
    reachable the answer is always "running"."""
    return "Pipeline already running."


@router.get("/stop", response_class=PlainTextResponse)
async def stop(state: AppState = Depends(get_state)) -> str:
    """Request a graceful shutdown.

    main.py's stop sequence is wired to ``SIGTERM``; sending it here
    matches Java's shutdown semantics (return immediately, let the
    bootstrap finalizer drain everything).
    """
    logger.info("[/stop] dashboard requested shutdown — sending SIGTERM to self")
    os.kill(os.getpid(), signal.SIGTERM)
    return "Pipeline stopped."


@router.get("/health", response_class=PlainTextResponse)
async def health() -> str:
    return "BookNow is up."


# ── Manual trading ───────────────────────────────────────────────────────


async def _resolve_current_price(state: AppState, symbol: str) -> Optional[Dict[str, Any]]:
    """Read the live price hash that ``MarketStreamService`` keeps fresh.

    Returns the parsed JSON value (matches the shape Spring's
    ``CurrentPrice`` POJO had: ``{price, percentage, ...}``).
    """
    raw = await state.redis.hget(redis_keys.CURRENT_PRICE, symbol)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return None


@router.get("/sell/{symbol}", response_class=PlainTextResponse)
async def manual_sell(
    symbol: str,
    qty: Optional[float] = Query(default=None),
    state: AppState = Depends(get_state),
) -> str:
    cp = await _resolve_current_price(state, symbol)
    if cp is None:
        raise HTTPException(
            status_code=400,
            detail=f"No live price found for {symbol}. Is the pipeline running?",
        )
    await state.trade_executor.try_manual_sell(
        symbol, cp, qty=qty, rule_label="MANUAL_DASHBOARD",
    )
    return f"Sell executed for {symbol} @ {cp.get('price')}"


@router.get("/order/buy/{symbol}")
async def manual_market_buy(
    symbol: str,
    qty: float = Query(..., ge=0),
    state: AppState = Depends(get_state),
) -> Any:
    cp = await _resolve_current_price(state, symbol)
    if cp is None:
        raise HTTPException(
            status_code=400,
            detail=f"No live price for {symbol}. Is the pipeline running?",
        )
    resp = await state.trade_executor.try_manual_market_buy(symbol, cp, qty)
    if resp is None:
        raise HTTPException(status_code=400, detail="Failed to place market order.")
    return resp


@router.get("/order/limit-buy/{symbol}")
async def manual_limit_buy(
    symbol: str,
    qty: float = Query(default=0, ge=0),
    offsetPct: float = Query(default=0.3),
    profitPct: float = Query(default=2.0),
    state: AppState = Depends(get_state),
) -> Any:
    cp = await _resolve_current_price(state, symbol)
    if cp is None:
        raise HTTPException(
            status_code=400,
            detail=f"No live price for {symbol}. Is the pipeline running?",
        )
    resp = await state.trade_executor.try_manual_limit_buy(
        symbol, cp, manual_qty=qty, offset_pct=offsetPct, profit_pct=profitPct,
    )
    if resp is None:
        raise HTTPException(status_code=400, detail="Failed to place limit order.")
    return resp


# iter 130 — Quick Trade SELL endpoints.
#
# The legacy /api/v1/sell/{symbol} is gated by HARD_DISABLE_AUTOSELL
# (iter94) so bot paths can't trigger sells.  The Quick Trade panel is
# the operator's EXPLICIT manual exit tool, so these two routes bypass
# the kill switch.  Bot paths still hit the old endpoint.

@router.get("/order/market-sell/{symbol}")
async def manual_market_sell(
    symbol: str,
    qty: Optional[float] = Query(default=None),
    state: AppState = Depends(get_state),
) -> Any:
    cp = await _resolve_current_price(state, symbol)
    if cp is None:
        raise HTTPException(
            status_code=400,
            detail=f"No live price for {symbol}. Is the pipeline running?",
        )
    resp = await state.trade_executor.try_manual_sell(
        symbol, cp, qty=qty,
        rule_label="QUICK_TRADE_MARKET",
        bypass_kill_switch=True,
    )
    if resp is None:
        raise HTTPException(status_code=400, detail="Failed to place market sell.")
    return resp


@router.get("/order/limit-sell/{symbol}")
async def manual_limit_sell(
    symbol: str,
    qty: float = Query(..., ge=0),
    price: float = Query(..., ge=0),
    state: AppState = Depends(get_state),
) -> Any:
    if qty <= 0:
        raise HTTPException(status_code=400, detail="qty must be > 0")
    if price <= 0:
        raise HTTPException(status_code=400, detail="price must be > 0")
    resp = await state.trade_executor.try_manual_limit_sell(
        symbol, qty=qty, price=price, rule_label="QUICK_TRADE_LIMIT",
    )
    if resp is None:
        raise HTTPException(status_code=400, detail="Failed to place limit sell.")
    return resp


@router.post("/order/pattern-buy/{symbol}")
async def pattern_bot_buy(
    symbol: str,
    sell_pct: float = Query(default=5.0, ge=0.0, le=20.0),
    rule_label: str = Query(default="PATTERN_BOT"),
    state: AppState = Depends(get_state),
) -> Any:
    """Delegation endpoint used by the frontend's pattern-bot-worker.

    Routes a Pattern Bot signal through the same try_buy() path R1/R2/R3
    use, which gives the position the full iter47 + iter48 protection set:
      - check-coin filter pipeline (pre-buy)
      - hard stop-loss (HARD_SL)
      - dynamic chasing TP (MOVE_UP / FLOOR_SELL)
      - TSL, max-hold
    The trade lands in TradeState so the PositionMonitor manages exits.

    Returns a small JSON ack; the actual buy is async.
    """
    cp = await _resolve_current_price(state, symbol)
    if cp is None:
        raise HTTPException(
            status_code=400,
            detail=f"No live price for {symbol}. Is the pipeline running?",
        )
    # try_buy runs every gate (autoBuyEnabled, is_already_bought, delist,
    # check-coin, rate-limit) and short-circuits on any failure.  Caller
    # gets a 202-style "accepted, executing async" reply; the real status
    # shows up in METRICS:SKIP or BUY hash + logs.
    try:
        await state.trade_executor.try_buy(
            symbol=symbol,
            current_price_data=cp,
            sell_pct=sell_pct,
            rule_label=rule_label,
        )
    except Exception as e:
        logger.error("[pattern-buy] try_buy failed for %s: %s", symbol, e)
        raise HTTPException(status_code=500, detail=f"try_buy error: {e}")
    return {
        "status": "accepted",
        "symbol": symbol,
        "rule_label": rule_label,
        "current_price": cp.get("price"),
    }


# ── Analysis ─────────────────────────────────────────────────────────────


@router.get("/analyze/{symbol}")
async def analyze(symbol: str, state: AppState = Depends(get_state)) -> Any:
    """2-month coin analysis (CoinAnalysisResult).

    If CoinAnalyzer wasn't wired in main.py we still return a 200 with
    an empty-shape result, matching the dashboard's expectation that
    this endpoint is best-effort.
    """
    cp = await _resolve_current_price(state, symbol)
    price = float(cp.get("price")) if cp and cp.get("price") is not None else 0.0
    if state.coin_analyzer is None:
        return {
            "symbol": symbol,
            "currentPrice": price,
            "buyScore": 0,
            "recommendation": "UNAVAILABLE",
            "reason": "CoinAnalyzer not wired",
        }
    result = await state.coin_analyzer.analyze(symbol, price)
    return result.to_dict()


# ── Order management ─────────────────────────────────────────────────────


@router.get("/order/status/{symbol}/{order_id}")
async def get_order_status(
    symbol: str,
    order_id: int,
    state: AppState = Depends(get_state),
) -> Any:
    try:
        order = await state.ws_api.get_order_status(symbol=symbol, order_id=order_id)
    except Exception as e:
        # Match Java's "404 if missing" by mapping any failure here.
        logger.warning("[/order/status] %s/%s failed: %s", symbol, order_id, e)
        raise HTTPException(status_code=404, detail=str(e))
    return order


@router.get("/order/cancel/{symbol}/{order_id}", response_class=PlainTextResponse)
async def cancel_order(
    symbol: str,
    order_id: int,
    state: AppState = Depends(get_state),
) -> str:
    try:
        await state.trade_executor.cancel_order(symbol, order_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Cancel failed: {e}")
    return "Order cancelled successfully."


@router.get("/orders/open")
async def get_open_orders(state: AppState = Depends(get_state)) -> Any:
    try:
        orders = await state.ws_api.get_open_orders()
    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Failed to fetch open orders: {e}",
        )
    return orders
