"""
routes_scalper.py
─────────────────────────────────────────────────────────────────────────────
``/api/v1/scalper/*`` — live order-flow scalper surface for the dashboard.

Reads snapshots off the running :class:`ScalperEngine` (wired onto
:class:`AppState` in main.py). Exposes JSON endpoints for polling, a websocket
that pushes the full state at 1 Hz, and a self-contained HTML dashboard so the
feature is usable without the separate frontend build.

Endpoints:
    GET /api/v1/scalper/status
    GET /api/v1/scalper/snapshots
    GET /api/v1/scalper/snapshot/{symbol}
    GET /api/v1/scalper/signals?limit=50
    WS  /api/v1/scalper/ws
    GET /api/v1/scalper/dashboard          (HTML UI)
"""

from __future__ import annotations

import asyncio
import json
import logging

from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from booknow.api.deps import get_state
from booknow.api.state import AppState
from booknow.repository import redis_keys
from booknow.scalper.dashboard import DASHBOARD_HTML
from booknow.util.momentum import DEFAULT_DELIST_SEED


router = APIRouter(prefix="/api/v1/scalper", tags=["scalper"])
logger = logging.getLogger("booknow.api.scalper")


def _engine(state: AppState):
    engine = getattr(state, "scalper_engine", None)
    if engine is None:
        raise HTTPException(status_code=503, detail="Scalper engine not running")
    return engine


@router.get("/status")
async def scalper_status(state: AppState = Depends(get_state)):
    return _engine(state).status()


@router.get("/snapshots")
async def scalper_snapshots(state: AppState = Depends(get_state)):
    return {"success": True, "snapshots": _engine(state).get_all_snapshots()}


@router.get("/snapshot/{symbol}")
async def scalper_snapshot(symbol: str, state: AppState = Depends(get_state)):
    snap = _engine(state).get_snapshot(symbol)
    if snap is None:
        raise HTTPException(status_code=404, detail=f"Unknown symbol: {symbol}")
    return snap


@router.get("/signals")
async def scalper_signals(
    limit: int = Query(50, ge=1, le=100),
    state: AppState = Depends(get_state),
):
    return {"success": True, "signals": _engine(state).recent_signals(limit)}


@router.get("/delisted")
async def scalper_delisted(state: AppState = Depends(get_state)):
    """Binance delist details + which scalper coins are blocked (for verification).

    - ``scraped_delisted``: live ``BINANCE:DELIST:*`` from Redis — real delistings
      derived from Binance announcements. These are what the scalper blocks.
    - ``legacy_skip_seed``: the static ``DEFAULT_DELIST_SEED`` skip list (contains
      BTC/ETH and is NOT applied to the scalper) — shown only for reference.
    - ``scalper``: configured vs. active vs. blocked symbols.
    """
    engine = _engine(state)
    prefix = redis_keys.DELIST_PREFIX
    scraped = []
    try:
        keys = await state.redis.keys(f"{prefix}*")
        scraped = sorted(
            (k.split(prefix, 1)[1] if prefix in k else k) for k in keys
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("delisted: redis scan failed: %s", e)

    # iter 115 + iter 116 — pull per-symbol reasons.
    reasons: dict = {}
    try:
        rkeys = await state.redis.keys("BINANCE:DELIST_REASON:*")
        for k in rkeys:
            sym = k.split(":", 2)[-1]
            val = await state.redis.get(k)
            if val is not None:
                reasons[sym] = val
    except Exception as e:  # noqa: BLE001
        logger.warning("delisted: reason scan failed: %s", e)

    # iter 116 — also pull the announced delisting timestamps so the
    # dashboard can show "delisting in X hours" for PRE_DELISTING coins.
    delist_at: dict = {}
    try:
        atkeys = await state.redis.keys("BINANCE:DELIST_AT:*")
        for k in atkeys:
            sym = k.split(":", 2)[-1]
            val = await state.redis.get(k)
            if val is not None:
                try:
                    delist_at[sym] = int(val)
                except (TypeError, ValueError):
                    pass
    except Exception:
        pass

    # Bucket symbols by reason.  iter 116 adds PRE_DELISTING, BREAK,
    # HALT, AUCTION_MATCH buckets.
    by_reason: dict = {
        "PRE_DELISTING": [],
        "MONITORING": [],
        "SEED": [],
        "BREAK": [],
        "HALT": [],
        "AUCTION_MATCH": [],
        "ANNOUNCEMENT": [],
        "OTHER": [],
    }
    for s in scraped:
        r = reasons.get(s) or "ANNOUNCEMENT"
        by_reason.setdefault(r, []).append(s)
    for k in by_reason:
        by_reason[k].sort()

    return {
        "success": True,
        "scraped_delisted": scraped,
        "scraped_count": len(scraped),
        "by_reason": by_reason,
        "by_reason_counts": {k: len(v) for k, v in by_reason.items()},
        "reasons_map": reasons,
        "delist_at_ms": delist_at,   # iter 116 — PRE_DELISTING scheduled times
        "legacy_skip_seed": sorted(DEFAULT_DELIST_SEED),
        "scalper": {
            "configured": engine.configured_symbols,
            "active": engine.config.symbols,
            "blocked": engine.blocked_symbols,
            "active_count": len(engine.config.symbols),
            "blocked_count": len(engine.blocked_symbols),
        },
    }


@router.websocket("/ws")
async def scalper_ws(websocket: WebSocket):
    """Push status + snapshots + recent signals to the dashboard at 1 Hz."""
    await websocket.accept()
    state: AppState = websocket.app.state.engine_state
    engine = getattr(state, "scalper_engine", None)
    if engine is None:
        await websocket.send_text(json.dumps({"error": "scalper engine not running"}))
        await websocket.close()
        return
    try:
        while True:
            payload = {
                "status": engine.status(),
                "snapshots": engine.get_all_snapshots(),
                "signals": engine.recent_signals(20),
            }
            await websocket.send_text(json.dumps(payload, default=str))
            await asyncio.sleep(1.0)
    except WebSocketDisconnect:
        pass
    except Exception as e:  # noqa: BLE001
        logger.debug("scalper websocket closed: %s", e)


@router.get("/dashboard", response_class=HTMLResponse)
async def scalper_dashboard():
    return HTMLResponse(content=DASHBOARD_HTML)
