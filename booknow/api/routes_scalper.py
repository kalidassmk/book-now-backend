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
from booknow.scalper.dashboard import DASHBOARD_HTML


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
