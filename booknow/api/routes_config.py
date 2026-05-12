"""
routes_config.py
─────────────────────────────────────────────────────────────────────────────
``/api/v1/config`` — dashboard-editable trading configuration.

The current dashboard talks to Redis directly for this, but having
the python-engine expose the same endpoint means we can flip
``server.js`` over to the engine port in one move (Phase 16) without
losing this surface.

GET returns the current TradingConfig as JSON; POST validates and
saves new values via :class:`TradingConfigService`.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

from fastapi import APIRouter, Body, Depends, HTTPException

from booknow.api.deps import get_state
from booknow.api.state import AppState
from booknow.config.trading_config import TradingConfig


router = APIRouter(prefix="/api/v1", tags=["config"])
logger = logging.getLogger("booknow.api.config")


@router.get("/config")
async def get_config(state: AppState = Depends(get_state)) -> Dict[str, Any]:
    cfg = await state.config_service.refresh()
    return cfg.to_dict()


@router.post("/config")
async def post_config(
    payload: Dict[str, Any] = Body(...),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    if not isinstance(payload.get("autoBuyEnabled"), bool):
        raise HTTPException(status_code=400, detail="Invalid autoBuyEnabled")
    for fld in ("buyAmountUsdt", "profitAmountUsdt"):
        try:
            float(payload.get(fld))  # will raise on None / non-numeric
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail=f"Invalid {fld}")

    # 2026-05-12 iter 15 fix: MERGE payload with existing Redis config
    # before saving. Without this, any field NOT in the payload silently
    # reverts to its dataclass default — and the dashboard form only sends
    # 6 fields (autoBuyEnabled, buyAmountUsdt, profitPct, profitAmountUsdt,
    # tslPct, limitBuyOffsetPct). Every "save" was wiping virtualScalperLiveMode,
    # postPump*, ladderTimeExit*, ladderTrailing*, etc. back to defaults.
    # Operator reported "after every deploy virtualScalperLiveMode reverts
    # to false" — that wasn't a deploy issue, it was every dashboard save.
    existing = await state.config_service.refresh()
    merged = {**existing.to_dict(), **payload}
    cfg = TradingConfig.from_dict(merged)
    await state.config_service.save(cfg)
    logger.info("[/config] updated (merged %d payload fields over %d existing): %s",
                len(payload), len(existing.to_dict()), cfg.to_dict())
    return {"success": True, "config": cfg.to_dict()}
