"""
routes_wallet.py
─────────────────────────────────────────────────────────────────────────────
``/api/wallet/*`` — direct port of Java's ``WalletController``.

Reads the BalanceService / DustService Redis caches the engine
maintains, plus a write endpoint for the dashboard's "transfer dust"
button (Funding → Spot universal transfer).

Endpoints:
    GET  /api/wallet/balances
    GET  /api/wallet/dust
    POST /api/wallet/dust-transfer?asset=
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException, Query

from booknow.api.deps import get_state
from booknow.api.state import AppState
from booknow.repository import redis_keys


router = APIRouter(prefix="/api/wallet", tags=["wallet"])
logger = logging.getLogger("booknow.api.wallet")


@router.get("/balances")
async def get_balances(state: AppState = Depends(get_state)) -> List[Dict[str, Any]]:
    """All wallet balances as the dashboard expects them.

    The Java endpoint scanned ``BINANCE:BALANCE:*`` and returned a
    list of WalletBalance JSON objects. We do the same against the
    same key prefix (the schema is shared).
    """
    pattern = f"{redis_keys.BALANCE_PREFIX}*"
    balances: List[Dict[str, Any]] = []
    async for key in state.redis.scan_iter(match=pattern, count=200):
        raw = await state.redis.get(key)
        if not raw:
            continue
        try:
            balances.append(json.loads(raw))
        except (TypeError, ValueError):
            logger.warning("[/balances] unparseable JSON at key %s", key)
    return balances


@router.get("/dust")
async def get_dust(state: AppState = Depends(get_state)) -> List[Dict[str, Any]]:
    pattern = f"{redis_keys.DUST_PREFIX}*"
    dust: List[Dict[str, Any]] = []
    async for key in state.redis.scan_iter(match=pattern, count=200):
        raw = await state.redis.get(key)
        if not raw:
            continue
        try:
            dust.append(json.loads(raw))
        except (TypeError, ValueError):
            logger.warning("[/dust] unparseable JSON at key %s", key)
    return dust


@router.post("/dust-transfer")
async def dust_transfer(
    asset: str = Query(...),
    state: AppState = Depends(get_state),
) -> str:
    """Transfer dust ``asset`` from Funding → Spot.

    Mirrors Java's WalletController#dustTransfer: looks up the dust
    entry for diagnostics, calls ``RestApiClient.universal_transfer``
    (FUNDING_MAIN), and on success removes the Redis entry.
    """
    key = f"{redis_keys.DUST_PREFIX}{asset.upper()}"
    raw = await state.redis.get(key)
    if not raw:
        return "Error: Asset not found in dust cache"
    try:
        d = json.loads(raw)
    except (TypeError, ValueError):
        return "Error: Asset not found in dust cache"

    free = str(d.get("free") or d.get("amount") or "0")
    try:
        result = await state.rest.universal_transfer(
            asset=asset, amount=free, transfer_type="FUNDING_MAIN",
        )
    except Exception as e:
        return f"Error: {e}"

    tran_id = result.get("tranId") if isinstance(result, dict) else None
    if not tran_id:
        return "Error: Transfer failed on Binance side"

    # Remove from cache; either via the dust service if available, or
    # directly by key if we're in paper mode without one.
    if state.dust_service is not None:
        try:
            await state.dust_service.remove(asset)
        except Exception:
            await state.redis.delete(key)
    else:
        await state.redis.delete(key)

    return f"Success: Transferred {free} {asset} to Spot Wallet. ID: {tran_id}"
