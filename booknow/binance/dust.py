"""
dust.py
─────────────────────────────────────────────────────────────────────────────
Async port of BinanceDustService.java — but wired to react to the
WebSocket balance push instead of polling ``getAccount()``.

How it works:

  1. ``UserDataStreamService`` calls
     :meth:`DustService.evaluate_balances(B-array)` every time Binance
     emits ``outboundAccountPosition`` (i.e. on every balance change).
  2. We compare each non-USDT asset's free amount to the symbol's
     ``minQty`` (read from ``FilterService``). If ``free < minQty``,
     that's dust → mark it in Redis under ``BINANCE:DUST:<asset>``.
  3. A background task runs every 30 seconds and tries to transfer any
     pinned dust. ``universalTransfer`` (Funding → Spot) is the first
     pass; ``sweep_to_bnb`` is invoked manually after a sale.

Both transfer endpoints are SAPI write-ops with no WebSocket equivalent,
so they stay REST. Rate-limit guard is honoured so we don't dig a ban
deeper while it's active.
"""

from __future__ import annotations

import asyncio
import json
import logging
from decimal import Decimal, InvalidOperation
from time import time
from typing import Iterable, List, Mapping, Optional

import redis.asyncio as aioredis

from booknow.binance.filters import FilterService
from booknow.binance.rate_limit import get_default as _get_rate_limit_guard
from booknow.binance.rest_api import BinanceIpBannedException, RestApiClient
from booknow.repository import redis_keys


logger = logging.getLogger("booknow.dust")

TRANSFER_INTERVAL_S = 30          # was 10s in Java; 30s avoids hammering /sapi
TRANSFER_GAP_S = 0.5              # space between transfers in one cycle


def _dust_key(asset: str) -> str:
    return f"{redis_keys.DUST_PREFIX}{asset}"


class DustService:
    """Detect + transfer dust assets, WS-driven."""

    def __init__(
        self,
        redis_client: aioredis.Redis,
        rest: RestApiClient,
        filter_service: FilterService,
    ):
        self._redis = redis_client
        self._rest = rest
        self._filters = filter_service
        self._guard = _get_rate_limit_guard()

        self._transfer_task: Optional[asyncio.Task] = None
        self._running = False

    # ── Lifecycle ────────────────────────────────────────────────────────

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._transfer_task = asyncio.create_task(
            self._auto_transfer_loop(), name="dust-auto-transfer",
        )
        logger.info("[DustService] auto-transfer loop spawned (every %ds)", TRANSFER_INTERVAL_S)

    async def stop(self) -> None:
        self._running = False
        if self._transfer_task is not None:
            self._transfer_task.cancel()
            try:
                await self._transfer_task
            except (asyncio.CancelledError, Exception):
                pass
            self._transfer_task = None

    # ── WS-driven dust detection ─────────────────────────────────────────

    async def evaluate_balances(self, balances: Iterable[Mapping]) -> None:
        """Called from the user-data-stream after every account snapshot.

        Iterates non-USDT assets, classifies each as dust when ``free``
        is below ``minQty``. Pins dust into ``BINANCE:DUST:<asset>``;
        clears the entry when the asset goes back above ``minQty``.
        """
        if balances is None:
            return
        marked = 0
        cleared = 0
        for b in balances:
            asset = b.get("a") or b.get("asset")
            free = b.get("f") if "f" in b else b.get("free")
            if not asset or asset == "USDT":
                continue
            try:
                free_d = Decimal(str(free or "0"))
            except InvalidOperation:
                continue
            if free_d <= 0:
                continue

            symbol = f"{asset}USDT"
            try:
                rule = await self._filters.get_or_fetch(symbol)
            except Exception:
                # Unknown trading pair (some assets aren't traded against
                # USDT directly) — skip silently.
                continue

            try:
                min_qty = Decimal(rule.get("minQty") or "0")
            except InvalidOperation:
                continue

            if min_qty > 0 and free_d < min_qty:
                payload = {
                    "asset": asset,
                    "free": str(free or "0"),
                    "valueUsdt": "0.0",
                    "status": "DUST",
                    "updatedAt": int(time() * 1000),
                }
                await self._redis.set(_dust_key(asset), json.dumps(payload))
                marked += 1
            else:
                # Asset is back above the dust threshold.
                if await self._redis.delete(_dust_key(asset)):
                    cleared += 1

        if marked or cleared:
            logger.debug(
                "[DustService] balance scan — %d marked, %d cleared", marked, cleared,
            )

    # ── Background transfer ──────────────────────────────────────────────

    async def _auto_transfer_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(TRANSFER_INTERVAL_S)
            except asyncio.CancelledError:
                break
            if not self._running:
                break
            if self._guard.is_banned():
                continue
            await self._transfer_pinned_dust()

    async def _transfer_pinned_dust(self) -> None:
        keys: List[str] = await self._redis.keys(f"{redis_keys.DUST_PREFIX}*")
        if not keys:
            return
        logger.info("[DustService] auto-transfer found %d dust asset(s)", len(keys))
        for key in keys:
            if self._guard.is_banned() or not self._running:
                return
            raw = await self._redis.get(key)
            if not raw:
                continue
            try:
                rec = json.loads(raw)
            except json.JSONDecodeError:
                continue
            asset = rec.get("asset")
            free = rec.get("free")
            if not asset or asset == "USDT" or not free:
                continue
            try:
                # FUNDING_MAIN moves Funding → Spot (matches Java).
                result = await self._rest.universal_transfer(
                    asset=asset, amount=str(free), transfer_type="FUNDING_MAIN",
                )
                if isinstance(result, dict) and result.get("tranId"):
                    logger.info(
                        "[DustService] SUCCESS — transferred %s qty=%s, tranId=%s",
                        asset, free, result["tranId"],
                    )
                    await self.remove(asset)
            except BinanceIpBannedException as e:
                logger.warning(
                    "[DustService] transfer paused — Binance ban (%ds)",
                    e.retry_after_seconds,
                )
                return
            except Exception as e:
                if self._guard.report_if_banned(e):
                    return
                logger.error("[DustService] transfer failed for %s: %s", asset, e)
                # Leave the entry in Redis so the next cycle retries.

            await asyncio.sleep(TRANSFER_GAP_S)

    # ── Manual sweep + cleanup ──────────────────────────────────────────

    async def sweep_to_bnb(self, asset: str) -> None:
        """Convert dust of `asset` to BNB. Called after a sell."""
        asset = (asset or "").upper()
        if asset in ("BNB", "USDT", ""):
            return
        if self._guard.is_banned():
            logger.warning(
                "[DustService] sweep_to_bnb(%s) deferred — Binance ban for %ds",
                asset, self._guard.ban_remaining_seconds(),
            )
            return
        try:
            result = await self._rest.dust_transfer([asset])
            logger.info("[DustService] BNB conversion OK for %s: %s", asset, result)
        except BinanceIpBannedException as e:
            logger.warning(
                "[DustService] sweep_to_bnb(%s) blocked by ban (%ds)",
                asset, e.retry_after_seconds,
            )
        except Exception as e:
            if self._guard.report_if_banned(e):
                return
            logger.error("[DustService] sweep_to_bnb(%s) failed: %s", asset, e)

    async def remove(self, asset: str) -> None:
        await self._redis.delete(_dust_key(asset))
