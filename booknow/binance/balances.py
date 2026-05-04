"""
balances.py
─────────────────────────────────────────────────────────────────────────────
Owner of the ``BINANCE:BALANCE:<asset>`` Redis cache.

Direct port of ``BinanceBalanceService.java``. Fed by
``UserDataStreamService`` over the WebSocket (events
``outboundAccountPosition`` and ``balanceUpdate``); REST is reserved
for a single one-shot seed at engine boot before the WS frame arrives.

Redis schema (matches what the dashboard reads):
    key:   ``BINANCE:BALANCE:<asset>``      (e.g. ``BINANCE:BALANCE:BTC``)
    value: JSON ``{"asset":..., "free":..., "locked":..., "updatedAt":...}``
"""

from __future__ import annotations

import json
import logging
from decimal import Decimal, InvalidOperation
from time import time
from typing import Iterable, List, Mapping, Optional

import redis.asyncio as aioredis

from booknow.binance.rate_limit import get_default as _get_rate_limit_guard
from booknow.binance.ws_api import WsApiClient, is_ip_ban
from booknow.repository import redis_keys


logger = logging.getLogger("booknow.balances")


def _balance_key(asset: str) -> str:
    return f"{redis_keys.BALANCE_PREFIX}{asset}"


def _to_float(s: Optional[str]) -> float:
    if s is None or s == "":
        return 0.0
    try:
        return float(s)
    except (TypeError, ValueError):
        return 0.0


class BalanceService:
    """Holds the wallet-balance Redis cache.

    Two pathways feed this cache:

    1. ``apply_account_snapshot(balances)`` — the WS event
       ``outboundAccountPosition`` carries an ``B`` array with every
       non-zero balance in the account. Each row replaces the matching
       Redis key; assets that have gone to zero get deleted so they
       don't linger as stale rows.

    2. ``apply_balance_delta(asset, delta)`` — the rare per-asset
       ``balanceUpdate`` event. We add the delta into the existing
       cached row; if there's no row yet, we let the next snapshot
       fill it (snapshots are emitted alongside any balance change).

    A third optional path, ``seed_from_rest()``, is called once at
    boot so the cache has data before the WS subscription warms up.
    Honours :class:`RateLimitGuard` so it skips cleanly when the IP
    is banned.
    """

    def __init__(
        self,
        redis_client: aioredis.Redis,
        ws_api: Optional[WsApiClient] = None,
    ):
        self.redis = redis_client
        self.ws_api = ws_api
        self._guard = _get_rate_limit_guard()
        # Async callbacks invoked after every successful snapshot apply.
        # Replaces the older "monkey-patch apply_account_snapshot" pattern
        # in main.py — see add_snapshot_listener docstring.
        self._snapshot_listeners: list = []

    def add_snapshot_listener(self, listener) -> None:
        """Register an async callback ``async fn(balances)`` invoked
        after every ``apply_account_snapshot``.

        Used in live mode to tee the same balance payload into
        ``DustService.evaluate_balances`` so dust gets re-classified
        every time the account changes. Listeners that raise are
        logged but don't break the snapshot pipeline.
        """
        self._snapshot_listeners.append(listener)

    # ── Public API used by the WebSocket service ─────────────────────────

    async def apply_account_snapshot(self, balances: Iterable[Mapping]) -> None:
        """Persist a full account snapshot from ``outboundAccountPosition``.

        ``balances`` is the raw ``B`` array (Binance-shaped):
            [{"a": "BTC", "f": "0.001", "l": "0"}, ...]
        """
        if balances is None:
            return
        now_ms = int(time() * 1000)
        changed = 0
        # Materialise once — listeners and Redis writes both consume it.
        balances_list = list(balances)
        async with self.redis.pipeline(transaction=False) as pipe:
            for b in balances_list:
                asset = b.get("a") or b.get("asset")
                free = b.get("f") if "f" in b else b.get("free")
                locked = b.get("l") if "l" in b else b.get("locked")
                if not asset:
                    continue
                free_f = _to_float(free)
                locked_f = _to_float(locked)
                if free_f <= 0 and locked_f <= 0:
                    pipe.delete(_balance_key(asset))
                    continue
                payload = json.dumps({
                    "asset": asset,
                    "free": free or "0",
                    "locked": locked or "0",
                    "updatedAt": now_ms,
                })
                pipe.set(_balance_key(asset), payload)
                changed += 1
            await pipe.execute()
        logger.debug("[BalanceService] WS account snapshot applied — %d non-zero assets", changed)
        # Notify listeners after the cache write so they observe a consistent state.
        for listener in self._snapshot_listeners:
            try:
                await listener(balances_list)
            except Exception as e:
                logger.warning("[BalanceService] snapshot listener failed: %s", e)

    async def apply_balance_delta(self, asset: str, balance_delta: str) -> None:
        """Apply a per-asset delta from ``balanceUpdate``."""
        if not asset or balance_delta is None:
            return
        key = _balance_key(asset)
        raw = await self.redis.get(key)
        if raw is None:
            logger.debug(
                "[BalanceService] balanceUpdate %s delta=%s (no cached row, will sync on next snapshot)",
                asset, balance_delta,
            )
            return
        try:
            cur = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("[BalanceService] Corrupted balance JSON for %s; replacing on next snapshot", asset)
            return
        try:
            new_free = Decimal(cur.get("free", "0")) + Decimal(balance_delta)
            cur["free"] = format(new_free, "f")
            cur["updatedAt"] = int(time() * 1000)
            await self.redis.set(key, json.dumps(cur))
        except (InvalidOperation, ValueError):
            logger.warning("[BalanceService] Bad balance delta for %s: %s", asset, balance_delta)

    # ── One-shot REST seed ───────────────────────────────────────────────

    async def seed_from_rest(self) -> None:
        """Single ``account.status`` call at boot to fill the cache.

        WebSocket pushes own every update afterwards. Skips cleanly
        when the IP is banned.
        """
        if self.ws_api is None:
            logger.debug("[BalanceService] No WsApiClient configured; skipping initial seed.")
            return
        if self._guard.is_banned():
            logger.warning(
                "[BalanceService] Initial seed skipped — Binance ban active for %ds. "
                "WS pushes will fill the cache once the listenKey reconnects.",
                self._guard.ban_remaining_seconds(),
            )
            return
        try:
            logger.info("[BalanceService] Seeding balances via WS-API account.status — WS will own updates afterwards.")
            account = await self.ws_api.get_account()
        except Exception as e:
            if is_ip_ban(e) or self._guard.report_if_banned(e):
                return
            logger.error(
                "[BalanceService] Initial seed failed: %s. WS pushes will fill the cache once it connects.",
                e,
            )
            return

        balances = (account or {}).get("balances") or []
        # account.status returns Binance-shape balances: [{"asset":..., "free":..., "locked":...}, ...]
        # Translate to the WS event-shape so apply_account_snapshot can stay one-shape only.
        translated: List[dict] = [
            {"a": b.get("asset"), "f": b.get("free"), "l": b.get("locked")}
            for b in balances
        ]
        await self.apply_account_snapshot(translated)
        non_zero = sum(
            1 for b in translated
            if _to_float(b.get("f")) > 0 or _to_float(b.get("l")) > 0
        )
        logger.info("[BalanceService] Seed complete — %d assets with non-zero balances cached.", non_zero)
