"""
orphan_reconciler.py — iter 80 (2026-05-24)
─────────────────────────────────────────────────────────────────────────────
Detects positions held on Binance that our TradeState doesn't know about
and auto-arms a safety sell within seconds.

WHY THIS EXISTS:
  CHIPUSDT 2026-05-24: a MARKET BUY happened on Binance (likely from the
  user pressing BUY in the Binance UI/app) for 1996 CHIP @ $0.04790.
  Our bot had no record of it because the buy bypassed every try_buy
  path.  No auto-TP was armed → the position sat unprotected for 10
  minutes until a manual sell.  Had the price dumped 5% in that window,
  we'd have lost $4.78 with zero downside protection.

THIS RECONCILER:
  1. Polls every `orphanReconPollSec` seconds (default 30s).
  2. Reads BINANCE:BALANCE:* keys (maintained by BalanceService from the
     userDataStream).
  3. For every non-stablecoin asset with USD value >= `orphanReconMinUsd`:
       a. Skip if already in TradeState (`is_already_bought`).
       b. Skip if it's BNB (used for fees, not a trade position).
       c. Look up last price from CURRENT_PRICE Redis hash.
       d. Compute target sell price = last_price * (1 + targetPct/100).
       e. Place a LIMIT SELL via WsApiClient.
       f. Register in TradeState so subsequent ticks don't re-arm.
       g. Emit a dashboard alert so the user knows.

  Conservative defaults:
    - target_pct = 0.5%  (small win, mostly about protection)
    - min_usd    = 5     (skip true dust)
    - poll_sec   = 30    (30s max exposure on orphan positions)

  Bot trades that DO go through try_buy are unaffected — they're
  registered in TradeState within milliseconds of placement, so the
  reconciler's `is_already_bought` check skips them immediately.
"""
from __future__ import annotations

import asyncio
import json
import logging
from decimal import Decimal
from typing import Any, Dict, Optional

import redis.asyncio as aioredis

from booknow.binance.ws_api import WsApiClient
from booknow.binance.filters import FilterService
from booknow.config.settings import Settings
from booknow.trading.state import TradeState

logger = logging.getLogger("booknow.orphan_recon")

CONFIG_KEY = "TRADING_CONFIG"
STABLECOINS = frozenset({"USDT", "USDC", "USDP", "TUSD", "BUSD", "FDUSD", "DAI", "PYUSD"})
SKIP_ASSETS = frozenset({"BNB"})  # used for fees, not a position


class OrphanReconciler:
    """Periodic scanner for un-tracked positions."""

    def __init__(
        self,
        *,
        redis_client: aioredis.Redis,
        ws_api: WsApiClient,
        filter_service: FilterService,
        trade_state: TradeState,
        settings: Settings,
        live_mode: bool = False,
    ):
        self._redis = redis_client
        self._ws_api = ws_api
        self._filters = filter_service
        self._state = trade_state
        self._settings = settings
        self._live_mode = live_mode

        self._task: Optional[asyncio.Task] = None
        self._running = False
        # Avoid arming the same orphan twice if a tick races itself
        self._arming: set[str] = set()
        # Track which orphans we've already armed so we don't keep
        # trying to re-arm the same one if the sell hasn't filled yet
        # (TradeState handles that, but belt-and-suspenders)
        self._armed_symbols: set[str] = set()

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="orphan-reconciler")
        logger.info("[OrphanRecon] started")

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

    async def _cfg(self) -> Dict[str, Any]:
        """Read live config from Redis."""
        try:
            raw = await self._redis.get(CONFIG_KEY)
            if raw:
                return json.loads(raw)
        except Exception:
            pass
        return {}

    async def _loop(self) -> None:
        while self._running:
            try:
                cfg = await self._cfg()
                if not bool(cfg.get("orphanReconEnabled", True)):
                    poll = int(cfg.get("orphanReconPollSec", 30) or 30)
                    await asyncio.sleep(poll)
                    continue
                poll = int(cfg.get("orphanReconPollSec", 30) or 30)
                await self._scan_once(cfg)
                await asyncio.sleep(poll)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("[OrphanRecon] loop error: %s", e)
                await asyncio.sleep(30)

    async def _scan_once(self, cfg: Dict[str, Any]) -> None:
        """One pass: find orphans + arm safety sells."""
        min_usd = float(cfg.get("orphanReconMinUsd", 5.0))
        target_pct = float(cfg.get("orphanReconTargetPct", 0.5))
        fee_buffer = float(cfg.get("sellQtyFeeBuffer", 0.999))

        # Pull all BINANCE:BALANCE:* keys
        try:
            keys = await self._redis.keys("BINANCE:BALANCE:*")
        except Exception as e:
            logger.debug("[OrphanRecon] balance keys scan failed: %s", e)
            return
        if not keys:
            return

        for key in keys:
            try:
                raw = await self._redis.get(key)
                if not raw:
                    continue
                row = json.loads(raw)
                asset = row.get("asset")
                free = float(row.get("free") or 0)
                if not asset or asset in STABLECOINS or asset in SKIP_ASSETS:
                    continue
                if free <= 0:
                    continue

                symbol = f"{asset}USDT"

                # Skip if we're already tracking it
                if self._state.is_already_bought(symbol):
                    continue

                # Skip if we're currently arming it (race guard)
                if symbol in self._arming:
                    continue

                # Lookup last price from CURRENT_PRICE
                px_raw = await self._redis.hget("CURRENT_PRICE", symbol)
                if not px_raw:
                    continue
                try:
                    px = float(json.loads(px_raw).get("price") or 0)
                except Exception:
                    continue
                if px <= 0:
                    continue

                value_usd = free * px
                if value_usd < min_usd:
                    continue

                # Check trading pair exists
                try:
                    rules = await self._filters.get_or_fetch(symbol)
                    if not rules:
                        continue
                except Exception:
                    continue

                # We have a real orphan position — arm a safety sell
                self._arming.add(symbol)
                try:
                    await self._arm_orphan(symbol, asset, free, px, target_pct, fee_buffer)
                finally:
                    self._arming.discard(symbol)
            except Exception as e:
                logger.warning("[OrphanRecon] key %s error: %s", key, e)

    async def _arm_orphan(
        self,
        symbol: str,
        asset: str,
        free_qty: float,
        last_price: float,
        target_pct: float,
        fee_buffer: float,
    ) -> None:
        """Place a LIMIT SELL at +target_pct above current and register
        the position in TradeState so we don't re-arm next tick.
        """
        sell_price_raw = last_price * (1.0 + target_pct / 100.0)
        qty_raw = free_qty * fee_buffer

        try:
            # Round to symbol step + tick size
            qty = await self._filters.round_quantity(symbol, Decimal(str(qty_raw)))
            sell_price = await self._filters.round_price(symbol, Decimal(str(sell_price_raw)))
            await self._filters.validate_notional(symbol, qty, sell_price)
        except Exception as e:
            logger.info(
                "[OrphanRecon] %s skipped — notional/round failed: %s "
                "(qty=%.6f last=%.8f)", symbol, e, qty_raw, last_price,
            )
            return

        if not self._live_mode:
            logger.info(
                "[OrphanRecon] PAPER %s would arm sell qty=%s @ %s "
                "(orphan free=%.6f last=%.8f)",
                symbol, qty, sell_price, free_qty, last_price,
            )
            # Still mark it so we don't log the same orphan every 30s in paper.
            self._state.mark_bought(symbol, "ORPHAN_RECON", Decimal(str(last_price)))
            self._armed_symbols.add(symbol)
            return

        try:
            resp = await self._ws_api.place_order(
                symbol=symbol, side="SELL", order_type="LIMIT",
                quantity=str(qty), price=str(sell_price), time_in_force="GTC",
            )
            order_id = resp.get("orderId") if isinstance(resp, dict) else None
            logger.info(
                "[OrphanRecon] 🛡️ ARMED orphan %s qty=%s @ %s "
                "(+%s%% above last %.8f) — order #%s",
                symbol, qty, sell_price, target_pct, last_price, order_id,
            )
            # Register so we don't re-arm
            pos = self._state.mark_bought(symbol, "ORPHAN_RECON", Decimal(str(last_price)))
            if order_id and hasattr(self._state, "record_open_sell_order"):
                self._state.record_open_sell_order(symbol, int(order_id))
            self._armed_symbols.add(symbol)

            # Dashboard alert
            try:
                from booknow.util.alerts import publish_trade_alert
                await publish_trade_alert(
                    redis_client=self._redis,
                    symbol=symbol,
                    action="ORPHAN_ARMED",
                    price=Decimal(str(last_price)),
                    extra={
                        "qty": float(qty),
                        "sell_target": float(sell_price),
                        "target_pct": target_pct,
                    },
                )
            except Exception:
                pass
        except Exception as e:
            logger.error("[OrphanRecon] %s SELL placement failed: %s", symbol, e)
