"""
executor.py
─────────────────────────────────────────────────────────────────────────────
The TradeExecutor — the only module that places real money on Binance.

Async port of TradeExecutor.java. Every order goes through the WS-API
client (Phase 3); REST is reserved exclusively for endpoints with no
WS equivalent (none here). Every public method honours the
RateLimitGuard and is safe to call concurrently.

Public surface (all async):

  Rule-driven flow
    try_buy(symbol, current_price_data, sell_pct, rule_label)
        Auto-trader entry. Validates state (already-bought / delisted /
        auto-buy disabled / fast-scalp gate), places a LIMIT BUY at
        currentPrice × (1 − offset/100), then a GTC LIMIT SELL at the
        +$0.20 take-profit target. Updates TradeState + TSL.

  Forced exit (called by PositionMonitor)
    force_market_exit(symbol, current_price, reason)
        Cancels the open limit-sell, places a MARKET SELL, and cleans
        up state. Implements the ExitExecutor protocol so the monitor
        replaces its LoggingExecutor stub with us in live mode.

  Manual / dashboard flow
    try_manual_limit_buy(symbol, current_price_data, manual_qty,
                         offset_pct, profit_pct)
    try_manual_market_buy(symbol, current_price_data, manual_qty)
    try_manual_sell(symbol, current_price_data, qty, rule_label)
    cancel_order(symbol, order_id)

Paper mode (``settings.live_mode == False``):
  - try_buy still runs the rule path: simulates the LIMIT BUY fill at
    the offset price, writes the BUY record to Redis, marks state.
    Does NOT place a real order or limit-sell on Binance.
  - force_market_exit logs a paper exit and cleans up state.

This lets you dry-run the rule wiring (Phase 11) end-to-end without
touching real funds.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from decimal import Decimal, ROUND_CEILING
from typing import Any, Dict, Mapping, Optional

import redis.asyncio as aioredis

from booknow.binance.delist import DelistService
from booknow.binance.dust import DustService
from booknow.binance.filters import FilterService
from booknow.binance.rate_limit import get_default as _get_rate_limit_guard
from booknow.binance.ws_api import (
    BinanceIpBannedException,
    WsApiClient,
    is_ip_ban,
)
from booknow.config.trading_config import TradingConfigService
from booknow.repository import redis_keys
from booknow.trading.state import TradeState
from booknow.trading.tsl import TrailingStopLoss
from booknow.util.momentum import get_hms


logger = logging.getLogger("booknow.executor")


def _to_decimal(v: Any) -> Decimal:
    if isinstance(v, Decimal):
        return v
    if v is None or v == "":
        return Decimal(0)
    try:
        return Decimal(str(v))
    except Exception:
        return Decimal(0)


def _price_of(current_price_data: Mapping[str, Any]) -> Decimal:
    """Pull the price out of a CURRENT_PRICE Redis row, defensively."""
    raw = current_price_data.get("price")
    if isinstance(raw, dict):
        # Some legacy rows had {"price": {"value": "..."}}.
        return _to_decimal(raw.get("value"))
    return _to_decimal(raw)


def _percentage_of(cp: Mapping[str, Any]) -> float:
    try:
        return float(cp.get("percentage") or 0)
    except (TypeError, ValueError):
        return 0.0


class TradeExecutor:
    """Async order-placement + position-bookkeeping service.

    Concurrent calls for the same symbol are still safe — the
    "is_already_bought" guard short-circuits the second call before
    any external state changes.
    """

    def __init__(
        self,
        *,
        redis_client: aioredis.Redis,
        ws_api: WsApiClient,
        filter_service: FilterService,
        delist_service: DelistService,
        trade_state: TradeState,
        tsl: TrailingStopLoss,
        config_service: TradingConfigService,
        dust_service: Optional[DustService] = None,
        coin_analyzer=None,           # CoinAnalyzer; optional
        trailing_tp=None,             # TrailingTakeProfit; optional (iter 47)
        live_mode: bool = False,
    ):
        self._redis = redis_client
        self._ws_api = ws_api
        self._filters = filter_service
        self._delist = delist_service
        self._state = trade_state
        self._tsl = tsl
        self._config = config_service
        self._dust = dust_service
        self._analyzer = coin_analyzer
        self._trailing_tp = trailing_tp
        self.live_mode = live_mode
        self._guard = _get_rate_limit_guard()

    def set_dust_service(self, dust_service: Optional[DustService]) -> None:
        """Late-bind the dust service.

        ``DustService`` only exists in live mode and the executor is
        constructed before that branch runs in main.py — so the
        executor takes ``None`` initially and gets the service via
        this setter once it's available. Replaces direct attribute
        access from outside the class.
        """
        self._dust = dust_service

    def _register_trailing_tp(
        self, *, symbol: str, buy_price: Decimal, qty: Decimal,
        profit_amount_usdt: float,
    ) -> None:
        """Compute base_tp + arm the trailing-TP tracker for this symbol.

        ``base_tp`` is the original net-profit target: the price at which
        the limit-sell would yield exactly +``profit_amount_usdt`` after
        round-trip fees on a ``buy_price × qty`` position.
        """
        if self._trailing_tp is None or qty <= 0 or buy_price <= 0:
            return
        # Same formula used in _place_limit_sell, kept here so we can also
        # arm the trail when the limit-sell is placed from on_buy_filled.
        total_cost = buy_price * qty
        target_total = total_cost + Decimal(str(profit_amount_usdt))
        base_tp = target_total / qty
        # The initial limit-sell sits at base_tp too; ratchet starts only
        # when price rises above it.
        self._trailing_tp.register(
            symbol=symbol,
            base_tp_price=base_tp,
            current_tp_price=base_tp,
            qty=qty,
            profit_amount_usdt=profit_amount_usdt,
        )
        # Also persist the floor on the in-memory Position so the monitor
        # can read it back without going through trailing_tp.
        pos = self._state.get_position(symbol)
        if pos is not None:
            pos.qty = qty
            pos.base_tp_price = base_tp

    async def on_buy_filled(
        self,
        *,
        symbol: str,
        order_id: int,
        filled_qty: Decimal,
        fill_price: Decimal,
        sell_pct: float,
    ) -> Optional[int]:
        """Called from the user-data-stream executionReport handler when a
        BUY limit-order fills.

        Steps:
          1. Refresh the BUY row in Redis with the real ``executedQty`` /
             ``status=FILLED`` (was 0 / NEW at the moment of placement).
          2. Place the GTC limit-sell at the net-profit target using the
             real qty.  Pin the new sell-orderId on TradeState.
          3. Register the position with TrailingTakeProfit so the
             PositionMonitor's ratchet logic can take over.

        Idempotent: if a sell order has already been placed on this
        Position (e.g. the buy was reported FILLED at place time), we
        bail out without placing a duplicate.
        """
        if not self.live_mode:
            return None
        if filled_qty <= 0 or fill_price <= 0:
            logger.warning(
                "[on_buy_filled] %s skipped — bad fill (qty=%s price=%s)",
                symbol, filled_qty, fill_price,
            )
            return None

        pos = self._state.get_position(symbol)
        if pos is None:
            logger.info("[on_buy_filled] %s not in TradeState — ignoring fill", symbol)
            return None
        if pos.open_sell_order_id is not None:
            logger.debug(
                "[on_buy_filled] %s already has sell-order #%s — skipping duplicate",
                symbol, pos.open_sell_order_id,
            )
            return pos.open_sell_order_id

        cfg = await self._config.get()

        # 1) Refresh the BUY row.
        try:
            raw = await self._redis.hget(redis_keys.BUY_KEY, symbol)
            if raw:
                buy = json.loads(raw)
            else:
                buy = {}
            buy["status"] = "FILLED"
            buy["executedQty"] = str(filled_qty)
            buy["buyPrice"] = float(fill_price)
            buy["orderId"] = order_id
            await self._redis.hset(redis_keys.BUY_KEY, symbol, json.dumps(buy))
        except Exception as e:
            logger.warning("[on_buy_filled] %s BUY refresh failed: %s", symbol, e)

        # 2) Place the limit-sell now that we know the real qty.
        sell_order_id = await self._place_limit_sell(
            symbol=symbol,
            qty=filled_qty,
            buy_price=fill_price,
            sell_pct=sell_pct,
            profit_amount_usdt=cfg.profitAmountUsdt,
        )
        if sell_order_id is not None:
            self._state.record_open_sell_order(symbol, sell_order_id)

        # 3) Arm the trailing-TP tracker.
        if cfg.dynamicTpEnabled:
            self._register_trailing_tp(
                symbol=symbol,
                buy_price=fill_price,
                qty=filled_qty,
                profit_amount_usdt=cfg.profitAmountUsdt,
            )

        logger.info(
            "[+TP ARMED] %s qty=%s buy=%s sell-order=#%s",
            symbol, filled_qty, fill_price, sell_order_id,
        )
        return sell_order_id

    async def move_limit_sell(
        self, *, symbol: str, new_price: Decimal,
    ) -> Optional[int]:
        """Cancel the open limit-sell for a position and place a new one
        at ``new_price``.  Used by the trailing-TP ratchet.

        Returns the new orderId, or None on failure.  Best-effort: if the
        cancel fails with -2011 (Unknown order), the previous limit-sell
        already filled — in that case we DON'T place a new sell (we'd
        oversell) and return None.
        """
        if not self.live_mode:
            return None
        pos = self._state.get_position(symbol)
        if pos is None or pos.qty <= 0:
            return None
        old_order_id = pos.open_sell_order_id

        # 1) Cancel the old limit-sell.
        if old_order_id is not None:
            try:
                await self._ws_api.cancel_order(symbol=symbol, order_id=old_order_id)
            except Exception as e:
                msg = str(e)
                if "-2011" in msg or "Unknown order" in msg:
                    # Already filled / cancelled — don't oversell.
                    logger.info(
                        "[MoveTP] %s old sell-order #%s already gone — skipping replace",
                        symbol, old_order_id,
                    )
                    return None
                logger.warning(
                    "[MoveTP] %s cancel old sell-order #%s failed: %s — aborting move",
                    symbol, old_order_id, e,
                )
                return None
            pos.open_sell_order_id = None

        # 2) Place the new limit-sell.
        try:
            qty = await self._filters.round_quantity(symbol, pos.qty)
            sell_price = await self._filters.round_price(symbol, new_price)
            resp = await self._ws_api.place_order(
                symbol=symbol, side="SELL", order_type="LIMIT",
                quantity=str(qty), price=str(sell_price), time_in_force="GTC",
            )
            new_order_id = resp.get("orderId") if isinstance(resp, dict) else None
            if new_order_id is not None:
                self._state.record_open_sell_order(symbol, int(new_order_id))
                if self._trailing_tp is not None:
                    self._trailing_tp.update_current_tp(symbol, sell_price)
                logger.info(
                    "[MoveTP] %s new sell-order #%s @ %s",
                    symbol, new_order_id, sell_price,
                )
                return int(new_order_id)
        except Exception as e:
            logger.error("[MoveTP] %s place new sell-order failed: %s", symbol, e)
        return None

    # ── Auto-trader entry ───────────────────────────────────────────────

    async def try_buy(
        self,
        symbol: str,
        current_price_data: Mapping[str, Any],
        sell_pct: float,
        rule_label: str,
    ) -> None:
        """Rule-driven buy. Mirrors Java TradeExecutor.tryBuy line-for-line."""
        cfg = await self._config.get()

        if not cfg.autoBuyEnabled:
            logger.info("[%s] Auto-buy DISABLED via config — skipping %s", rule_label, symbol)
            return

        if self._state.is_already_bought(symbol):
            logger.debug("[%s] Skip %s — already in position", rule_label, symbol)
            return

        try:
            if await self._delist.is_delisted(symbol):
                logger.warning(
                    "[%s] CRITICAL: skip %s — Symbol is marked DELISTED", rule_label, symbol,
                )
                return
        except Exception as e:
            # Redis hiccup → fail closed (don't trade if we can't check).
            logger.error("[%s] delist check failed for %s: %s — skipping for safety", rule_label, symbol, e)
            return

        # ── 2-month history gate (skipped in fast-scalp mode) ─────────
        if not cfg.fastScalpMode and self._analyzer is not None:
            cur_price_f = float(_price_of(current_price_data))
            try:
                if not await self._analyzer.should_buy(symbol, cur_price_f):
                    logger.info("[%s] Skip %s — analysis gate rejected (score < 4)", rule_label, symbol)
                    return
            except Exception as e:
                logger.warning("[%s] CoinAnalyzer error for %s: %s — fast-scalp fallback (proceeding)", rule_label, symbol, e)
        else:
            logger.debug("[%s] Fast-scalp mode: skipping CoinAnalyzer for %s", rule_label, symbol)

        # ── Place the buy order ──────────────────────────────────────
        if self._guard.is_banned():
            logger.warning(
                "[%s] BUY %s deferred — Binance ban active for %ds",
                rule_label, symbol, self._guard.ban_remaining_seconds(),
            )
            return

        try:
            price = _price_of(current_price_data)
            if price <= 0:
                logger.error("[%s] Bad current_price for %s: %r", rule_label, symbol, current_price_data)
                return

            qty_str = await self._calculate_qty(symbol, price, cfg.buyAmountUsdt)

            order = await self._place_buy_order(symbol, qty_str, price, cfg.limitBuyOffsetPct)
            buy_price = _to_decimal(order.get("price"))
            executed_qty = order.get("executedQty") or qty_str
            order_id = order.get("orderId") or 0
            order_status = (order.get("status") or "FILLED").upper()

            # Persist the BUY row + register state.
            buy_payload = {
                "status": order_status,
                "buyPercentage": _percentage_of(current_price_data),
                "buyPrice": float(buy_price),
                "selP": sell_pct,
                "hms": get_hms(),
                "buyTimeStamp": int(time.time() * 1000),
                "orderId": order_id,
                "executedQty": str(executed_qty),
                "origQty": str(order.get("origQty") or executed_qty),
            }
            await self._redis.hset(redis_keys.BUY_KEY, symbol, json.dumps(buy_payload))
            self._state.mark_bought(symbol, rule_label, buy_price)
            self._tsl.start_tracking(symbol, buy_price)

            # ── Limit-sell at the +$0.20 (or +sellPct) target ──────
            # 2026-05-23: only place the limit-sell here if the buy already
            # filled (rare for LIMIT orders — usually status=NEW).  Going
            # ahead with executedQty=0 used to crash with DivisionByZero
            # and left positions unprotected.  For NEW orders we defer to
            # the buy-fill execution-report handler (see on_buy_filled),
            # which uses the actual filled qty.
            if self.live_mode and order_status == "FILLED" and _to_decimal(executed_qty) > 0:
                sell_order_id = await self._place_limit_sell(
                    symbol=symbol,
                    qty=_to_decimal(executed_qty),
                    buy_price=buy_price,
                    sell_pct=sell_pct,
                    profit_amount_usdt=cfg.profitAmountUsdt,
                )
                if sell_order_id is not None:
                    self._state.record_open_sell_order(symbol, sell_order_id)
                # Register the position with the trailing-TP tracker.
                if self._trailing_tp is not None and cfg.dynamicTpEnabled:
                    self._register_trailing_tp(
                        symbol=symbol,
                        buy_price=buy_price,
                        qty=_to_decimal(executed_qty),
                        profit_amount_usdt=cfg.profitAmountUsdt,
                    )

            logger.info(
                "[%s] BUY %s @ %s qty=%s (target +%.2f%%, +%s USDT)",
                rule_label, symbol, buy_price, executed_qty, sell_pct, cfg.profitAmountUsdt,
            )
        except BinanceIpBannedException as e:
            logger.error("[%s] BUY %s failed — Binance ban: %s", rule_label, symbol, e)
        except Exception as e:
            if is_ip_ban(e):
                logger.error("[%s] BUY %s failed — Binance ban (wrapped)", rule_label, symbol)
                return
            logger.error("[%s] Error executing buy for %s: %s", rule_label, symbol, e, exc_info=True)

    # ── Forced market exit (PositionMonitor calls this) ────────────────

    async def force_market_exit(
        self,
        symbol: str,
        current_price: Decimal,
        reason: str,
    ) -> None:
        """Implements the ExitExecutor protocol the monitor consumes.

        Cancels the open GTC limit-sell, places a MARKET SELL, cleans
        up state. Best-effort — a "Unknown order" cancel just means the
        limit-sell already filled, in which case the market sell will
        fail cleanly.
        """
        if not self._state.is_already_bought(symbol):
            logger.debug("[ForceExit:%s] skip %s — not in position", reason, symbol)
            return

        # Resolve qty from the BUY record (it has the actual filled qty).
        try:
            buy_raw = await self._redis.hget(redis_keys.BUY_KEY, symbol)
        except Exception as e:
            logger.error("[ForceExit:%s] redis lookup failed for %s: %s", reason, symbol, e)
            return
        if not buy_raw:
            logger.warning("[ForceExit:%s] no buy record for %s — aborting exit", reason, symbol)
            return
        try:
            buy = json.loads(buy_raw)
        except json.JSONDecodeError:
            logger.warning("[ForceExit:%s] corrupted BUY row for %s", reason, symbol)
            return
        qty = _to_decimal(buy.get("executedQty"))
        if qty <= 0:
            # Fallback to the in-memory Position.qty (set on buy-fill).
            pos_for_qty = self._state.get_position(symbol)
            if pos_for_qty is not None and pos_for_qty.qty > 0:
                qty = pos_for_qty.qty
                logger.info(
                    "[ForceExit:%s] %s BUY row missing executedQty — using Position.qty=%s",
                    reason, symbol, qty,
                )
            else:
                logger.warning("[ForceExit:%s] no qty for %s — aborting exit", reason, symbol)
                return
        try:
            qty = await self._filters.round_quantity(symbol, qty)
        except Exception as e:
            logger.warning("[ForceExit:%s] round_quantity failed for %s: %s", reason, symbol, e)

        # 1) Cancel the open limit-sell.
        pos = self._state.get_position(symbol)
        if pos is not None and pos.open_sell_order_id is not None and self.live_mode:
            try:
                logger.info(
                    "[ForceExit:%s] Cancelling open limit-sell #%s for %s",
                    reason, pos.open_sell_order_id, symbol,
                )
                await self._ws_api.cancel_order(symbol=symbol, order_id=pos.open_sell_order_id)
            except BinanceIpBannedException as e:
                logger.error("[ForceExit:%s] cancel blocked by ban: %s", reason, e)
                return
            except Exception as e:
                # -2011 = "Unknown order sent" → already filled. Proceed.
                logger.warning(
                    "[ForceExit:%s] Cancel failed for %s #%s (likely already filled): %s",
                    reason, symbol, pos.open_sell_order_id, e,
                )

        # 2) Market sell.
        sell_price = current_price
        try:
            if self.live_mode:
                logger.info(
                    "[ForceExit:%s] LIVE marketSell %s qty=%s @ ~%s",
                    reason, symbol, qty, current_price,
                )
                resp = await self._ws_api.place_order(
                    symbol=symbol, side="SELL", order_type="MARKET",
                    quantity=str(qty),
                )
                # Fills aren't always reflected in the immediate price field.
                # If we got something useful, use it; else stick with current.
                if isinstance(resp, dict):
                    avg = _to_decimal(resp.get("price"))
                    if avg > 0:
                        sell_price = avg
                # Sweep dust to BNB after a clean exit.
                if self._dust is not None:
                    base = symbol.replace("USDT", "")
                    await self._dust.sweep_to_bnb(base)
            else:
                logger.info(
                    "[ForceExit:%s] PAPER marketSell %s qty=%s @ ~%s",
                    reason, symbol, qty, current_price,
                )
        except BinanceIpBannedException as e:
            logger.error("[ForceExit:%s] market-sell blocked by ban: %s", reason, e)
            return
        except Exception as e:
            if is_ip_ban(e):
                return
            logger.error(
                "[ForceExit:%s] market-sell failed for %s: %s",
                reason, symbol, e, exc_info=True,
            )
            return

        # 3) Persist the sell + clean up state.
        sell_payload = {
            "sellingPoint": str(sell_price),
            "status": "Y",
            "timestamp": int(time.time() * 1000),
            "sellMaxPercentage": 0.0,  # we don't track these in Python yet
            "sellAveragePercentage": 0.0,
            "sellLeastPercentage": 0.0,
        }
        try:
            await self._redis.hset(redis_keys.SELL_KEY, symbol, json.dumps(sell_payload))
            await self._redis.hdel(redis_keys.BUY_KEY, symbol)
        except Exception as e:
            logger.error("[ForceExit:%s] redis cleanup failed for %s: %s", reason, symbol, e)
        self._state.mark_sold(symbol)
        self._tsl.reset(symbol)
        if self._trailing_tp is not None:
            self._trailing_tp.unregister(symbol)
        logger.info("[FORCE-EXIT:%s] %s @ %s done", reason, symbol, sell_price)

    # ── Manual / dashboard flow ────────────────────────────────────────

    async def try_manual_limit_buy(
        self,
        symbol: str,
        current_price_data: Mapping[str, Any],
        manual_qty: float = 0,
        offset_pct: float = 0.3,
        profit_pct: float = 2.0,
    ) -> Optional[Dict[str, Any]]:
        if self._state.is_already_bought(symbol):
            logger.warning("[MANUAL] limit-buy %s skipped — already in position", symbol)
            return None
        if await self._delist.is_delisted(symbol):
            logger.warning("[MANUAL] CRITICAL skip limit-buy %s — DELISTED", symbol)
            return None
        try:
            price = _price_of(current_price_data)
            offset = Decimal(1) - Decimal(str(offset_pct)) / Decimal(100)
            limit_price = await self._filters.round_price(symbol, price * offset)

            cfg = await self._config.get()
            if manual_qty > 0:
                qty = await self._filters.round_quantity(symbol, _to_decimal(manual_qty))
            else:
                qty = _to_decimal(await self._calculate_qty(symbol, limit_price, cfg.buyAmountUsdt))

            await self._filters.validate_notional(symbol, qty, limit_price)

            if self.live_mode:
                logger.info("[MANUAL] LIVE limitBuy %s qty=%s @ %s (%.2f%% below %s)",
                            symbol, qty, limit_price, offset_pct, price)
                resp = await self._ws_api.place_order(
                    symbol=symbol, side="BUY", order_type="LIMIT",
                    quantity=str(qty), price=str(limit_price), time_in_force="GTC",
                )
            else:
                logger.info("[MANUAL] PAPER limitBuy %s qty=%s @ %s (%.2f%% below %s)",
                            symbol, qty, limit_price, offset_pct, price)
                resp = {
                    "symbol": symbol,
                    "orderId": int(time.time() * 1000),
                    "price": str(limit_price),
                    "executedQty": str(qty),
                    "origQty": str(qty),
                    "status": "FILLED",
                }

            await self._record_manual_buy(resp, current_price_data, profit_pct)
            return resp
        except BinanceIpBannedException as e:
            logger.error("[MANUAL] limit-buy %s blocked by ban: %s", symbol, e)
            return None
        except Exception as e:
            logger.error("[MANUAL] limit-buy %s failed: %s", symbol, e)
            return None

    async def try_manual_market_buy(
        self,
        symbol: str,
        current_price_data: Mapping[str, Any],
        manual_qty: float,
    ) -> Optional[Dict[str, Any]]:
        if self._state.is_already_bought(symbol):
            logger.warning("[MANUAL] market-buy %s skipped — already in position", symbol)
            return None
        try:
            price = _price_of(current_price_data)
            qty = await self._filters.round_quantity(symbol, _to_decimal(manual_qty))
            await self._filters.validate_notional(symbol, qty, price)

            if self.live_mode:
                logger.info("[MANUAL] LIVE marketBuy %s qty=%s @ ~%s", symbol, qty, price)
                resp = await self._ws_api.place_order(
                    symbol=symbol, side="BUY", order_type="MARKET", quantity=str(qty),
                )
                # MARKET orders may return price="0" — fall back to current.
                if not _to_decimal(resp.get("price")):
                    resp["price"] = str(price)
            else:
                logger.info("[MANUAL] PAPER marketBuy %s qty=%s @ ~%s", symbol, qty, price)
                resp = {
                    "symbol": symbol,
                    "orderId": int(time.time() * 1000),
                    "price": str(price),
                    "executedQty": str(qty),
                    "origQty": str(qty),
                    "status": "FILLED",
                }

            cfg = await self._config.get()
            await self._record_manual_buy(resp, current_price_data, cfg.profitPct)
            return resp
        except BinanceIpBannedException as e:
            logger.error("[MANUAL] market-buy %s blocked by ban: %s", symbol, e)
            return None
        except Exception as e:
            logger.error("[MANUAL] market-buy %s failed: %s", symbol, e)
            return None

    async def try_manual_sell(
        self,
        symbol: str,
        current_price_data: Mapping[str, Any],
        qty: Optional[float] = None,
        rule_label: str = "MANUAL",
    ) -> None:
        try:
            sell_price = _price_of(current_price_data)
            if qty is not None and qty > 0:
                qty_str = str(await self._filters.round_quantity(symbol, _to_decimal(qty)))
            else:
                buy_raw = await self._redis.hget(redis_keys.BUY_KEY, symbol)
                if not buy_raw:
                    logger.error("[%s SELL] No position in Redis for %s", rule_label, symbol)
                    return
                buy = json.loads(buy_raw)
                qty_str = str(buy.get("executedQty") or "0")

            if self.live_mode:
                logger.info("[%s] LIVE marketSell %s qty=%s @ ~%s", rule_label, symbol, qty_str, sell_price)
                await self._ws_api.place_order(
                    symbol=symbol, side="SELL", order_type="MARKET", quantity=qty_str,
                )
                if self._dust is not None:
                    await self._dust.sweep_to_bnb(symbol.replace("USDT", ""))
            else:
                logger.info("[%s] PAPER marketSell %s qty=%s @ ~%s", rule_label, symbol, qty_str, sell_price)

            await self._redis.hdel(redis_keys.BUY_KEY, symbol)
            self._state.mark_sold(symbol)
            self._tsl.reset(symbol)

            sell_payload = {
                "sellingPoint": str(sell_price),
                "status": "Y",
                "timestamp": int(time.time() * 1000),
            }
            await self._redis.hset(redis_keys.SELL_KEY, symbol, json.dumps(sell_payload))
            logger.info("[%s SELL] Completed for %s", rule_label, symbol)
        except Exception as e:
            logger.error("[%s SELL] Failed for %s: %s", rule_label, symbol, e)

    async def cancel_order(self, symbol: str, order_id: int) -> None:
        logger.info("[Cancel] cancel order %s for %s", order_id, symbol)
        try:
            await self._ws_api.cancel_order(symbol=symbol, order_id=order_id)
            logger.info("[Cancel] %s #%s cancelled", symbol, order_id)
        except BinanceIpBannedException as e:
            logger.error("[Cancel] %s #%s blocked by ban: %s", symbol, order_id, e)
            raise
        except Exception as e:
            logger.error("[Cancel] %s #%s failed: %s", symbol, order_id, e)
            raise

    # ── Internal helpers ─────────────────────────────────────────────────

    async def _calculate_qty(
        self,
        symbol: str,
        price: Decimal,
        buy_amount_usdt: float,
    ) -> str:
        """qty = buyAmountUsdt / price, then rounded to symbol stepSize."""
        amount = _to_decimal(buy_amount_usdt)
        # Match Java: rounding=CEILING with extra precision so we don't
        # under-spend by a satoshi.
        qty = (amount / price).quantize(Decimal("0.00000001"), rounding=ROUND_CEILING)
        qty = await self._filters.round_quantity(symbol, qty)
        return str(qty)

    async def _place_buy_order(
        self,
        symbol: str,
        qty_str: str,
        current_price: Decimal,
        offset_pct: float,
    ) -> Dict[str, Any]:
        """LIMIT BUY at currentPrice × (1 − offset_pct/100). Live or paper."""
        offset = Decimal(1) - Decimal(str(offset_pct)) / Decimal(100)
        limit_price = current_price * offset
        limit_price = await self._filters.round_price(symbol, limit_price)
        qty = _to_decimal(qty_str)
        await self._filters.validate_notional(symbol, qty, limit_price)

        if self.live_mode:
            logger.info(
                "LIVE limitBuy %s qty=%s price=%s (%s%% below %s)",
                symbol, qty, limit_price, offset_pct, current_price,
            )
            resp = await self._ws_api.place_order(
                symbol=symbol, side="BUY", order_type="LIMIT",
                quantity=str(qty), price=str(limit_price), time_in_force="GTC",
            )
            return resp if isinstance(resp, dict) else {}

        # Paper: simulate immediate fill at the limit price.
        logger.info(
            "PAPER LIMIT-BUY %s qty=%s price=%s (%s%% below market)",
            symbol, qty, limit_price, offset_pct,
        )
        return {
            "symbol": symbol,
            "orderId": int(time.time() * 1000),
            "price": str(limit_price),
            "executedQty": str(qty),
            "origQty": str(qty),
            "status": "FILLED",
        }

    async def _place_limit_sell(
        self,
        *,
        symbol: str,
        qty: Decimal,
        buy_price: Decimal,
        sell_pct: float,
        profit_amount_usdt: float,
    ) -> Optional[int]:
        """GTC LIMIT SELL at the +$0.20 (or +sell_pct%) target."""
        try:
            qty = await self._filters.round_quantity(symbol, qty)
            if profit_amount_usdt > 0:
                # sellPrice = (cost + profit) / qty
                total_cost = buy_price * qty
                target_total = total_cost + Decimal(str(profit_amount_usdt))
                sell_price = target_total / qty
                logger.info(
                    "[AmountMode] target +$%s → sellPrice %s",
                    profit_amount_usdt, sell_price,
                )
            else:
                sell_mult = Decimal(1) + Decimal(str(sell_pct)) / Decimal(100)
                sell_price = buy_price * sell_mult
                logger.info("[PctMode] target +%s%% → sellPrice %s", sell_pct, sell_price)
            sell_price = await self._filters.round_price(symbol, sell_price)

            logger.info(
                "LIVE limitSell %s qty=%s sellPrice=%s (target=%s)",
                symbol, qty, sell_price,
                f"$${profit_amount_usdt}" if profit_amount_usdt > 0 else f"{sell_pct}%",
            )
            resp = await self._ws_api.place_order(
                symbol=symbol, side="SELL", order_type="LIMIT",
                quantity=str(qty), price=str(sell_price), time_in_force="GTC",
            )
            order_id = resp.get("orderId") if isinstance(resp, dict) else None
            if order_id is not None:
                logger.info("[+TARGET ARMED] %s sell order #%s @ %s", symbol, order_id, sell_price)
            return int(order_id) if order_id is not None else None
        except Exception as e:
            logger.error("limit-sell place failed for %s: %s", symbol, e, exc_info=True)
            return None

    async def _record_manual_buy(
        self,
        order: Dict[str, Any],
        current_price_data: Mapping[str, Any],
        profit_pct: float,
    ) -> None:
        """Persist a manual buy + register state. Used by dashboard flows."""
        symbol = order.get("symbol") or ""
        price = _to_decimal(order.get("price"))
        executed_qty = order.get("executedQty") or "0"
        order_id = order.get("orderId") or 0
        status = (order.get("status") or "FILLED").upper()

        payload = {
            "status": status,
            "buyPercentage": _percentage_of(current_price_data),
            "buyPrice": float(price),
            "selP": profit_pct,
            "hms": get_hms(),
            "buyTimeStamp": int(time.time() * 1000),
            "orderId": order_id,
            "executedQty": str(executed_qty),
            "origQty": str(order.get("origQty") or executed_qty),
        }
        await self._redis.hset(redis_keys.BUY_KEY, symbol, json.dumps(payload))
        self._state.mark_bought(symbol, "MANUAL", price)
        self._tsl.start_tracking(symbol, price)
