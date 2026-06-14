"""
vsp_autobuy.py
─────────────────────────────────────────────────────────────────────────────
iter170 (2026-06-15) — VSP-only REAL-MONEY auto-buy manager.

The operator wants auto-buy enabled for exactly ONE signal source: the
Volume-Spike-Pattern (VSP) ``BIG_PUMP`` event.  This manager is the
isolated, self-contained execution path for that — DELIBERATELY
independent of every other buy/sell gate so turning it on enables VSP
buys and NOTHING else:

  • NOT gated by global ``cfg.autoBuyEnabled``     (R1/R2/R3 + bots stay off)
  • NOT gated by ``TradeExecutor.HARD_DISABLE_AUTOBUY``
  • exits NOT gated by ``TradeExecutor.HARD_DISABLE_AUTOSELL``
    (the VSP bracket must take profit / stop loss on its OWN positions)

It also does NOT touch ``TradeState`` / ``PositionMonitor`` — those keep
managing the (currently disabled) main-bot positions.  VSP positions live
only in the ``VSP:AUTOBUY:POS`` Redis hash.

Operator spec (2026-06-15):
  • BUY at the SIGNAL price (limit).  SKIP if the current price is already
    ABOVE the signal price (no chasing).
  • 20 USDT per position.
  • +30% take-profit → resting GTC LIMIT SELL on the book.
  • -6% stop-loss   → monitored MARKET exit (this manager's tick).
  • Max 5 concurrent VSP positions ($100 exposure cap).

Lifecycle
─────────
  handle_signal()  (HTTP, from volume_spike_pattern.py)
      → gates → reserve slot → LIMIT BUY @ signal price
      → marketable (current ≤ signal) so it normally fills at once
      → on fill: place +30% TP LIMIT SELL, mark OPEN
      → otherwise mark PENDING (a later tick finishes it)

  _tick()  (every 2s)
      → PENDING : poll buy status; place TP on fill; cancel + free slot
                  if it never fills within ``buyTimeoutSec``.
      → OPEN    : poll TP status (FILLED ⇒ close); else check -6% SL and
                  market-sell (cancelling the TP first).
"""

from __future__ import annotations

import asyncio
import json
import time
from decimal import Decimal
from typing import Any, Dict, Optional

import redis.asyncio as aioredis

from booknow.processors.base import AsyncProcessor
from booknow.repository import redis_keys


POS_KEY = "VSP:AUTOBUY:POS"          # hash symbol -> position json
LOG_KEY_PREFIX = "VSP:AUTOBUY:LOG:"  # list per UTC date (observability)

# A marketable limit buy should fill within a second or two.  If it is
# still resting after this long the price moved away — cancel and release
# the slot rather than leaving stranded exposure.
BUY_TIMEOUT_S = 90
# Drop a reservation that never produced an order (placement crashed).
RESERVE_TIMEOUT_S = 120


def _d(v: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(v))
    except Exception:
        return Decimal(default)


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


class VspAutoBuyManager(AsyncProcessor):
    """Isolated VSP-only real-money auto-buy + bracket manager."""

    name = "vsp_autobuy"
    sleep_s = 2.0          # SL granularity ~2s (reads CURRENT_PRICE hash)
    error_sleep_s = 2.0

    def __init__(
        self,
        *,
        redis_client: aioredis.Redis,
        ws_api: Any,
        filter_service: Any,
        delist_service: Any,
        config_service: Any,
        guard: Any,
        live_mode: bool,
    ) -> None:
        super().__init__()
        self._redis = redis_client
        self._ws_api = ws_api
        self._filters = filter_service
        self._delist = delist_service
        self._config = config_service
        self._guard = guard
        self.live_mode = bool(live_mode)
        # Serialises the read-modify-write of the position hash so two
        # near-simultaneous signals can't both claim the last free slot.
        self._lock = asyncio.Lock()

    # ── observability ───────────────────────────────────────────────
    async def _log_event(self, **kw: Any) -> None:
        kw.setdefault("ts", int(time.time() * 1000))
        date = time.strftime("%Y-%m-%d", time.gmtime())
        try:
            await self._redis.rpush(LOG_KEY_PREFIX + date, json.dumps(kw))
            await self._redis.expire(LOG_KEY_PREFIX + date, 30 * 24 * 3600)
        except Exception:
            pass

    # ── entry point (called by the HTTP endpoint) ───────────────────
    async def handle_signal(
        self,
        symbol: str,
        signal_price: float,
        current_price: float,
        label: str = "BIG_PUMP",
        confidence: float = 0.0,
    ) -> Dict[str, Any]:
        """Process one VSP signal.  Returns a small status dict.

        ``signal_price`` is the VSP event's ``trigger_close`` (the price
        the spike fired at).  ``current_price`` is the live price now.
        """
        symbol = symbol.upper()
        cfg = await self._config.get()

        if not bool(getattr(cfg, "vspAutoBuyLiveEnabled", False)):
            return {"status": "skipped", "reason": "vspAutoBuyLiveEnabled=False"}

        # Only BIG_PUMP, and only at/above the confidence floor.
        if label != "BIG_PUMP":
            return {"status": "skipped", "reason": f"label={label} (only BIG_PUMP)"}
        min_conf = _f(getattr(cfg, "vspAutoBuyMinConfidence", 75), 75.0)
        if confidence and confidence < min_conf:
            return {"status": "skipped", "reason": f"confidence {confidence} < {min_conf}"}

        if signal_price <= 0:
            return {"status": "skipped", "reason": "bad signal_price"}

        # No-chase: don't buy a coin that already ran above the signal.
        if bool(getattr(cfg, "vspAutoBuyNoChase", True)):
            if current_price > 0 and current_price > signal_price:
                await self._log_event(
                    event="skip_chase", symbol=symbol,
                    signal=signal_price, now=current_price,
                )
                return {"status": "skipped", "reason": "current > signal (no-chase)"}

        if self._guard.is_banned():
            return {"status": "skipped", "reason": "binance ban active"}

        try:
            if await self._delist.is_delisted(symbol):
                return {"status": "skipped", "reason": "delisted"}
        except Exception:
            # fail-closed: don't trade if we can't verify
            return {"status": "skipped", "reason": "delist check failed"}

        usdt = _f(getattr(cfg, "vspAutoBuyUsdt", 20.0), 20.0)
        tp_pct = _f(getattr(cfg, "vspAutoBuyTpPct", 30.0), 30.0)
        sl_pct = _f(getattr(cfg, "vspAutoBuySlPct", 6.0), 6.0)
        max_pos = int(getattr(cfg, "vspAutoBuyMaxPositions", 5) or 5)

        # ── reserve a slot atomically (cap + dedup) ──────────────────
        async with self._lock:
            existing = await self._redis.hget(POS_KEY, symbol)
            if existing:
                return {"status": "skipped", "reason": "already in VSP position"}
            count = await self._redis.hlen(POS_KEY)
            if count >= max_pos:
                await self._log_event(
                    event="skip_cap", symbol=symbol, open_positions=count, cap=max_pos,
                )
                return {"status": "skipped", "reason": f"position cap {count}/{max_pos}"}
            await self._redis.hset(POS_KEY, symbol, json.dumps({
                "state": "RESERVED",
                "ts": int(time.time() * 1000),
                "signalPrice": signal_price,
                "label": label,
                "confidence": confidence,
            }))

        # ── size + place the limit buy (outside the lock) ────────────
        try:
            buy_price = await self._filters.round_price(symbol, _d(signal_price))
            qty = _d(usdt) / buy_price if buy_price > 0 else Decimal(0)
            qty = await self._filters.round_quantity(symbol, qty)
            if qty <= 0:
                await self._release(symbol)
                return {"status": "error", "reason": "qty rounded to 0"}
            try:
                await self._filters.validate_notional(symbol, qty, buy_price)
            except Exception as e:
                await self._release(symbol)
                return {"status": "skipped", "reason": f"min-notional: {e}"}

            tp_price = await self._filters.round_price(
                symbol, buy_price * (Decimal(1) + _d(tp_pct) / Decimal(100)))
            sl_price = await self._filters.round_price(
                symbol, buy_price * (Decimal(1) - _d(sl_pct) / Decimal(100)))

            if self.live_mode:
                self.log.info(
                    "[VSP-AB] LIVE limitBuy %s qty=%s @ %s (signal=%s, now=%s, "
                    "TP=%s SL=%s)",
                    symbol, qty, buy_price, signal_price, current_price,
                    tp_price, sl_price,
                )
                resp = await self._ws_api.place_order(
                    symbol=symbol, side="BUY", order_type="LIMIT",
                    quantity=str(qty), price=str(buy_price), time_in_force="GTC",
                )
            else:
                self.log.info(
                    "[VSP-AB] PAPER limitBuy %s qty=%s @ %s (signal=%s)",
                    symbol, qty, buy_price, signal_price,
                )
                resp = {
                    "orderId": int(time.time() * 1000),
                    "status": "FILLED",
                    "executedQty": str(qty),
                    "price": str(buy_price),
                }

            order_id = int(resp.get("orderId") or 0)
            status = (resp.get("status") or "NEW").upper()
            executed = _d(resp.get("executedQty"))
            # avg fill price if Binance reported it; else the limit price.
            cqq = _d(resp.get("cummulativeQuoteQty"))
            fill_price = (cqq / executed) if (executed > 0 and cqq > 0) else buy_price

            base = {
                "buyOrderId": order_id,
                "buyPrice": float(buy_price),      # cost basis = signal price
                "fillPrice": float(fill_price),
                "tpPrice": float(tp_price),
                "slPrice": float(sl_price),
                "qty": str(qty),
                "signalPrice": signal_price,
                "label": label,
                "confidence": confidence,
                "usdt": usdt,
                "tpPct": tp_pct,
                "slPct": sl_pct,
                "ts": int(time.time() * 1000),
            }

            if status == "FILLED" and executed > 0:
                tp_order_id = await self._place_tp(symbol, executed, tp_price)
                base.update({
                    "state": "OPEN",
                    "qty": str(executed),
                    "tpOrderId": tp_order_id,
                })
                await self._redis.hset(POS_KEY, symbol, json.dumps(base))
                await self._log_event(
                    event="buy_filled", symbol=symbol, qty=str(executed),
                    buyPrice=float(buy_price), tpPrice=float(tp_price),
                    slPrice=float(sl_price), tpOrderId=tp_order_id,
                )
                return {"status": "bought", "symbol": symbol,
                        "qty": str(executed), "tpPrice": float(tp_price),
                        "slPrice": float(sl_price)}

            # Resting (or partial) — let the tick loop finish it.
            base.update({"state": "PENDING", "tpOrderId": None})
            if executed > 0:
                base["qty"] = str(executed)
            await self._redis.hset(POS_KEY, symbol, json.dumps(base))
            await self._log_event(
                event="buy_placed", symbol=symbol, status=status,
                buyOrderId=order_id, buyPrice=float(buy_price),
            )
            return {"status": "placed", "symbol": symbol,
                    "orderState": status, "buyOrderId": order_id}

        except Exception as e:
            await self._release(symbol)
            self.log.error("[VSP-AB] buy %s failed: %s", symbol, e, exc_info=True)
            return {"status": "error", "reason": str(e)}

    async def _release(self, symbol: str) -> None:
        try:
            await self._redis.hdel(POS_KEY, symbol)
        except Exception:
            pass

    async def _place_tp(
        self, symbol: str, qty: Decimal, tp_price: Decimal,
    ) -> Optional[int]:
        """Resting GTC LIMIT SELL at +TP%.  Bypasses HARD_DISABLE_AUTOSELL
        by design — this is the VSP bracket's own take-profit leg."""
        try:
            sell_qty = await self._filters.round_quantity(symbol, qty)
            if self.live_mode:
                resp = await self._ws_api.place_order(
                    symbol=symbol, side="SELL", order_type="LIMIT",
                    quantity=str(sell_qty), price=str(tp_price),
                    time_in_force="GTC",
                )
                oid = resp.get("orderId") if isinstance(resp, dict) else None
                self.log.info(
                    "[VSP-AB] TP armed %s qty=%s @ %s (#%s)",
                    symbol, sell_qty, tp_price, oid,
                )
                return int(oid) if oid is not None else None
            self.log.info("[VSP-AB] PAPER TP %s qty=%s @ %s", symbol, sell_qty, tp_price)
            return int(time.time() * 1000)
        except Exception as e:
            self.log.error("[VSP-AB] TP place %s failed: %s", symbol, e, exc_info=True)
            return None

    # ── monitor loop ────────────────────────────────────────────────
    async def _tick(self) -> None:
        try:
            raw = await self._redis.hgetall(POS_KEY)
        except Exception:
            return
        if not raw:
            return

        now_ms = int(time.time() * 1000)
        for sym_b, val_b in raw.items():
            symbol = sym_b.decode() if isinstance(sym_b, (bytes, bytearray)) else sym_b
            try:
                pos = json.loads(val_b)
            except Exception:
                await self._release(symbol)
                continue
            state = pos.get("state")
            age_s = (now_ms - int(pos.get("ts", now_ms))) / 1000.0
            try:
                if state == "RESERVED":
                    if age_s > RESERVE_TIMEOUT_S:
                        self.log.warning("[VSP-AB] drop stale reservation %s", symbol)
                        await self._release(symbol)
                elif state == "PENDING":
                    await self._tick_pending(symbol, pos, age_s)
                elif state == "OPEN":
                    await self._tick_open(symbol, pos)
            except Exception as e:
                self.log.error("[VSP-AB] tick %s (%s) failed: %s",
                               symbol, state, e, exc_info=True)

    async def _order_status(self, symbol: str, order_id: int) -> Optional[Dict[str, Any]]:
        if not self.live_mode:
            return {"status": "FILLED", "executedQty": "0"}
        try:
            return await self._ws_api.get_order_status(symbol, order_id=order_id)
        except Exception as e:
            self.log.debug("[VSP-AB] status %s #%s: %s", symbol, order_id, e)
            return None

    async def _tick_pending(self, symbol: str, pos: Dict[str, Any], age_s: float) -> None:
        order_id = int(pos.get("buyOrderId") or 0)
        st = await self._order_status(symbol, order_id) if order_id else None
        status = (st.get("status") if st else "").upper()
        executed = _d(st.get("executedQty")) if st else Decimal(0)

        if status == "FILLED" and executed > 0:
            await self._promote_to_open(symbol, pos, executed)
            return
        if status in ("CANCELED", "EXPIRED", "REJECTED"):
            if executed > 0:
                # partial fill before cancel — protect what we own
                await self._promote_to_open(symbol, pos, executed)
            else:
                self.log.info("[VSP-AB] buy %s %s — releasing slot", symbol, status)
                await self._release(symbol)
            return

        # still resting (NEW / PARTIALLY_FILLED) — give up after timeout
        if age_s > BUY_TIMEOUT_S:
            self.log.info(
                "[VSP-AB] buy %s not filled in %ss — cancelling, free slot",
                symbol, int(age_s),
            )
            if order_id and self.live_mode:
                try:
                    await self._ws_api.cancel_order(symbol, order_id=order_id)
                except Exception:
                    pass
            # If a partial filled in the meantime, protect it.
            st2 = await self._order_status(symbol, order_id) if order_id else None
            ex2 = _d(st2.get("executedQty")) if st2 else Decimal(0)
            if ex2 > 0:
                await self._promote_to_open(symbol, pos, ex2)
            else:
                await self._release(symbol)

    async def _promote_to_open(self, symbol: str, pos: Dict[str, Any], qty: Decimal) -> None:
        tp_price = await self._filters.round_price(symbol, _d(pos.get("tpPrice")))
        tp_order_id = await self._place_tp(symbol, qty, tp_price)
        pos.update({"state": "OPEN", "qty": str(qty), "tpOrderId": tp_order_id})
        await self._redis.hset(POS_KEY, symbol, json.dumps(pos))
        await self._log_event(
            event="buy_filled_async", symbol=symbol, qty=str(qty),
            tpPrice=pos.get("tpPrice"), slPrice=pos.get("slPrice"),
            tpOrderId=tp_order_id,
        )
        self.log.info("[VSP-AB] %s OPEN qty=%s TP#%s", symbol, qty, tp_order_id)

    async def _tick_open(self, symbol: str, pos: Dict[str, Any]) -> None:
        qty = _d(pos.get("qty"))
        tp_order_id = pos.get("tpOrderId")

        # 1) Did the take-profit fill?
        if tp_order_id:
            st = await self._order_status(symbol, int(tp_order_id))
            if st and (st.get("status") or "").upper() == "FILLED":
                self.log.info("[VSP-AB] %s TP FILLED — closed +%.1f%%",
                              symbol, _f(pos.get("tpPct"), 30.0))
                await self._log_event(event="tp_filled", symbol=symbol,
                                      tpPrice=pos.get("tpPrice"), qty=str(qty))
                await self._release(symbol)
                return
            # TP order vanished (cancelled externally) — re-arm it.
            if st and (st.get("status") or "").upper() in ("CANCELED", "EXPIRED", "REJECTED"):
                new_id = await self._place_tp(
                    symbol, qty, await self._filters.round_price(symbol, _d(pos.get("tpPrice"))))
                pos["tpOrderId"] = new_id
                await self._redis.hset(POS_KEY, symbol, json.dumps(pos))
        else:
            # TP never got placed (transient failure on fill) — arm it now
            # so a +TP% move still auto-exits, not just the -SL% stop.
            new_id = await self._place_tp(
                symbol, qty, await self._filters.round_price(symbol, _d(pos.get("tpPrice"))))
            if new_id is not None:
                pos["tpOrderId"] = new_id
                await self._redis.hset(POS_KEY, symbol, json.dumps(pos))

        # 2) Stop-loss check on live price.
        cp = await self._current_price(symbol)
        if cp is None or cp <= 0:
            return
        sl_price = _f(pos.get("slPrice"))
        if sl_price > 0 and cp <= sl_price:
            self.log.warning(
                "[VSP-AB] %s STOP-LOSS hit (now=%s <= SL=%s) — market exit",
                symbol, cp, sl_price,
            )
            await self._stop_out(symbol, pos, qty, cp)

    async def _stop_out(
        self, symbol: str, pos: Dict[str, Any], qty: Decimal, price: float,
    ) -> None:
        # Cancel the resting TP first so we don't double-sell.
        tp_order_id = pos.get("tpOrderId")
        if tp_order_id and self.live_mode:
            try:
                await self._ws_api.cancel_order(symbol, order_id=int(tp_order_id))
            except Exception:
                pass  # may already be gone
        try:
            sell_qty = await self._filters.round_quantity(symbol, qty)
            if self.live_mode:
                # Bypasses HARD_DISABLE_AUTOSELL by design — VSP's own SL.
                await self._ws_api.place_order(
                    symbol=symbol, side="SELL", order_type="MARKET",
                    quantity=str(sell_qty),
                )
                self.log.info("[VSP-AB] %s SL market-sold qty=%s @ ~%s",
                              symbol, sell_qty, price)
            else:
                self.log.info("[VSP-AB] PAPER SL %s qty=%s @ ~%s",
                              symbol, sell_qty, price)
            await self._log_event(
                event="stop_loss", symbol=symbol, qty=str(sell_qty),
                price=price, slPrice=pos.get("slPrice"),
            )
        except Exception as e:
            self.log.error("[VSP-AB] SL market-sell %s failed: %s",
                           symbol, e, exc_info=True)
            # Keep the position so the next tick retries the stop-out.
            return
        await self._release(symbol)

    async def _current_price(self, symbol: str) -> Optional[float]:
        try:
            raw = await self._redis.hget(redis_keys.CURRENT_PRICE, symbol)
            if not raw:
                return None
            obj = json.loads(raw)
            p = float(obj.get("price") or 0)
            return p if p > 0 else None
        except Exception:
            return None
