"""
trailing_tp.py
─────────────────────────────────────────────────────────────────────────────
Dynamic / chasing take-profit (iter 47, 2026-05-23).

Replaces the static +$0.20 net limit-sell with a ratcheting one:

  1. When the buy fills, the executor places a limit-sell at the *base TP*
     (entry + profitAmountUsdt net of fees).  That limit-sell is registered
     here via ``register(symbol, base_tp_price, ...)``.

  2. PositionMonitor calls ``on_price_tick(symbol, price)`` on every 1s
     tick of an open position.  This module replies with one of:

       - ``NoneAction``  : leave the limit-sell where it is.
       - ``MoveUp(new_tp_price, new_offset_usdt)`` : cancel the current
         limit-sell and place a new one ``new_offset_usdt`` above the
         current price.  Fires when current price has risen
         ``move_step_pct`` above the *current* limit-sell price.
       - ``FloorSell()`` : the position was "armed" (price exceeded the
         base TP at some point) and has now retraced back to the base TP.
         Force a market sell so we still bank the original net profit.

  3. The executor calls ``unregister(symbol)`` on sell-fill so we stop
     ticking on closed positions.

State is in-memory.  If the engine restarts, the live limit-sell on
Binance is still in place; the ratchet just won't apply on those orders
until they're re-armed (which only happens on a fresh buy-fill).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from threading import RLock
from typing import Dict, Optional, Union


logger = logging.getLogger("booknow.trailing_tp")


@dataclass(frozen=True)
class NoneAction:
    pass


@dataclass(frozen=True)
class MoveUp:
    new_tp_price: Decimal
    profit_amount_usdt: float


@dataclass(frozen=True)
class FloorSell:
    base_tp_price: Decimal


@dataclass(frozen=True)
class PumpTrailExit:
    """iter 59 — pump-mode positions exit at market when price drops
    pumpModeTrailPct% from the running peak.  No static TP; the trail
    is the only profit-taking mechanism (plus HARD-SL as the floor)."""
    peak_price: Decimal
    trigger_price: Decimal


Action = Union[NoneAction, MoveUp, FloorSell, PumpTrailExit]


@dataclass
class _State:
    """Per-symbol ratchet state."""

    base_tp_price: Decimal         # the floor — original +profit_usdt target (NET)
    current_tp_price: Decimal      # where the active limit-sell sits
    qty: Decimal                   # filled base-asset qty
    profit_amount_usdt: float      # spread above current price for new TPs
    buy_price: Decimal = Decimal(0)  # iter 54: used to compute fee offset on ratchet
    fee_rate: float = 0.00075        # iter 54: round-trip fee per side
    armed: bool = False            # True once price first reached base_tp
    # iter 59 — pump-mode state
    pump_mode: bool = False          # if True, use peak-trail exit instead of static TP
    peak_price: Decimal = Decimal(0)  # running peak since buy (for pump-trail)
    pump_trail_pct: float = 1.5      # exit when price drops X% from peak


class TrailingTakeProfit:
    """Chase-up limit-sell + market-floor at base TP.

    Single instance per engine, shared by PositionMonitor + executor +
    main.py's user-data callback.  All public methods are thread-safe.
    """

    def __init__(self, move_step_pct: float = 0.3):
        self._lock = RLock()
        self._states: Dict[str, _State] = {}
        self.move_step_pct = move_step_pct

    # ── Public API ──────────────────────────────────────────────────────

    def register(
        self,
        symbol: str,
        base_tp_price: Decimal,
        current_tp_price: Decimal,
        qty: Decimal,
        profit_amount_usdt: float,
        buy_price: Decimal = Decimal(0),
        fee_rate: float = 0.00075,
        pump_mode: bool = False,
        pump_trail_pct: float = 1.5,
    ) -> None:
        """Called on buy-fill once the initial limit-sell is placed (or
        skipped in pump mode).

        iter 54: ``buy_price`` + ``fee_rate`` arrive so the ratchet
        (MoveUp) can include the round-trip fee component in its offset.

        iter 59: ``pump_mode`` switches the exit logic from
        static-TP-plus-ratchet to peak-trail-exit (sell at market when
        price drops ``pump_trail_pct`` % from the running peak).
        """
        with self._lock:
            self._states[symbol] = _State(
                base_tp_price=base_tp_price,
                current_tp_price=current_tp_price,
                qty=qty,
                profit_amount_usdt=profit_amount_usdt,
                buy_price=buy_price,
                fee_rate=fee_rate,
                pump_mode=pump_mode,
                peak_price=buy_price,
                pump_trail_pct=pump_trail_pct,
            )
        logger.info(
            "[TrailingTP] %s registered  base_tp=%s  current_tp=%s  qty=%s",
            symbol, base_tp_price, current_tp_price, qty,
        )

    def update_current_tp(self, symbol: str, new_tp_price: Decimal) -> None:
        """Called after a successful cancel + replace, so the next tick
        compares against the new resting TP, not the previous one."""
        with self._lock:
            st = self._states.get(symbol)
            if st is None:
                return
            st.current_tp_price = new_tp_price

    def unregister(self, symbol: str) -> None:
        """Called on sell-fill / forced exit — stop ticking this symbol."""
        with self._lock:
            self._states.pop(symbol, None)

    def is_tracking(self, symbol: str) -> bool:
        with self._lock:
            return symbol in self._states

    def is_pump_mode(self, symbol: str) -> bool:
        """iter 59 — inspector for the position monitor to choose the
        right max-hold timer (4h pump vs 1h normal)."""
        with self._lock:
            st = self._states.get(symbol)
            return bool(st and st.pump_mode)

    def on_price_tick(self, symbol: str, price: Decimal) -> Action:
        """The single decision point.

        Ordering matters:
          1. If not yet armed and price >= base_tp → mark armed.  Don't act
             yet: a fresh hit could be the genuine TP fill, in which case
             cancelling + replacing higher would be churn.  Wait one tick.
          2. If armed and price <= base_tp → FloorSell.  We've been above
             and retraced; bank the floor profit immediately at market.
          3. If price > current_tp × (1 + step_pct/100) → MoveUp.  Cancel
             the existing limit-sell and put a new one above current price.
        """
        with self._lock:
            st = self._states.get(symbol)
            if st is None:
                return NoneAction()

            # ── iter 59: pump-mode — peak-trail market exit ──────────────
            if st.pump_mode:
                # Update peak if price made a new high.
                if price > st.peak_price:
                    st.peak_price = price
                    logger.debug(
                        "[TrailingTP] %s pump-mode new peak %s", symbol, price,
                    )
                    return NoneAction()
                # Exit when price drops trail % from peak.
                trail_mult = Decimal(1) - (Decimal(str(st.pump_trail_pct)) / Decimal(100))
                trail_floor = st.peak_price * trail_mult
                if price <= trail_floor:
                    logger.info(
                        "[TrailingTP] %s PUMP-TRAIL EXIT — price %s <= peak %s × (1 - %.2f%%) = %s",
                        symbol, price, st.peak_price, st.pump_trail_pct, trail_floor,
                    )
                    return PumpTrailExit(peak_price=st.peak_price, trigger_price=price)
                return NoneAction()

            base = st.base_tp_price
            current = st.current_tp_price

            # 1. Arming pass — silent first cross of base_tp.
            if not st.armed and price >= base:
                st.armed = True
                logger.info(
                    "[TrailingTP] %s ARMED at %s (base_tp=%s)",
                    symbol, price, base,
                )
                return NoneAction()

            # 2. Floor exit — armed + retraced.
            if st.armed and price <= base:
                logger.info(
                    "[TrailingTP] %s FLOOR @ %s (retraced to base_tp=%s)",
                    symbol, price, base,
                )
                return FloorSell(base_tp_price=base)

            # 3. Ratchet up — price has run above current TP by step%.
            step_multiplier = Decimal(1) + Decimal(str(self.move_step_pct)) / Decimal(100)
            if price > current * step_multiplier:
                # iter 54: new_tp = current_price + (NET_profit / qty) + per-unit-fees
                # so realised NET at this new TP equals (price - buy_price) + profit_amount.
                # Without the fee component the ratchet only adds gross profit.
                if st.qty <= 0:
                    return NoneAction()
                net_offset_per_unit = Decimal(str(st.profit_amount_usdt)) / st.qty
                # Approximate fee per unit using current price (close to buy_price
                # when ratchet just started; small drift is acceptable for fees).
                fee_anchor = st.buy_price if st.buy_price > 0 else price
                fee_offset_per_unit = Decimal(2) * Decimal(str(st.fee_rate)) * fee_anchor
                offset_per_unit = net_offset_per_unit + fee_offset_per_unit
                new_tp = price + offset_per_unit
                logger.info(
                    "[TrailingTP] %s MOVE-UP %s → %s (price=%s, step=+%.2f%%, net_off=%s, fee_off=%s)",
                    symbol, current, new_tp, price, self.move_step_pct,
                    net_offset_per_unit, fee_offset_per_unit,
                )
                return MoveUp(new_tp_price=new_tp, profit_amount_usdt=st.profit_amount_usdt)

            return NoneAction()
