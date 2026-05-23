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


Action = Union[NoneAction, MoveUp, FloorSell]


@dataclass
class _State:
    """Per-symbol ratchet state."""

    base_tp_price: Decimal         # the floor — original +profit_usdt target
    current_tp_price: Decimal      # where the active limit-sell sits
    qty: Decimal                   # filled base-asset qty
    profit_amount_usdt: float      # spread above current price for new TPs
    armed: bool = False            # True once price first reached base_tp


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
    ) -> None:
        """Called on buy-fill once the initial limit-sell is placed."""
        with self._lock:
            self._states[symbol] = _State(
                base_tp_price=base_tp_price,
                current_tp_price=current_tp_price,
                qty=qty,
                profit_amount_usdt=profit_amount_usdt,
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
                # New TP target: current price + same dollar net offset.
                #   new_tp = price + profit_amount_usdt / qty
                if st.qty <= 0:
                    return NoneAction()
                offset_per_unit = Decimal(str(st.profit_amount_usdt)) / st.qty
                new_tp = price + offset_per_unit
                logger.info(
                    "[TrailingTP] %s MOVE-UP %s → %s (price=%s, step=+%.2f%%)",
                    symbol, current, new_tp, price, self.move_step_pct,
                )
                return MoveUp(new_tp_price=new_tp, profit_amount_usdt=st.profit_amount_usdt)

            return NoneAction()
