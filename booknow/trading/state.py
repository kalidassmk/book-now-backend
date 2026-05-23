"""
state.py
─────────────────────────────────────────────────────────────────────────────
In-memory active-position registry. Direct port of TradeState.java.

Tracks every open position with:
  - which rule triggered the buy (R1-FULL, R1-ULTRA, …)
  - entry price + timestamp (for PnL math + the max-hold timer)
  - the Binance order id of the open GTC limit-sell so we can cancel it
    before forcing a market exit

Sell-listener pattern: rules register a callback in their constructor
that clears their per-symbol "triggered" guard on close, so the same
coin can be scalped again on the next signal.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from threading import RLock
from time import time
from typing import Callable, Dict, List, Optional


logger = logging.getLogger("booknow.trade_state")


@dataclass
class Position:
    """Per-symbol open-position record."""

    symbol: str
    rule: str
    buy_price: Decimal = Decimal(0)
    entry_time: float = field(default_factory=time)  # epoch seconds
    open_sell_order_id: Optional[int] = None         # GTC limit-sell on Binance


SellListener = Callable[[str], None]


class TradeState:
    """Thread-safe in-memory map of active positions.

    Single instance per engine. Every rule, the position monitor, and
    the trade executor share this registry.
    """

    def __init__(self):
        self._lock = RLock()
        self._positions: Dict[str, Position] = {}
        self._sell_listeners: List[SellListener] = []

    # ── Reads ────────────────────────────────────────────────────────────

    def is_already_bought(self, symbol: str) -> bool:
        with self._lock:
            return symbol in self._positions

    def get_position(self, symbol: str) -> Optional[Position]:
        with self._lock:
            return self._positions.get(symbol)

    def snapshot(self) -> Dict[str, Position]:
        """Shallow copy of the position map; safe to iterate while
        other tasks mutate the source.
        """
        with self._lock:
            return dict(self._positions)

    def active_count(self) -> int:
        with self._lock:
            return len(self._positions)

    # ── Writes ───────────────────────────────────────────────────────────

    def mark_bought(
        self,
        symbol: str,
        rule: str,
        buy_price: Optional[Decimal] = None,
    ) -> Position:
        """Register a new open position. Replaces any existing entry."""
        pos = Position(
            symbol=symbol,
            rule=rule,
            buy_price=buy_price if buy_price is not None else Decimal(0),
        )
        with self._lock:
            self._positions[symbol] = pos
        logger.info("[TradeState] mark_bought %s rule=%s @ %s", symbol, rule, pos.buy_price)
        return pos

    def record_open_sell_order(self, symbol: str, order_id: int) -> None:
        """Pin the order id of the GTC limit-sell so the monitor can
        cancel it before a forced market exit.
        """
        with self._lock:
            pos = self._positions.get(symbol)
            if pos is not None:
                pos.open_sell_order_id = order_id

    def mark_sold(self, symbol: str) -> None:
        """Remove a position and notify every registered sell listener.

        Listeners must be non-blocking + side-effect-free w.r.t. each
        other; one raising won't stop the others from running.
        """
        with self._lock:
            removed = self._positions.pop(symbol, None)
            listeners = list(self._sell_listeners)
        if removed is None:
            return
        logger.info("[TradeState] mark_sold %s", symbol)
        for l in listeners:
            try:
                l(symbol)
            except Exception as e:
                logger.warning("[TradeState] sell listener raised: %s", e)

    # ── Listener registration ────────────────────────────────────────────

    def add_sell_listener(self, listener: SellListener) -> None:
        """Register a callable invoked on every successful close."""
        with self._lock:
            self._sell_listeners.append(listener)
