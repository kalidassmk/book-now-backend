"""
tsl.py
─────────────────────────────────────────────────────────────────────────────
Trailing Stop-Loss tracker. Direct port of TrailingStopLossService.java.

Per-symbol high-water-mark store. ``check_and_track(symbol, price)`` is
called once per tick by the position monitor for every open position:

  - if price > highest seen → update high-water mark (trail up)
  - if price ≤ highest * (1 - tsl_pct/100) → return True (TRIGGER)
  - else → return False (still in the green band)

Storage is in-memory (a dict). Re-tracking a symbol after a sale wipes
its history via ``reset(symbol)``.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from threading import RLock
from typing import Dict


logger = logging.getLogger("booknow.tsl")


class TrailingStopLoss:
    """Per-symbol high-water tracker."""

    def __init__(self, trailing_percentage: float = 2.0):
        self.trailing_percentage = trailing_percentage
        self._lock = RLock()
        self._highest: Dict[str, Decimal] = {}

    # ── Public API ───────────────────────────────────────────────────────

    def check_and_track(self, symbol: str, current_price: Decimal) -> bool:
        """Update high-water mark, return True if TSL should trigger.

        Mirrors the Java contract exactly: initialise to current_price
        on first sight, drop on any breach of the trailing band.
        """
        if current_price is None or current_price <= 0:
            return False

        with self._lock:
            highest = self._highest.get(symbol)
            if highest is None:
                self._highest[symbol] = current_price
                highest = current_price
            elif current_price > highest:
                self._highest[symbol] = current_price
                highest = current_price
                logger.debug("[TSL] %s new high: %s", symbol, highest)

        multiplier = Decimal("1") - (Decimal(str(self.trailing_percentage)) / Decimal("100"))
        stop_loss = highest * multiplier
        if current_price <= stop_loss:
            logger.info(
                "[TSL] TRIGGER SELL for %s — current %s <= stop %s (high %s)",
                symbol, current_price, stop_loss, highest,
            )
            return True
        return False

    def start_tracking(self, symbol: str, initial_price: Decimal) -> None:
        """Seed the high-water mark explicitly (called from TradeExecutor)."""
        with self._lock:
            self._highest[symbol] = initial_price
        logger.info("[TSL] start_tracking %s @ %s", symbol, initial_price)

    def reset(self, symbol: str) -> None:
        """Drop the high-water entry — call after a successful close."""
        with self._lock:
            self._highest.pop(symbol, None)
        logger.debug("[TSL] reset %s", symbol)

    def highest(self, symbol: str) -> Decimal:
        """Inspector for tests + dashboards."""
        with self._lock:
            return self._highest.get(symbol, Decimal(0))
