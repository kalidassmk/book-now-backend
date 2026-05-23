"""
tsl.py
─────────────────────────────────────────────────────────────────────────────
Trailing Stop-Loss tracker. Direct port of TrailingStopLossService.java.

Per-symbol high-water-mark store. ``check_and_track(symbol, price)`` is
called once per tick by the position monitor for every open position:

  - if price > highest seen → update high-water mark (trail up)
  - if price ≤ highest * (1 - tsl_pct/100) → consider firing
  - iter 57: only ACTUALLY fire if the drop velocity (% per minute from
    the peak) is fast enough.  Slow drifts get to wait — HARD-SL at the
    configured floor still catches catastrophe.

Storage is in-memory (a dict). Re-tracking a symbol after a sale wipes
its history via ``reset(symbol)``.

iter 57 (2026-05-23) — ORCAUSDT post-mortem:
  Buy $1.397, peak $1.400 (10:50), dip to $1.395x (10:56).
  Old TSL fired because drop reached the 0.29% trail (0.07% below buy).
  But the dip took 6 minutes — that's 0.06%/min, basically noise.
  New rule: TSL only fires if drop velocity >= configurable threshold
  (default 0.15%/min — that's a 0.9% drop in 6 min, real reversal).
"""

from __future__ import annotations

import logging
import time
from decimal import Decimal
from threading import RLock
from typing import Dict, Optional


logger = logging.getLogger("booknow.tsl")


class TrailingStopLoss:
    """Per-symbol high-water tracker with velocity-aware exit."""

    def __init__(
        self,
        trailing_percentage: float = 2.0,
        min_drop_pct_per_minute: float = 0.15,
    ):
        self.trailing_percentage = trailing_percentage
        # iter 57 — velocity gate.  0 (or negative) = always fire on
        # trail-break (legacy behaviour).
        self.min_drop_pct_per_minute = float(min_drop_pct_per_minute or 0.0)
        self._lock = RLock()
        self._highest: Dict[str, Decimal] = {}
        # iter 57 — when the highest was set (epoch seconds).
        self._highest_ts: Dict[str, float] = {}

    # ── Public API ───────────────────────────────────────────────────────

    def check_and_track(self, symbol: str, current_price: Decimal) -> bool:
        """Update high-water mark, return True if TSL should trigger.

        iter 57 adds a velocity gate so slow drifts don't fire.  Caller
        contract is unchanged — still returns bool.
        """
        if current_price is None or current_price <= 0:
            return False

        now = time.time()
        with self._lock:
            highest = self._highest.get(symbol)
            peak_ts = self._highest_ts.get(symbol, now)
            if highest is None:
                self._highest[symbol] = current_price
                self._highest_ts[symbol] = now
                highest = current_price
                peak_ts = now
            elif current_price > highest:
                self._highest[symbol] = current_price
                self._highest_ts[symbol] = now
                highest = current_price
                peak_ts = now
                logger.debug("[TSL] %s new high: %s", symbol, highest)

        multiplier = Decimal("1") - (Decimal(str(self.trailing_percentage)) / Decimal("100"))
        stop_loss = highest * multiplier
        if current_price > stop_loss:
            return False

        # ── iter 57: velocity gate ─────────────────────────────────────
        # Compute drop velocity in % per minute since the peak.  If the
        # drop is slow, this is normal noise — let the position breathe.
        # If the drop is fast, we're in a real reversal — fire.
        time_since_peak_s = max(1.0, now - peak_ts)  # avoid div-by-zero
        try:
            drop_pct = float((Decimal(str(highest)) - Decimal(str(current_price))) / Decimal(str(highest)) * 100)
        except Exception:
            drop_pct = 0.0
        velocity_pct_per_min = drop_pct / (time_since_peak_s / 60.0)

        if self.min_drop_pct_per_minute > 0 and velocity_pct_per_min < self.min_drop_pct_per_minute:
            logger.info(
                "[TSL] %s slow-drift — drop %.3f%% in %.0fs = %.3f%%/min < %.3f%%/min threshold "
                "(current %s vs stop %s, peak %s) — WAITING",
                symbol, drop_pct, time_since_peak_s, velocity_pct_per_min,
                self.min_drop_pct_per_minute, current_price, stop_loss, highest,
            )
            return False

        logger.info(
            "[TSL] TRIGGER SELL for %s — current %s <= stop %s (high %s, "
            "drop %.3f%%/%.0fs = %.3f%%/min)",
            symbol, current_price, stop_loss, highest,
            drop_pct, time_since_peak_s, velocity_pct_per_min,
        )
        return True

    def start_tracking(self, symbol: str, initial_price: Decimal) -> None:
        """Seed the high-water mark explicitly (called from TradeExecutor)."""
        now = time.time()
        with self._lock:
            self._highest[symbol] = initial_price
            self._highest_ts[symbol] = now
        logger.info("[TSL] start_tracking %s @ %s", symbol, initial_price)

    def reset(self, symbol: str) -> None:
        """Drop the high-water entry — call after a successful close."""
        with self._lock:
            self._highest.pop(symbol, None)
            self._highest_ts.pop(symbol, None)
        logger.debug("[TSL] reset %s", symbol)

    def highest(self, symbol: str) -> Decimal:
        """Inspector for tests + dashboards."""
        with self._lock:
            return self._highest.get(symbol, Decimal(0))

    def peak_age_seconds(self, symbol: str) -> Optional[float]:
        """Inspector — seconds since the current peak was set."""
        with self._lock:
            ts = self._highest_ts.get(symbol)
        if ts is None:
            return None
        return max(0.0, time.time() - ts)
