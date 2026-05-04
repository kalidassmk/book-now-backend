"""
safety.py
─────────────────────────────────────────────────────────────────────────────
Anti-parabolic safety gate ported from TradingSafetyService.java.

Pure boolean check used by CoinAnalyzer to override an otherwise-bullish
score when the coin's already overextended (price spike, volume spike,
or RSI overbought). Three knobs, all configurable:

    price_spike_threshold   default 1.50  (current > SMA × 1.5 = block)
    volume_surge_threshold  default 5.0   (current > avg × 5 = block)
    rsi_overbought_limit    default 80.0  (RSI > 80 = block)

Java reads these from application.properties; Python takes them in
the constructor and keeps the defaults.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass


logger = logging.getLogger("booknow.safety")


@dataclass
class SafetyConfig:
    price_spike_threshold: float = 1.5
    volume_surge_threshold: float = 5.0
    rsi_overbought_limit: float = 80.0


class TradingSafety:
    """Stateless evaluator. One instance per engine; share freely."""

    def __init__(self, config: SafetyConfig | None = None):
        self.cfg = config or SafetyConfig()

    def is_safe_to_buy(
        self,
        symbol: str,
        current_price: float,
        sma: float,
        current_volume: float,
        avg_volume: float,
        rsi_value: float,
    ) -> bool:
        """Return True when none of the parabolic-risk conditions trip."""
        is_price_parabolic = sma > 0 and current_price > sma * self.cfg.price_spike_threshold
        is_volume_abnormal = avg_volume > 0 and current_volume > avg_volume * self.cfg.volume_surge_threshold
        is_overbought      = rsi_value > self.cfg.rsi_overbought_limit

        if is_price_parabolic or is_volume_abnormal or is_overbought:
            reasons = []
            if is_price_parabolic:
                reasons.append(
                    f"price {current_price} > {(self.cfg.price_spike_threshold - 1) * 100:.0f}% above SMA {sma}",
                )
            if is_volume_abnormal:
                reasons.append(
                    f"volume {current_volume} > {self.cfg.volume_surge_threshold}x avg {avg_volume}",
                )
            if is_overbought:
                reasons.append(f"RSI {rsi_value:.1f} > {self.cfg.rsi_overbought_limit}")
            logger.warning(
                "[Safety] %s BLOCKED — parabolic risk: %s",
                symbol, "; ".join(reasons),
            )
            return False
        return True
