"""
Range Strategy ("Band Cross") — Active during RANGING regime.

Uses Bollinger Bands for mean reversion:
  Entry BUY:   Price touches lower BB + RSI < 35 (oversold near support)
  Entry SELL:  Price touches upper BB + RSI > 65 (overbought near resistance)
  Exit:        Price returns to middle BB (mean reversion complete)
"""

from indicators import IndicatorState
from regime_detector import RegimeState
from strategies.base import BaseStrategy, TradeSignal, Signal


class RangeStrategy(BaseStrategy):
    """
    Mean-reversion strategy using Bollinger Band bounces.
    Designed for RANGING markets (ADX < 20, low ATR).
    """

    def __init__(self, rsi_oversold: float = 35.0, rsi_overbought: float = 65.0,
                 bb_touch_threshold: float = 0.02):
        self.rsi_oversold = rsi_oversold
        self.rsi_overbought = rsi_overbought
        self.bb_touch_pct = bb_touch_threshold  # How close to BB to count as "touch"
        self._in_position = False
        self._position_side = None  # 'LONG' or 'SHORT'

    @property
    def name(self) -> str:
        return "RangeStrategy (Band Cross)"

    @property
    def target_regime(self) -> str:
        return "RANGING"

    def evaluate(self, price: float, indicators: IndicatorState,
                 regime: RegimeState) -> TradeSignal:
        signal = TradeSignal(strategy_name=self.name)

        if not indicators.ready:
            return signal

        bb_upper = indicators.bb_upper
        bb_lower = indicators.bb_lower
        bb_middle = indicators.bb_middle
        rsi = indicators.rsi
        atr = indicators.atr

        if bb_upper <= bb_lower:
            return signal

        # Band width and price position within bands
        band_width = bb_upper - bb_lower
        price_position = (price - bb_lower) / band_width if band_width > 0 else 0.5

        # How close is price to lower/upper band (as % of band width)
        near_lower = price_position < self.bb_touch_pct + 0.05
        near_upper = price_position > (1.0 - self.bb_touch_pct - 0.05)
        near_middle = 0.4 < price_position < 0.6

        # ── Decision ─────────────────────────────────────────────────
        if not self._in_position:
            if near_lower and rsi < self.rsi_oversold:
                signal.signal = Signal.BUY
                signal.confidence = min(80 + (self.rsi_oversold - rsi), 100)
                signal.reason = (
                    f"BB lower touch (pos={price_position:.2f}) + RSI oversold ({rsi:.1f})"
                )
                signal.entry_price = price
                signal.stop_loss = bb_lower - (atr * 0.5)     # Tight stop below BB
                signal.take_profit = bb_middle                  # Target: mean reversion
                signal.position_size_pct = 70.0                 # Smaller size in range
                self._in_position = True
                self._position_side = 'LONG'

            elif near_upper and rsi > self.rsi_overbought:
                signal.signal = Signal.SELL
                signal.confidence = min(80 + (rsi - self.rsi_overbought), 100)
                signal.reason = (
                    f"BB upper touch (pos={price_position:.2f}) + RSI overbought ({rsi:.1f})"
                )
                self._in_position = True
                self._position_side = 'SHORT'
        else:
            # Exit when price reverts to mean
            if self._position_side == 'LONG' and (near_middle or near_upper):
                signal.signal = Signal.SELL
                signal.confidence = 70
                signal.reason = f"Mean reversion complete (pos={price_position:.2f})"
                self._in_position = False
                self._position_side = None

            elif self._position_side == 'SHORT' and (near_middle or near_lower):
                signal.signal = Signal.BUY
                signal.confidence = 70
                signal.reason = f"Short cover — mean reversion complete (pos={price_position:.2f})"
                self._in_position = False
                self._position_side = None

        return signal

    def reset(self):
        self._in_position = False
        self._position_side = None
