"""
Volatility Strategy — Active during VOLATILE regime.

Defensive strategy that reduces exposure during high-volatility periods:
  - Avoids new entries entirely if ATR ratio > 2.0 (extreme volatility)
  - Reduces position size if ATR ratio is moderately high (1.5–2.0)
  - Closes existing positions if volatility spike occurs
"""

from indicators import IndicatorState
from regime_detector import RegimeState
from strategies.base import BaseStrategy, TradeSignal, Signal


class VolatilityStrategy(BaseStrategy):
    """
    Defensive strategy that protects capital during volatile markets.
    Reduces or avoids positions when ATR is significantly above average.
    """

    def __init__(self, extreme_atr_ratio: float = 2.0):
        self.extreme_atr_ratio = extreme_atr_ratio
        self._warned = False

    @property
    def name(self) -> str:
        return "VolatilityStrategy (Defensive)"

    @property
    def target_regime(self) -> str:
        return "VOLATILE"

    def evaluate(self, price: float, indicators: IndicatorState,
                 regime: RegimeState) -> TradeSignal:
        signal = TradeSignal(strategy_name=self.name)

        if not indicators.ready:
            return signal

        atr_ratio = regime.atr_ratio

        if atr_ratio > self.extreme_atr_ratio:
            # Extreme volatility — close all positions, no new trades
            signal.signal = Signal.SELL
            signal.confidence = 95
            signal.reason = (
                f"EXTREME volatility! ATR ratio={atr_ratio:.2f}x. "
                f"Close positions and wait."
            )
            signal.position_size_pct = 0  # No position allowed
            self._warned = True

        elif atr_ratio > 1.5:
            # Moderate volatility — reduce size but allow cautious entries
            signal.signal = Signal.HOLD
            signal.confidence = 60
            signal.reason = (
                f"Elevated volatility (ATR ratio={atr_ratio:.2f}x). "
                f"Reduced position size recommended."
            )
            # Scale position inversely with volatility
            signal.position_size_pct = max(20, 100 - (atr_ratio - 1.0) * 60)

            # If +DI strongly leads and we haven't warned, allow small buy
            if regime.plus_di > regime.minus_di + 10 and regime.adx > 20:
                signal.signal = Signal.BUY
                signal.confidence = 40
                signal.reason += f" Cautious trend entry (DI alignment)."
                signal.entry_price = price
                signal.stop_loss = price - (indicators.atr * 3.0)  # Wide stop
                signal.take_profit = price + (indicators.atr * 2.0)
        else:
            signal.signal = Signal.HOLD
            signal.reason = f"Volatility subsiding (ATR ratio={atr_ratio:.2f}x). Monitoring."
            signal.position_size_pct = 60
            self._warned = False

        return signal

    def reset(self):
        self._warned = False
