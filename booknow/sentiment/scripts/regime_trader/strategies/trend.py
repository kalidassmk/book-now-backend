"""
Trend Strategy ("Steady Climb") — Active during TRENDING regime.

Uses dual confirmation:
  1. SMA crossover (fast > slow = bullish, fast < slow = bearish)
  2. +DI / -DI alignment from ADX

Entry:  SMA fast crosses above slow AND +DI > -DI → BUY
Exit:   SMA fast crosses below slow OR -DI > +DI → SELL
"""

from indicators import IndicatorState
from regime_detector import RegimeState
from strategies.base import BaseStrategy, TradeSignal, Signal


class TrendStrategy(BaseStrategy):
    """
    Trend-following strategy using SMA crossover + DI alignment.
    Designed for TRENDING markets (ADX > 25).
    """

    def __init__(self, atr_sl_multiplier: float = 2.0, atr_tp_multiplier: float = 3.0):
        self.atr_sl_multiplier = atr_sl_multiplier
        self.atr_tp_multiplier = atr_tp_multiplier
        self._prev_sma_fast = None
        self._prev_sma_slow = None
        self._in_position = False

    @property
    def name(self) -> str:
        return "TrendStrategy (Steady Climb)"

    @property
    def target_regime(self) -> str:
        return "TRENDING"

    def evaluate(self, price: float, indicators: IndicatorState,
                 regime: RegimeState) -> TradeSignal:
        signal = TradeSignal(strategy_name=self.name)

        if not indicators.ready:
            return signal

        sma_fast = indicators.sma_fast
        sma_slow = indicators.sma_slow
        plus_di = indicators.plus_di
        minus_di = indicators.minus_di
        atr = indicators.atr

        # ── BUY conditions ────────────────────────────────────────────
        # 1. SMA crossover: fast just crossed above slow
        crossover = (
            self._prev_sma_fast is not None and
            self._prev_sma_fast <= self._prev_sma_slow and
            sma_fast > sma_slow
        )
        # 2. Or already above with strong DI confirmation
        sma_bullish = sma_fast > sma_slow
        di_bullish = plus_di > minus_di + 3  # +DI leads by at least 3

        # ── SELL conditions ───────────────────────────────────────────
        crossunder = (
            self._prev_sma_fast is not None and
            self._prev_sma_fast >= self._prev_sma_slow and
            sma_fast < sma_slow
        )
        di_bearish = minus_di > plus_di + 3

        # ── Decision ─────────────────────────────────────────────────
        if not self._in_position:
            if crossover and di_bullish:
                signal.signal = Signal.BUY
                signal.confidence = min(regime.confidence + 20, 100)
                signal.reason = f"SMA crossover + DI bullish (+DI={plus_di:.1f} > -DI={minus_di:.1f})"
                signal.entry_price = price
                signal.stop_loss = price - (atr * self.atr_sl_multiplier)
                signal.take_profit = price + (atr * self.atr_tp_multiplier)
                signal.position_size_pct = 100.0
                self._in_position = True
            elif sma_bullish and di_bullish and regime.adx > 30:
                # Strong trend confirmation even without fresh crossover
                signal.signal = Signal.BUY
                signal.confidence = min(regime.confidence, 80)
                signal.reason = f"Strong trend: ADX={regime.adx:.1f}, SMA bullish, DI aligned"
                signal.entry_price = price
                signal.stop_loss = price - (atr * self.atr_sl_multiplier)
                signal.take_profit = price + (atr * self.atr_tp_multiplier)
                signal.position_size_pct = 80.0
                self._in_position = True
        else:
            if crossunder or di_bearish:
                signal.signal = Signal.SELL
                signal.confidence = 80
                signal.reason = f"SMA crossunder or DI bearish (-DI={minus_di:.1f} > +DI={plus_di:.1f})"
                self._in_position = False

        # Update previous values
        self._prev_sma_fast = sma_fast
        self._prev_sma_slow = sma_slow

        return signal

    def reset(self):
        self._prev_sma_fast = None
        self._prev_sma_slow = None
        self._in_position = False
