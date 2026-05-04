"""
Indicator Engine — Efficient rolling calculations for ATR, ADX, Bollinger Bands.

All indicators use incremental updates to avoid recomputing entire history on each tick.
"""

import numpy as np
from collections import deque
from dataclasses import dataclass, field


@dataclass
class OHLCV:
    """Single OHLCV candle."""
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class IndicatorState:
    """Current state of all indicators."""
    atr: float = 0.0
    atr_sma: float = 0.0        # SMA of ATR (for volatility regime detection)
    adx: float = 0.0
    plus_di: float = 0.0
    minus_di: float = 0.0
    bb_upper: float = 0.0
    bb_middle: float = 0.0
    bb_lower: float = 0.0
    sma_fast: float = 0.0       # 9-period SMA
    sma_slow: float = 0.0       # 21-period SMA
    rsi: float = 50.0
    ready: bool = False         # True once enough data has been collected


class IndicatorEngine:
    """
    Computes ATR, ADX (+DI, -DI), Bollinger Bands, and SMAs using rolling windows.

    After initial seeding, each new candle updates incrementally via Wilder's smoothing
    (for ATR/ADX) and simple deque-based rolling windows (for BB/SMA).
    """

    def __init__(self, period: int = 14, bb_period: int = 20, bb_std: float = 2.0,
                 sma_fast: int = 9, sma_slow: int = 21, atr_ma_period: int = 20):
        self.period = period
        self.bb_period = bb_period
        self.bb_std = bb_std
        self.sma_fast_period = sma_fast
        self.sma_slow_period = sma_slow
        self.atr_ma_period = atr_ma_period

        # Rolling windows
        self._closes = deque(maxlen=max(bb_period, sma_slow, atr_ma_period) + 5)
        self._candles = deque(maxlen=period + 5)
        self._atr_history = deque(maxlen=atr_ma_period + 5)

        # Wilder smoothing state
        self._prev_atr = None
        self._prev_plus_dm_smooth = None
        self._prev_minus_dm_smooth = None
        self._prev_tr_smooth = None
        self._dx_history = deque(maxlen=period)
        self._prev_adx = None

        # RSI state
        self._prev_avg_gain = None
        self._prev_avg_loss = None
        self._prev_close = None

        self._tick_count = 0
        self._state = IndicatorState()

    @property
    def state(self) -> IndicatorState:
        return self._state

    def seed(self, candles: list[OHLCV]) -> IndicatorState:
        """
        Initialize indicators with historical candles.
        Must be called with at least `period + 1` candles.
        """
        for candle in candles:
            self.update(candle)
        return self._state

    def update(self, candle: OHLCV) -> IndicatorState:
        """
        Update all indicators with a new candle.
        Uses Wilder's smoothing for ATR/ADX (O(1) per update).
        """
        self._tick_count += 1
        self._closes.append(candle.close)
        self._candles.append(candle)

        if len(self._candles) < 2:
            self._prev_close = candle.close
            return self._state

        prev = self._candles[-2]

        # ── True Range ────────────────────────────────────────────────
        tr = max(
            candle.high - candle.low,
            abs(candle.high - prev.close),
            abs(candle.low - prev.close)
        )

        # ── Directional Movement ──────────────────────────────────────
        plus_dm = max(candle.high - prev.high, 0) if (candle.high - prev.high) > (prev.low - candle.low) else 0
        minus_dm = max(prev.low - candle.low, 0) if (prev.low - candle.low) > (candle.high - prev.high) else 0

        n = self.period

        if self._tick_count <= n + 1:
            # Accumulation phase — collect initial values
            if self._prev_tr_smooth is None:
                self._prev_tr_smooth = tr
                self._prev_plus_dm_smooth = plus_dm
                self._prev_minus_dm_smooth = minus_dm
                self._prev_atr = tr
            else:
                self._prev_tr_smooth += tr
                self._prev_plus_dm_smooth += plus_dm
                self._prev_minus_dm_smooth += minus_dm
                self._prev_atr += tr

            if self._tick_count == n + 1:
                # First smoothed values
                self._prev_atr = self._prev_atr / n
                self._prev_tr_smooth = self._prev_tr_smooth  # Keep sum for DI calc
                # Calculate first DI values
                if self._prev_tr_smooth > 0:
                    self._state.plus_di = 100 * self._prev_plus_dm_smooth / self._prev_tr_smooth
                    self._state.minus_di = 100 * self._prev_minus_dm_smooth / self._prev_tr_smooth
                # Prepare for Wilder smoothing
                self._prev_tr_smooth = self._prev_tr_smooth / n
                self._prev_plus_dm_smooth = self._prev_plus_dm_smooth / n
                self._prev_minus_dm_smooth = self._prev_minus_dm_smooth / n

                self._state.atr = self._prev_atr

                # First DX
                di_sum = self._state.plus_di + self._state.minus_di
                if di_sum > 0:
                    dx = 100 * abs(self._state.plus_di - self._state.minus_di) / di_sum
                    self._dx_history.append(dx)
        else:
            # Wilder's smoothing: ATR = (prev_ATR * (n-1) + TR) / n
            self._prev_atr = (self._prev_atr * (n - 1) + tr) / n
            self._prev_tr_smooth = (self._prev_tr_smooth * (n - 1) + tr) / n
            self._prev_plus_dm_smooth = (self._prev_plus_dm_smooth * (n - 1) + plus_dm) / n
            self._prev_minus_dm_smooth = (self._prev_minus_dm_smooth * (n - 1) + minus_dm) / n

            self._state.atr = self._prev_atr

            # +DI and -DI
            if self._prev_tr_smooth > 0:
                self._state.plus_di = 100 * self._prev_plus_dm_smooth / self._prev_tr_smooth
                self._state.minus_di = 100 * self._prev_minus_dm_smooth / self._prev_tr_smooth

            # DX → ADX
            di_sum = self._state.plus_di + self._state.minus_di
            if di_sum > 0:
                dx = 100 * abs(self._state.plus_di - self._state.minus_di) / di_sum
                self._dx_history.append(dx)

                if self._prev_adx is None and len(self._dx_history) >= n:
                    self._prev_adx = sum(self._dx_history) / n
                elif self._prev_adx is not None:
                    self._prev_adx = (self._prev_adx * (n - 1) + dx) / n

                if self._prev_adx is not None:
                    self._state.adx = self._prev_adx

        # ── ATR SMA (for volatility regime) ───────────────────────────
        self._atr_history.append(self._state.atr)
        if len(self._atr_history) >= self.atr_ma_period:
            self._state.atr_sma = sum(self._atr_history) / len(self._atr_history)

        # ── Bollinger Bands ───────────────────────────────────────────
        if len(self._closes) >= self.bb_period:
            closes_arr = list(self._closes)[-self.bb_period:]
            self._state.bb_middle = sum(closes_arr) / len(closes_arr)
            std = np.std(closes_arr, ddof=0)
            self._state.bb_upper = self._state.bb_middle + self.bb_std * std
            self._state.bb_lower = self._state.bb_middle - self.bb_std * std

        # ── SMAs ──────────────────────────────────────────────────────
        if len(self._closes) >= self.sma_fast_period:
            self._state.sma_fast = sum(list(self._closes)[-self.sma_fast_period:]) / self.sma_fast_period
        if len(self._closes) >= self.sma_slow_period:
            self._state.sma_slow = sum(list(self._closes)[-self.sma_slow_period:]) / self.sma_slow_period

        # ── RSI ───────────────────────────────────────────────────────
        if self._prev_close is not None:
            change = candle.close - self._prev_close
            gain = max(change, 0)
            loss = abs(min(change, 0))

            if self._prev_avg_gain is None:
                self._prev_avg_gain = gain
                self._prev_avg_loss = loss
            else:
                self._prev_avg_gain = (self._prev_avg_gain * (n - 1) + gain) / n
                self._prev_avg_loss = (self._prev_avg_loss * (n - 1) + loss) / n

            if self._prev_avg_loss > 0:
                rs = self._prev_avg_gain / self._prev_avg_loss
                self._state.rsi = 100 - (100 / (1 + rs))
            elif self._prev_avg_gain > 0:
                self._state.rsi = 100.0
            else:
                self._state.rsi = 50.0

        self._prev_close = candle.close

        # Mark ready once we have enough data
        min_required = max(self.period * 2, self.bb_period, self.sma_slow_period) + 2
        self._state.ready = self._tick_count >= min_required

        return self._state
