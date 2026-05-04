"""
Market Regime Detector — Classifies market into TRENDING, RANGING, or VOLATILE.

Uses ADX and ATR with persistence-based smoothing to prevent rapid regime flipping.
"""

import time
from enum import Enum
from dataclasses import dataclass
from indicators import IndicatorState


class Regime(Enum):
    TRENDING = "TRENDING"
    RANGING = "RANGING"
    VOLATILE = "VOLATILE"
    UNKNOWN = "UNKNOWN"


@dataclass
class RegimeState:
    """Current regime classification with metadata."""
    regime: Regime = Regime.UNKNOWN
    confidence: float = 0.0          # 0–100
    adx: float = 0.0
    atr: float = 0.0
    atr_ratio: float = 0.0          # ATR / ATR_SMA
    plus_di: float = 0.0
    minus_di: float = 0.0
    trend_direction: str = "FLAT"    # UP, DOWN, FLAT
    persistence_count: int = 0       # How many ticks in current regime
    last_switch_time: float = 0.0


class RegimeDetector:
    """
    Classifies market regime based on ADX and ATR analysis.

    Rules:
        TRENDING  → ADX > adx_trending_threshold (default 25)
        RANGING   → ADX < adx_ranging_threshold (default 20) AND ATR ratio < 1.2
        VOLATILE  → ATR significantly above its moving average (ratio > volatility_threshold)

    Smoothing:
        A regime change only takes effect after `confirmation_ticks` consecutive
        signals for the new regime. This prevents whipsawing.
    """

    def __init__(
        self,
        adx_trending_threshold: float = 25.0,
        adx_ranging_threshold: float = 20.0,
        volatility_threshold: float = 1.5,
        confirmation_ticks: int = 3,
    ):
        self.adx_trending = adx_trending_threshold
        self.adx_ranging = adx_ranging_threshold
        self.volatility_threshold = volatility_threshold
        self.confirmation_ticks = confirmation_ticks

        self._state = RegimeState()
        self._candidate_regime = Regime.UNKNOWN
        self._candidate_count = 0

    @property
    def state(self) -> RegimeState:
        return self._state

    @property
    def current_regime(self) -> Regime:
        return self._state.regime

    def update(self, indicators: IndicatorState) -> RegimeState:
        """
        Classify regime from latest indicator values.
        Returns the (possibly unchanged) RegimeState.
        """
        if not indicators.ready:
            return self._state

        adx = indicators.adx
        atr = indicators.atr
        atr_sma = indicators.atr_sma if indicators.atr_sma > 0 else atr
        atr_ratio = atr / atr_sma if atr_sma > 0 else 1.0
        plus_di = indicators.plus_di
        minus_di = indicators.minus_di

        # ── Raw classification ────────────────────────────────────────
        if atr_ratio > self.volatility_threshold:
            raw_regime = Regime.VOLATILE
            confidence = min((atr_ratio - 1.0) / 1.0 * 100, 100)
        elif adx > self.adx_trending:
            raw_regime = Regime.TRENDING
            confidence = min((adx - self.adx_trending) / 25.0 * 100, 100)
        elif adx < self.adx_ranging and atr_ratio < 1.2:
            raw_regime = Regime.RANGING
            confidence = min((self.adx_ranging - adx) / self.adx_ranging * 100, 100)
        else:
            # Transitional zone — keep current regime
            raw_regime = self._state.regime if self._state.regime != Regime.UNKNOWN else Regime.RANGING
            confidence = 30.0

        # ── Persistence smoothing ─────────────────────────────────────
        if raw_regime == self._candidate_regime:
            self._candidate_count += 1
        else:
            self._candidate_regime = raw_regime
            self._candidate_count = 1

        # Only switch if candidate has been consistent for N ticks
        if self._candidate_count >= self.confirmation_ticks and raw_regime != self._state.regime:
            self._state.regime = raw_regime
            self._state.persistence_count = 0
            self._state.last_switch_time = time.time()
        else:
            self._state.persistence_count += 1

        # ── Trend direction from DI ───────────────────────────────────
        if plus_di > minus_di + 5:
            direction = "UP"
        elif minus_di > plus_di + 5:
            direction = "DOWN"
        else:
            direction = "FLAT"

        self._state.confidence = round(confidence, 1)
        self._state.adx = round(adx, 2)
        self._state.atr = round(atr, 6)
        self._state.atr_ratio = round(atr_ratio, 3)
        self._state.plus_di = round(plus_di, 2)
        self._state.minus_di = round(minus_di, 2)
        self._state.trend_direction = direction

        return self._state
