"""
Base Strategy Interface — All strategies implement this contract.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from indicators import IndicatorState
from regime_detector import RegimeState


class Signal(Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


@dataclass
class TradeSignal:
    """Output from a strategy evaluation."""
    signal: Signal = Signal.HOLD
    confidence: float = 0.0       # 0–100
    reason: str = ""
    strategy_name: str = ""
    entry_price: float = 0.0
    stop_loss: float = 0.0
    take_profit: float = 0.0
    position_size_pct: float = 100.0   # % of normal position size


class BaseStrategy(ABC):
    """
    Base class for all trading strategies.
    Each strategy is activated by a specific market regime.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable strategy name."""
        ...

    @property
    @abstractmethod
    def target_regime(self) -> str:
        """Which regime activates this strategy (TRENDING, RANGING, VOLATILE)."""
        ...

    @abstractmethod
    def evaluate(self, price: float, indicators: IndicatorState,
                 regime: RegimeState) -> TradeSignal:
        """
        Evaluate current market conditions and return a trade signal.

        Args:
            price: Current market price
            indicators: Latest indicator values (ATR, ADX, BB, etc.)
            regime: Current regime classification

        Returns:
            TradeSignal with BUY/SELL/HOLD decision
        """
        ...

    @abstractmethod
    def reset(self):
        """Reset strategy state (called on regime change)."""
        ...
