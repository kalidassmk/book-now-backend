import logging
from collections import deque

log = logging.getLogger("obi_trader.signal")

class SignalGenerator:
    """
    Generates trading signals based on OBI metrics.
    Filters noise using a persistence window.
    """
    def __init__(self, pressure_threshold=0.3, persistence_count=5):
        self.pressure_threshold = pressure_threshold
        self.persistence_count = persistence_count
        self.history = deque(maxlen=persistence_count)

    def generate(self, metrics):
        if not metrics:
            return None

        pressure = metrics['weighted_imbalance']
        self.history.append(pressure)

        if len(self.history) < self.persistence_count:
            return "HOLD"

        # Check persistence
        all_bullish = all(p > self.pressure_threshold for p in self.history)
        all_bearish = all(p < -self.pressure_threshold for p in self.history)

        if all_bullish:
            return "BUY"
        elif all_bearish:
            return "SELL"
        
        return "HOLD"

class StrategyManager:
    """
    Manages entries and exits based on signals and current position.
    """
    def __init__(self, executor):
        self.executor = executor
        self.in_position = False

    def on_signal(self, symbol, signal, price, metrics):
        if signal == "BUY" and not self.in_position:
            log.info(f"SIGNAL: Strong Buy Pressure detected. Entering position for {symbol}")
            self.executor.open_position(symbol, price, "BUY", metrics['bid_walls'])
            self.in_position = True
        
        elif signal == "SELL" and self.in_position:
            log.info(f"SIGNAL: Pressure Reversal or Sell Signal. Exiting position for {symbol}")
            self.executor.close_position(symbol, price, "SELL")
            self.in_position = False
        
        # Additional exit logic: if imbalance reverses significantly
        elif self.in_position and metrics['weighted_imbalance'] < -0.1:
            log.info(f"EXIT: Pressure reversed to {metrics['weighted_imbalance']}. Closing position.")
            self.executor.close_position(symbol, price, "REVERSAL_EXIT")
            self.in_position = False
