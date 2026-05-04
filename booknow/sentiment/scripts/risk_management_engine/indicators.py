import pandas as pd
import numpy as np

class RiskIndicators:
    """
    Calculates volatility and risk metrics.
    """
    @staticmethod
    def calculate_atr(df, period=14):
        """
        Calculates Average True Range (ATR).
        """
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = np.max(ranges, axis=1)
        
        atr = true_range.rolling(window=period).mean()
        return atr.iloc[-1]
