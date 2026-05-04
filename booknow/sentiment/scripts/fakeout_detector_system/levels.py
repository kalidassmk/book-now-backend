import pandas as pd
import numpy as np

class LevelsManager:
    """
    Identifies Support and Resistance levels based on Swing Highs/Lows.
    """
    @staticmethod
    def identify(df, window=20):
        """
        df: DataFrame with OHLCV data.
        window: Number of candles for swing lookback.
        """
        # Resistance: Highest high in the window (excluding current candle)
        resistance = df['high'].shift(1).rolling(window=window).max().iloc[-1]
        
        # Support: Lowest low in the window (excluding current candle)
        support = df['low'].shift(1).rolling(window=window).min().iloc[-1]
        
        return {
            "resistance": resistance,
            "support": support
        }
