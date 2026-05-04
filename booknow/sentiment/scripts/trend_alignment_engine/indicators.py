import pandas as pd
import numpy as np

class TrendIndicator:
    """
    Determines trend using EMA, Price Structure, and Momentum.
    """
    @staticmethod
    def analyze(klines):
        """
        klines: list of kline lists
        Returns: trend_score (1, -1, 0), details
        """
        if not klines or len(klines) < 30:
            return 0, "insufficient data"

        # Prepare DataFrame
        num_cols = len(klines[0])
        if num_cols >= 12:
            cols = ['time', 'open', 'high', 'low', 'close', 'volume', 
                    'close_time', 'q_vol', 'trades', 't_buy_vol', 't_buy_q_vol', 'ignore']
        else:
            cols = ['time', 'open', 'high', 'low', 'close', 'volume']
            
        df = pd.DataFrame(klines, columns=cols[:num_cols])
        df['close'] = df['close'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)

        # 1. EMA Trend (9, 21)
        ema9 = df['close'].ewm(span=9, adjust=False).mean()
        ema21 = df['close'].ewm(span=21, adjust=False).mean()
        
        ema_bullish = ema9.iloc[-1] > ema21.iloc[-1]
        ema_bearish = ema9.iloc[-1] < ema21.iloc[-1]

        # 2. Price Structure (Higher Highs / Lower Lows)
        # Check last 3 pivots (simplified)
        last_highs = df['high'].rolling(window=5).max()
        last_lows = df['low'].rolling(window=5).min()
        
        struct_bullish = df['high'].iloc[-1] > last_highs.iloc[-6] and df['low'].iloc[-1] > last_lows.iloc[-6]
        struct_bearish = df['low'].iloc[-1] < last_lows.iloc[-6] and df['high'].iloc[-1] < last_highs.iloc[-6]

        # 3. Momentum (% change last 10 candles)
        mom = (df['close'].iloc[-1] - df['close'].iloc[-11]) / df['close'].iloc[-11]
        mom_bullish = mom > 0.002 # 0.2%
        mom_bearish = mom < -0.002

        # Final Score
        bull_votes = sum([ema_bullish, struct_bullish, mom_bullish])
        bear_votes = sum([ema_bearish, struct_bearish, mom_bearish])

        if bull_votes >= 2:
            return 1, "bullish"
        elif bear_votes >= 2:
            return -1, "bearish"
        else:
            return 0, "neutral"
