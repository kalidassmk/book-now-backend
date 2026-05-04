import pandas as pd
import numpy as np

class IndicatorCalculator:
    """
    Computes technical indicators and derivatives metrics.
    """
    @staticmethod
    def calculate_price_trend(klines):
        """
        klines: list of kline lists
        Returns: current_price, trend (UP/DOWN/FLAT), trend_strength
        """
        df = pd.DataFrame(klines, columns=['time', 'open', 'high', 'low', 'close', 'vol', 'close_time', 'q_vol', 'trades', 't_buy_vol', 't_buy_q_vol', 'ignore'])
        df['close'] = df['close'].astype(float)
        
        ema9 = df['close'].ewm(span=9, adjust=False).mean()
        ema21 = df['close'].ewm(span=21, adjust=False).mean()
        
        current_price = df['close'].iloc[-1]
        last_ema9 = ema9.iloc[-1]
        last_ema21 = ema21.iloc[-1]
        
        trend = "FLAT"
        if last_ema9 > last_ema21 * 1.0005:
            trend = "UP"
        elif last_ema9 < last_ema21 * 0.9995:
            trend = "DOWN"
            
        # Strength based on gap between EMAs
        trend_strength = abs(last_ema9 - last_ema21) / last_ema21 * 1000 # scaling factor
        return current_price, trend, min(trend_strength, 1.0)

    @staticmethod
    def calculate_oi_change(oi_hist):
        """
        oi_hist: list of OI history data
        Returns: current_oi, oi_change_pct
        """
        if not oi_hist or len(oi_hist) < 2:
            return 0, 0
            
        current_oi = float(oi_hist[-1]['sumOpenInterest'])
        prev_oi = float(oi_hist[-2]['sumOpenInterest'])
        
        oi_change_pct = (current_oi - prev_oi) / prev_oi
        return current_oi, oi_change_pct

    @staticmethod
    def normalize_funding(funding_rate):
        """
        Normalize funding rate signal.
        Extreme: +/- 0.05%
        """
        f = float(funding_rate)
        if f > 0.0005:
            return "EXTREME_POSITIVE", f
        elif f < -0.0005:
            return "EXTREME_NEGATIVE", f
        return "NEUTRAL", f
