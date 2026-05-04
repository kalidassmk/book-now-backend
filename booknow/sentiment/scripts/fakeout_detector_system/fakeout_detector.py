import pandas as pd
import numpy as np
import logging

log = logging.getLogger("fakeout.detector")

class FakeoutDetector:
    """
    Core logic to detect fake breakouts and liquidity sweeps.
    """
    def __init__(self, wick_threshold=2.0, volume_threshold=1.1):
        self.wick_threshold = wick_threshold
        self.volume_threshold = volume_threshold

    def analyze(self, df, levels):
        """
        Analyzes the last few candles for fakeout patterns.
        """
        if df is None or len(df) < 5:
            return None

        res = levels['resistance']
        sup = levels['support']
        
        # Current and previous candle data
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        
        avg_vol = df['volume'].iloc[-21:-1].mean()
        
        analysis = {
            "is_fakeout": False,
            "type": None,
            "strength": 0.0,
            "reason": ""
        }

        # 1. Bearish Fake Breakout Detection (Resistance Break)
        if prev['close'] > res: # Breakout occurred on previous candle
            # Check for failure to continue or rejection
            if curr['close'] < res: # Re-entered the range
                analysis["is_fakeout"] = True
                analysis["type"] = "RESISTANCE_FAKE"
                analysis["reason"] = "Price broke resistance but failed to hold and re-entered range."
                
                # Check for rejection wick strength
                upper_wick = curr['high'] - max(curr['open'], curr['close'])
                body = abs(curr['open'] - curr['close'])
                if body > 0 and (upper_wick / body) > self.wick_threshold:
                    analysis["strength"] += 0.5
                    analysis["reason"] += " Strong rejection wick present."

                # Check volume
                if prev['volume'] < avg_vol:
                    analysis["strength"] += 0.4
                    analysis["reason"] += " Low breakout volume."

        # 2. Bullish Fake Breakdown Detection (Support Break)
        elif prev['close'] < sup: # Breakdown occurred on previous candle
            if curr['close'] > sup: # Re-entered the range
                analysis["is_fakeout"] = True
                analysis["type"] = "SUPPORT_FAKE"
                analysis["reason"] = "Price broke support but failed to hold and re-entered range."

                # Check for rejection wick strength
                lower_wick = min(curr['open'], curr['close']) - curr['low']
                body = abs(curr['open'] - curr['close'])
                if body > 0 and (lower_wick / body) > self.wick_threshold:
                    analysis["strength"] += 0.5
                    analysis["reason"] += " Strong rejection wick present."

                # Check volume
                if prev['volume'] < avg_vol:
                    analysis["strength"] += 0.4
                    analysis["reason"] += " Low breakdown volume."

        return analysis
