import logging

log = logging.getLogger("trend_alignment.strategy")

class TrendAlignmentStrategy:
    """
    Generates signals based on trend alignment.
    """
    def evaluate(self, symbol, alignment_data):
        """
        alignment_data: output from AlignmentEngine.calculate()
        """
        score = alignment_data['weighted_score']
        pct = alignment_data['alignment_percentage']
        trends = alignment_data['trends']
        
        signal = "NO TRADE"
        confidence = pct / 100.0
        reason = f"Alignment at {pct}% below threshold"

        if alignment_data['is_aligned']:
            if score > 0:
                signal = "STRONG BUY"
                reason = "Multi-timeframe bullish alignment detected"
            else:
                signal = "STRONG SELL"
                reason = "Multi-timeframe bearish alignment detected"
        else:
            # Check for Early Reversal
            low_tf_bullish = trends.get("5m") == "bullish" and trends.get("15m") == "bullish"
            high_tf_bearish = trends.get("1d") == "bearish" or trends.get("1w") == "bearish"
            
            if low_tf_bullish and high_tf_bearish:
                signal = "EARLY REVERSAL"
                reason = "Low timeframe bullish momentum vs high timeframe bearish trend"

        return {
            "symbol": symbol,
            "timeframe_trends": trends,
            "alignment_score": pct,
            "weighted_score": score,
            "signal": signal,
            "confidence": round(confidence, 2),
            "reason": reason
        }
