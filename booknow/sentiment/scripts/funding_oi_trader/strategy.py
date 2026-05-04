import logging

log = logging.getLogger("funding_oi.strategy")

class FundingOIStrategy:
    """
    Core signal logic based on Price, OI, and Funding Rate.
    """
    def __init__(self, price_weight=0.4, oi_weight=0.4, funding_weight=0.2):
        self.price_weight = price_weight
        self.oi_weight = oi_weight
        self.funding_weight = funding_weight

    def evaluate(self, symbol, price, trend, trend_strength, oi_change, funding_status, funding_rate):
        signal = "NEUTRAL"
        reason = "No clear divergence"
        confidence = 0.0

        # Logic 1: STRONG BUY (Price Up, OI Up, Funding Neutral/Slightly Pos)
        if trend == "UP" and oi_change > 0.005:
            if funding_status != "EXTREME_POSITIVE":
                signal = "STRONG BUY"
                reason = "Price and OI rising with healthy funding (Trend continuation)"
            else:
                signal = "SELL / CAUTION"
                reason = "Price rising but funding extremely high (Overcrowded longs)"

        # Logic 2: WEAK PUMP (Price Up, OI Down)
        elif trend == "UP" and oi_change < -0.005:
            signal = "WEAK BUY"
            reason = "Price rising but OI falling (Short covering move)"

        # Logic 3: STRONG SELL (Price Down, OI Up)
        elif trend == "DOWN" and oi_change > 0.005:
            signal = "STRONG SELL"
            reason = "Price falling and OI rising (Short build-up)"

        # Logic 4: Reversal - Short Squeeze
        elif funding_status == "EXTREME_NEGATIVE" and trend != "DOWN":
            signal = "BUY"
            reason = "Extreme negative funding with price support (Short squeeze potential)"

        # Confidence Scoring
        # Simplified scoring
        trend_score = trend_strength
        oi_score = min(abs(oi_change) * 20, 1.0) # 5% change = max score
        funding_score = min(abs(funding_rate) * 1000, 1.0) # 0.1% = max score
        
        confidence = (
            self.price_weight * trend_score +
            self.oi_weight * oi_score +
            self.funding_weight * funding_score
        )

        return {
            "symbol": symbol,
            "price": price,
            "price_trend": trend,
            "oi_change": round(oi_change, 4),
            "funding_rate": funding_rate,
            "signal": signal,
            "confidence": round(min(confidence, 1.0), 2),
            "reason": reason
        }
