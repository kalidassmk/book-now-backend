import logging

log = logging.getLogger("volume_profile.strategy")

class VolumeProfileStrategy:
    """
    Generates signals based on POC, VAH, and VAL.
    """
    def evaluate(self, symbol, data):
        """
        data: output from VolumeProfileEngine.calculate()
        """
        if not data:
            return None

        price = data['current_price']
        poc = data['poc']
        vah = data['vah']
        val = data['val']
        
        signal = "NEUTRAL"
        reason = "Price within value area"
        confidence = 0.0

        # 1. Support Logic (Price near VAL)
        if val * 0.998 <= price <= val * 1.002:
            signal = "BUY"
            reason = "Price at Value Area Low (Strong Support)"
            confidence = 0.75

        # 2. Resistance Logic (Price near VAH)
        elif vah * 0.998 <= price <= vah * 1.002:
            signal = "SELL"
            reason = "Price at Value Area High (Strong Resistance)"
            confidence = 0.75

        # 3. Breakout Logic
        elif price > vah * 1.005:
            signal = "STRONG BUY"
            reason = "Bullish Breakout above Value Area High"
            confidence = 0.85
        elif price < val * 0.995:
            signal = "STRONG SELL"
            reason = "Bearish Breakdown below Value Area Low"
            confidence = 0.85

        # 4. Magnet Zone (far from POC)
        distance_to_poc = abs(price - poc) / poc
        if signal == "NEUTRAL" and distance_to_poc > 0.02:
            reason = f"Price extended from POC ({distance_to_poc:.2%}). Expect mean reversion."
            confidence = 0.40

        return {
            "symbol": symbol,
            "poc": poc,
            "vah": vah,
            "val": val,
            "current_price": price,
            "signal": signal,
            "confidence": round(confidence, 2),
            "reason": reason
        }
