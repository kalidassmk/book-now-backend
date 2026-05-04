import logging

log = logging.getLogger("fakeout.strategy")

class FakeoutStrategy:
    """
    Generates signals based on fakeout analysis.
    """
    def evaluate(self, symbol, analysis, price):
        if not analysis or not analysis["is_fakeout"]:
            return {
                "symbol": symbol,
                "signal": "NEUTRAL",
                "confidence": 0.0,
                "reason": "No fakeout pattern detected."
            }

        signal = "NEUTRAL"
        if analysis["type"] == "RESISTANCE_FAKE":
            signal = "STRONG SELL"
        elif analysis["type"] == "SUPPORT_FAKE":
            signal = "STRONG BUY"

        return {
            "symbol": symbol,
            "breakout_type": analysis["type"],
            "fakeout_detected": True,
            "signal": signal,
            "confidence": round(analysis["strength"], 2),
            "reason": analysis["reason"]
        }
