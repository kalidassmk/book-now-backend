class StrategyFilter:
    """
    Applies global BTC filters to individual altcoin signals.
    """
    @staticmethod
    def apply(symbol, btc_trend, btc_score, correlation):
        """
        Determines if a trade is allowed based on BTC context.
        """
        allowed = True
        reason = "BTC context is supportive or asset is decoupled."
        adjusted_signal = "KEEP"

        # Rule 1: Weak Correlation Exception (Independence)
        if correlation < 0.3:
            return {
                "symbol": symbol,
                "btc_trend": btc_trend,
                "btc_score": btc_score,
                "correlation": correlation,
                "trade_allowed": True,
                "filter_reason": "Low correlation (<0.3). Asset moving independently.",
                "adjusted_signal": "KEEP"
            }

        # Rule 2: Block BUYs in Bearish BTC (High Correlation)
        if btc_trend == "bearish" and correlation > 0.6:
            allowed = False
            reason = "BTC is bearish and correlation is high (>0.6). High risk of drag-down."
            adjusted_signal = "NO TRADE"

        # Rule 3: Block BUYs if BTC Strength is very low
        elif btc_score < -0.3:
            allowed = False
            reason = f"BTC strength is too weak ({btc_score}). Global market risk high."
            adjusted_signal = "NO TRADE"

        # Rule 4: High Volatility / Unclear Trend
        elif abs(btc_score) < 0.1:
            allowed = True
            reason = "BTC is neutral/sideways. Exercise caution."
            adjusted_signal = "CAUTION"

        return {
            "symbol": symbol,
            "btc_trend": btc_trend,
            "btc_score": btc_score,
            "correlation": correlation,
            "trade_allowed": allowed,
            "filter_reason": reason,
            "adjusted_signal": adjusted_signal
        }
