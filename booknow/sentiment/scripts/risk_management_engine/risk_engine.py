import logging

log = logging.getLogger("risk.engine")

class RiskEngine:
    """
    Core risk logic: Position sizing, SL/TP calculation, and trade validation.
    """
    def __init__(self, risk_per_trade=0.01, sl_multiplier=2.0, rr_ratio=2.0):
        self.risk_per_trade = risk_per_trade
        self.sl_multiplier = sl_multiplier
        self.rr_ratio = rr_ratio

    def calculate_trade_params(self, symbol, entry_price, side, atr, portfolio_state):
        """
        Calculates position size, SL, and TP.
        """
        if portfolio_state["trading_halted"]:
            return self._rejected(symbol, "Trading halted due to max drawdown")

        if portfolio_state["active_trades"] >= 3:
            return self._rejected(symbol, "Max active trades reached")

        # 1. Stop Loss Distance
        sl_distance = atr * self.sl_multiplier
        
        # 2. Stop Loss and Take Profit Prices
        if side.upper() == "BUY":
            sl_price = entry_price - sl_distance
            tp_price = entry_price + (sl_distance * self.rr_ratio)
        else:
            sl_price = entry_price + sl_distance
            tp_price = entry_price - (sl_distance * self.rr_ratio)

        # 3. Position Sizing
        equity = portfolio_state["equity"]
        
        # Dynamic Risk Reduction if drawdown > 10%
        current_risk_pct = self.risk_per_trade
        if portfolio_state["drawdown"] > 0.10:
            current_risk_pct *= 0.5
            log.warning(f"Drawdown > 10%. Reducing risk to {current_risk_pct:.2%}")

        risk_amount = equity * current_risk_pct
        
        if sl_distance == 0:
            return self._rejected(symbol, "ATR is 0, cannot calculate SL")
            
        position_size = risk_amount / sl_distance

        # 4. Validation
        if position_size <= 0:
            return self._rejected(symbol, "Invalid position size")

        return {
            "symbol": symbol,
            "entry_price": round(entry_price, 4),
            "side": side,
            "position_size": round(position_size, 6),
            "stop_loss": round(sl_price, 4),
            "take_profit": round(tp_price, 4),
            "risk_amount": round(risk_amount, 2),
            "risk_reward_ratio": self.rr_ratio,
            "trade_allowed": True,
            "drawdown": portfolio_state["drawdown"],
            "reason": "Risk parameters valid"
        }

    def _rejected(self, symbol, reason):
        return {
            "symbol": symbol,
            "trade_allowed": False,
            "reason": reason
        }
