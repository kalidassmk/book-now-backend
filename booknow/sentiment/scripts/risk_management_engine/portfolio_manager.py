import redis
import json

class PortfolioManager:
    """
    Tracks account equity, drawdown, and trade limits.
    """
    def __init__(self, redis_client, initial_balance=10000):
        self.redis = redis_client
        self.initial_balance = initial_balance
        self.state_key = "RISK_PORTFOLIO_STATE"

    def get_state(self):
        state = self.redis.get(self.state_key)
        if state:
            return json.loads(state)
        return {
            "equity": self.initial_balance,
            "peak_equity": self.initial_balance,
            "drawdown": 0.0,
            "active_trades": 0,
            "trading_halted": False
        }

    def update_equity(self, current_equity):
        state = self.get_state()
        state["equity"] = current_equity
        
        if current_equity > state["peak_equity"]:
            state["peak_equity"] = current_equity
        
        # Calculate Drawdown
        drawdown = (state["peak_equity"] - current_equity) / state["peak_equity"]
        state["drawdown"] = round(drawdown, 4)
        
        # Check for halt
        if drawdown > 0.20:
            state["trading_halted"] = True
            
        self.redis.set(self.state_key, json.dumps(state))
        return state
