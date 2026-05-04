import redis
import json
import time

# Binance default fee (0.1%)
FEE_RATE = 0.001 

class FeeIntelligenceUtil:
    """
    Calculates precise trade targets factoring in Binance execution fees.
    """
    def __init__(self):
        self.r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    def calculate_net_targets(self, investment_usdt=12.0, target_net_profit=0.20):
        # 1. Buy Phase
        buy_fee = investment_usdt * FEE_RATE
        actual_investment = investment_usdt - buy_fee
        
        # 2. To get target_net_profit, we need to cover the buy fee AND the upcoming sell fee
        # Let GP = Gross Profit. Net = GP - BuyFee - SellFee
        # Net = GP - BuyFee - (Investment + GP)*FeeRate
        # GP * (1 - FeeRate) = Net + BuyFee + Investment*FeeRate
        
        required_gross_profit = (target_net_profit + buy_fee + (investment_usdt * FEE_RATE)) / (1 - FEE_RATE)
        
        total_fees = buy_fee + (investment_usdt + required_gross_profit) * FEE_RATE
        break_even_profit = total_fees # Profit needed just to pay fees
        
        return {
            "investment": investment_usdt,
            "target_net": target_net_profit,
            "buy_fee": round(buy_fee, 4),
            "estimated_sell_fee": round((investment_usdt + required_gross_profit) * FEE_RATE, 4),
            "total_fees": round(total_fees, 4),
            "required_gross_profit": round(required_gross_profit, 4),
            "required_price_move_pct": round((required_gross_profit / investment_usdt) * 100, 3)
        }

    def run_and_store(self):
        # Store a lookup table in Redis for the UI.
        # Includes the active trading config (12 USDT / +$0.20) plus a few
        # alternative budgets so the dashboard can show what fees look like
        # for larger plans.
        configs = [
            {"inv": 12,  "target": 0.20},   # actual fast-scalp config
            {"inv": 100, "target": 0.05},
            {"inv": 100, "target": 0.10},
            {"inv": 100, "target": 0.20},
            {"inv": 100, "target": 0.50},
        ]

        # Format the target with two decimals so keys are stable across
        # values like 0.20 vs 0.2 (Python's default float repr drops the
        # trailing zero, which would break dictionary lookups).
        results = {}
        for c in configs:
            key = f"fee_plan_{c['inv']}_{c['target']:.2f}"
            results[key] = self.calculate_net_targets(c['inv'], c['target'])

        self.r.set("TRADING_FEE_INTELLIGENCE", json.dumps(results))
        print("✅ Fee Intelligence calculated and stored in Redis.")
        sample_key = f"fee_plan_12_{0.20:.2f}"
        print(f"Sample (12 USDT, 0.20 Profit): {results[sample_key]}")

if __name__ == "__main__":
    util = FeeIntelligenceUtil()
    util.run_and_store()
