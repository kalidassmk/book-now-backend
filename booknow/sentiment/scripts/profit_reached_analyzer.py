import redis
import json
import time
import logging
from datetime import datetime

# ==========================================
# 1. LOGGING & CONFIG
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ProfitReachedAnalyzer")

# Redis Keys
WATCH_ALL = "BASE_CURRENT_INC_%"
PROFIT_REACHED_KEY = "PROFIT_REACHED_020"
BUY_AMOUNT_USDT = 12.0
PROFIT_THRESHOLD = 0.20

class ProfitReachedAnalyzer:
    def __init__(self):
        self.r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

    def run(self):
        logger.info(f"🚀 Profit Reached Analyzer started. Threshold: ${PROFIT_THRESHOLD}")
        
        while True:
            try:
                # 1. Iterate over all coins from baseline watch list using scan to avoid blocking
                for symbol, data_json in self.r.hscan_iter(WATCH_ALL):
                    try:
                        data = json.loads(data_json)
                        
                        base_price = float(data.get('basePrice', 0))
                        curr_price = float(data.get('currentPrice', 0))
                        
                        if base_price <= 0:
                            continue

                        # 2. Calculate Profit on a standard $12 buy
                        # Qty = BuyAmount / BasePrice
                        # Profit = (CurrPrice - BasePrice) * Qty
                        profit = (curr_price - base_price) * (BUY_AMOUNT_USDT / base_price)
                        
                        # 3. Check if threshold reached
                        if profit >= PROFIT_THRESHOLD:
                            # Prepare identification record
                            record = {
                                "symbol": symbol,
                                "basePrice": base_price,
                                "currentPrice": curr_price,
                                "profit": round(profit, 4),
                                "reachedAt": datetime.now().isoformat(),
                                "hms": datetime.now().strftime("%H:%M:%S")
                            }
                            
                            # Store in new Redis hash for identification
                            self.r.hset(PROFIT_REACHED_KEY, symbol, json.dumps(record))
                            
                            # Log the discovery
                            logger.info(f"🎯 [PROFIT REACHED] {symbol}: +${profit:.2f} (Base: {base_price} -> Curr: {curr_price})")
                            
                    except Exception as e:
                        logger.error(f"Error processing {symbol}: {e}")

                # Gentle sleep to respect system resources
                time.sleep(2)

            except Exception as e:
                logger.error(f"Loop error: {e}")
                time.sleep(5)

if __name__ == "__main__":
    analyzer = ProfitReachedAnalyzer()
    analyzer.run()
