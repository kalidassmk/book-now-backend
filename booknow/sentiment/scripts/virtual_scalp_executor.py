import redis
import json
import time
import uuid
from datetime import datetime

# --- CONFIGURATION ---
REDIS_HOST = '127.0.0.1'
REDIS_PORT = 6379
ANALYSIS_020_KEY = 'ANALYSIS_020_TIMELINE'
VIRTUAL_POSITIONS_KEY = 'VIRTUAL_POSITIONS:MICRO'
VIRTUAL_HISTORY_KEY = 'VIRTUAL_HISTORY:MICRO'

# Risk Settings for Virtual Scalper
TAKE_PROFIT = 0.005 # 0.5%
STOP_LOSS = 0.003   # 0.3%
MIN_HOLD_SECONDS = 180 # 3 Minutes "Patience" Rule
MAX_VIRTUAL_LOSS_USDT = 1.0 # Allow up to $1 loss during patience period

class VirtualScalpExecutor:
    def __init__(self):
        self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        print("🚀 [VIRTUAL SCALPER] Patience Mode Active (3m Hold Rule).")

    def process_signals(self):
        last_heartbeat = 0
        while True:
            try:
                now = time.time()
                all_analysis = self.r.hgetall(ANALYSIS_020_KEY)
                
                if now - last_heartbeat > 30:
                    active_pos_count = self.r.hlen(VIRTUAL_POSITIONS_KEY)
                    print(f"💓 [HEARTBEAT] Monitoring {len(all_analysis)} coins | Active Virtual Positions: {active_pos_count}")
                    last_heartbeat = now

                for symbol, data_json in all_analysis.items():
                    timeline = json.loads(data_json)
                    if not timeline: continue
                    
                    last = timeline[-1]
                    signal = last.get('micro_signal', 'NEUTRAL')
                    curr_price = last.get('price', 0)
                    curr_vol = last.get('volume', 0)
                    
                    if signal == 'SCALP_BUY_SIGNAL':
                        if not self.r.hexists(VIRTUAL_POSITIONS_KEY, symbol):
                            self.open_virtual_position(symbol, curr_price)
                    
                    self.monitor_positions(symbol, curr_price, signal, curr_vol)

                time.sleep(1)
            except Exception as e:
                print(f"❌ [VIRTUAL SCALPER ERROR] {e}")
                time.sleep(5)

    def open_virtual_position(self, symbol, price):
        pos = {
            "id": str(uuid.uuid4())[:8],
            "symbol": symbol,
            "entry_price": price,
            "entry_timestamp": time.time(),
            "entry_time": datetime.now().strftime('%H:%M:%S'),
            "max_drawdown": 0,
            "status": "OPEN",
            "quantity": 100 / price
        }
        self.r.hset(VIRTUAL_POSITIONS_KEY, symbol, json.dumps(pos))
        print(f"💰 [VIRTUAL BUY] {symbol} at {price} (Holding for min 3m)")

    def monitor_positions(self, symbol, curr_price, signal, curr_vol):
        pos_raw = self.r.hget(VIRTUAL_POSITIONS_KEY, symbol)
        if not pos_raw: return
        
        pos = json.loads(pos_raw)
        entry_price = pos['entry_price']
        elapsed = time.time() - pos['entry_timestamp']
        pnl_pct = (curr_price - entry_price) / entry_price
        pnl_usdt = 100 * pnl_pct
        
        # Track Max Drawdown for learning
        if pnl_pct < pos['max_drawdown']:
            pos['max_drawdown'] = pnl_pct
            self.r.hset(VIRTUAL_POSITIONS_KEY, symbol, json.dumps(pos))

        exit_reason = None
        
        # 1. Take Profit (Instant - Don't wait 3m)
        if pnl_pct >= TAKE_PROFIT:
            exit_reason = "TAKE_PROFIT"
        
        # 2. Hard Stop Loss (Instant if loss > $1)
        elif pnl_usdt <= -MAX_VIRTUAL_LOSS_USDT:
            exit_reason = "HARD_STOP_LOSS"

        # 3. Strategy Exit (Instant if exhaustion detected)
        elif signal == "EXHAUSTION_EXIT":
            exit_reason = "STRATEGY_EXIT"
            
        # 4. Soft Stop Loss (Patience Logic: Wait 3m if loss < $1)
        elif pnl_pct <= -STOP_LOSS:
            if elapsed < MIN_HOLD_SECONDS:
                pass # Patiently holding for recovery...
            else:
                exit_reason = "SOFT_STOP_LOSS"
        
        if exit_reason:
            self.close_virtual_position(symbol, pos, curr_price, pnl_pct, exit_reason, curr_vol)

    def close_virtual_position(self, symbol, pos, exit_price, pnl_pct, reason, exit_vol):
        # Calculate Real-World Fees (0.1% per side)
        investment = 100.0 # Updated to User Config
        buy_fee = investment * 0.001
        sell_value = investment * (1 + pnl_pct)
        sell_fee = sell_value * 0.001
        total_fees = buy_fee + sell_fee
        
        net_pnl_usdt = (sell_value - investment) - total_fees
        
        pos['exit_price'] = exit_price
        pos['exit_time'] = datetime.now().strftime('%H:%M:%S')
        pos['exit_vol'] = exit_vol
        pos['pnl_pct'] = pnl_pct * 100
        pos['pnl_usdt'] = net_pnl_usdt # Now showing NET after fees
        pos['fees_paid'] = total_fees
        pos['reason'] = reason
        pos['status'] = "CLOSED"
        pos['hold_duration'] = time.time() - pos['entry_timestamp']
        
        self.r.lpush(VIRTUAL_HISTORY_KEY, json.dumps(pos))
        self.r.ltrim(VIRTUAL_HISTORY_KEY, 0, 99)
        self.r.hdel(VIRTUAL_POSITIONS_KEY, symbol)
        
        color = "🟢" if net_pnl_usdt > 0 else "🔴"
        print(f"{color} [VIRTUAL SELL] {symbol} | Net PnL: ${net_pnl_usdt:.4f} | Fees: ${total_fees:.4f} | Held: {int(pos['hold_duration'])}s")

if __name__ == "__main__":
    executor = VirtualScalpExecutor()
    executor.process_signals()
