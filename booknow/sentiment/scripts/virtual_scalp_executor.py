import os
import redis
import json
import time
import uuid
from datetime import datetime

# --- CONFIGURATION ---
# Read from env so Docker can point at the `redis` service while local
# dev still defaults to 127.0.0.1. Compose sets REDIS_HOST=redis.
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
ANALYSIS_020_KEY = 'ANALYSIS_020_TIMELINE'
VIRTUAL_POSITIONS_KEY = 'VIRTUAL_POSITIONS:MICRO'
VIRTUAL_HISTORY_KEY = 'VIRTUAL_HISTORY:MICRO'
TRADING_CONFIG_KEY = 'TRADING_CONFIG'

# Fallbacks if Redis trading config is missing/corrupt; kept aligned with
# booknow.config.trading_config.TradingConfig defaults.
DEFAULT_BUY_AMOUNT_USDT = 100.0
DEFAULT_PROFIT_TARGET_USDT = 0.20

# Risk Settings for Virtual Scalper
STOP_LOSS = 0.003   # 0.3%
MIN_HOLD_SECONDS = 3600 # 1 Hour "Patience" Rule
MAX_VIRTUAL_LOSS_USDT = 1.0 # Allow up to $1 loss during patience period

class VirtualScalpExecutor:
    def __init__(self):
        self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        print("🚀 [VIRTUAL SCALPER] Patience Mode Active (5m Hold Rule).")

    def _load_trading_config(self):
        """Read live TradingConfig from Redis at position-open time.
        Returns (buy_amount_usdt, profit_target_usdt). Falls back to defaults on any failure."""
        try:
            raw = self.r.get(TRADING_CONFIG_KEY)
            if raw:
                cfg = json.loads(raw)
                return (
                    float(cfg.get("buyAmountUsdt", DEFAULT_BUY_AMOUNT_USDT)),
                    float(cfg.get("profitAmountUsdt", DEFAULT_PROFIT_TARGET_USDT)),
                )
        except Exception:
            pass
        return (DEFAULT_BUY_AMOUNT_USDT, DEFAULT_PROFIT_TARGET_USDT)

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
        # Snapshot the live trading config at entry so this position uses a
        # stable investment + TP target even if the dashboard mutates them mid-trade.
        investment, profit_target_usdt = self._load_trading_config()
        # Look up the symbol's baseline (set by profit_reached_analyzer when
        # the coin first crossed +$0.20 from base) so the dashboard can show
        # the operator how far the entry price is above base.
        base_price = self._fetch_base_price(symbol)
        pos = {
            "id": str(uuid.uuid4())[:8],
            "symbol": symbol,
            "entry_price": price,
            "base_price": base_price,
            "entry_timestamp": time.time(),
            "entry_time": datetime.now().strftime('%H:%M:%S'),
            "max_drawdown": 0,
            "status": "OPEN",
            "investment": investment,
            "profit_target_usdt": profit_target_usdt,
            "quantity": investment / price,
        }
        self.r.hset(VIRTUAL_POSITIONS_KEY, symbol, json.dumps(pos))
        print(f"💰 [VIRTUAL BUY] {symbol} at {price} | base={base_price} | inv=${investment:.2f} | TP=${profit_target_usdt:.2f} (min hold 5m)")

    def _fetch_base_price(self, symbol):
        """Return the symbol's stored base price, or None if not available."""
        try:
            raw = self.r.hget("PROFIT_REACHED_020", symbol)
            if raw:
                data = json.loads(raw)
                bp = data.get("basePrice")
                if bp is not None:
                    return float(bp)
        except Exception:
            pass
        return None

    def monitor_positions(self, symbol, curr_price, signal, curr_vol):
        pos_raw = self.r.hget(VIRTUAL_POSITIONS_KEY, symbol)
        if not pos_raw: return

        pos = json.loads(pos_raw)
        entry_price = pos['entry_price']
        elapsed = time.time() - pos['entry_timestamp']
        investment = pos.get('investment', DEFAULT_BUY_AMOUNT_USDT)
        profit_target_usdt = pos.get('profit_target_usdt', DEFAULT_PROFIT_TARGET_USDT)
        pnl_pct = (curr_price - entry_price) / entry_price
        pnl_usdt = investment * pnl_pct

        # Track Max Drawdown for learning
        if pnl_pct < pos['max_drawdown']:
            pos['max_drawdown'] = pnl_pct
            self.r.hset(VIRTUAL_POSITIONS_KEY, symbol, json.dumps(pos))

        exit_reason = None

        # 1. Take Profit (Instant - $-target hit, don't wait 5m)
        if pnl_usdt >= profit_target_usdt:
            exit_reason = "TAKE_PROFIT"

        # 2. Hard Stop Loss (Instant if loss > $1)
        elif pnl_usdt <= -MAX_VIRTUAL_LOSS_USDT:
            exit_reason = "HARD_STOP_LOSS"

        # 3. Strategy Exit (Instant if exhaustion detected)
        elif signal == "EXHAUSTION_EXIT":
            exit_reason = "STRATEGY_EXIT"

        # 4. Soft Stop Loss (Patience Logic: Wait 5m if loss < $1)
        elif pnl_pct <= -STOP_LOSS:
            if elapsed < MIN_HOLD_SECONDS:
                pass # Patiently holding for recovery...
            else:
                exit_reason = "SOFT_STOP_LOSS"

        if exit_reason:
            self.close_virtual_position(symbol, pos, curr_price, pnl_pct, exit_reason, curr_vol)

    def close_virtual_position(self, symbol, pos, exit_price, pnl_pct, reason, exit_vol):
        # Investment is the per-position snapshot taken at open (from TradingConfig).
        investment = pos.get('investment', DEFAULT_BUY_AMOUNT_USDT)
        # Calculate Real-World Fees (0.1% per side)
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
