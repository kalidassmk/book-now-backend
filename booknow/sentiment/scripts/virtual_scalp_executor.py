import os
import redis
import json
import time
import uuid
from datetime import datetime

from booknow.util.trade_archive import archive_closed_trade

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
DEFAULT_PROFIT_TARGET_USDT = 0.20    # legacy USDT target — only used when profitPct == 0
DEFAULT_PROFIT_PCT = 0.50            # % above entry; matches the user's scalping formula
DEFAULT_LIMIT_OFFSET_PCT = 0.50      # % below signal price; from same formula

# Risk Settings for Virtual Scalper
SOFT_STOP_LOSS_USDT = 0.50 # Soft stop kicks in at $0.50 loss; respects MIN_HOLD_SECONDS patience
MIN_HOLD_SECONDS = 3600    # 1 Hour "Patience" Rule
MAX_VIRTUAL_LOSS_USDT = 1.0 # Hard stop loss; immediate exit at $1.00 loss

# Limit-buy lifecycle: when SCALP_BUY_SIGNAL fires we don't take the
# market price. Instead we "place" a paper limit at limitBuyOffsetPct
# below signal and wait for price to come down. Order sits in
# VIRTUAL_POSITIONS_KEY with status=PENDING_LIMIT until either filled
# (current price <= limit price) or expired (timeout).
LIMIT_ORDER_TIMEOUT_SECONDS = 60

class VirtualScalpExecutor:
    def __init__(self):
        self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        print("🚀 [VIRTUAL SCALPER] Limit-buy + %-TP mode (1h patience).")

    def _load_trading_config(self):
        """Read live TradingConfig from Redis at position-open time.
        Returns (buy_amount_usdt, profit_target_usdt, profit_pct,
        limit_offset_pct). Falls back to defaults on any failure."""
        try:
            raw = self.r.get(TRADING_CONFIG_KEY)
            if raw:
                cfg = json.loads(raw)
                return (
                    float(cfg.get("buyAmountUsdt", DEFAULT_BUY_AMOUNT_USDT)),
                    float(cfg.get("profitAmountUsdt", DEFAULT_PROFIT_TARGET_USDT)),
                    float(cfg.get("profitPct", DEFAULT_PROFIT_PCT)),
                    float(cfg.get("limitBuyOffsetPct", DEFAULT_LIMIT_OFFSET_PCT)),
                )
        except Exception:
            pass
        return (DEFAULT_BUY_AMOUNT_USDT, DEFAULT_PROFIT_TARGET_USDT,
                DEFAULT_PROFIT_PCT, DEFAULT_LIMIT_OFFSET_PCT)

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

                # Sweep PENDING_LIMIT orders first so a fill from this
                # round is reflected before the per-symbol monitor tick.
                # Decoupled from the analysis loop so orders on coins
                # that drop out still get cleaned up.
                self._sweep_pending_orders()

                for symbol, data_json in all_analysis.items():
                    timeline = json.loads(data_json)
                    if not timeline: continue

                    last = timeline[-1]
                    signal = last.get('micro_signal', 'NEUTRAL')
                    curr_price = last.get('price', 0)
                    curr_vol = last.get('volume', 0)

                    if signal == 'SCALP_BUY_SIGNAL':
                        # Skip if there's already any state (PENDING_LIMIT
                        # or OPEN) for this symbol — prevents duplicate
                        # orders on hot signals.
                        if not self.r.hexists(VIRTUAL_POSITIONS_KEY, symbol):
                            self.place_limit_order(symbol, curr_price)

                    self.monitor_positions(symbol, curr_price, signal, curr_vol)

                time.sleep(1)
            except Exception as e:
                print(f"❌ [VIRTUAL SCALPER ERROR] {e}")
                time.sleep(5)

    def place_limit_order(self, symbol, signal_price):
        """Stage a paper limit order at limitBuyOffsetPct below signal
        price. The order sits in VIRTUAL_POSITIONS_KEY with status
        PENDING_LIMIT until _sweep_pending_orders fills or expires it."""
        investment, profit_target_usdt, profit_pct, limit_offset_pct = self._load_trading_config()
        base_price = self._fetch_base_price(symbol)
        limit_price = signal_price * (1 - limit_offset_pct / 100.0)
        now_ts = time.time()

        pos = {
            "id": str(uuid.uuid4())[:8],
            "symbol": symbol,
            "status": "PENDING_LIMIT",
            "signal_price": signal_price,
            "signal_timestamp": now_ts,
            "signal_time": datetime.now().strftime('%H:%M:%S'),
            "limit_price": limit_price,
            "limit_offset_pct": limit_offset_pct,
            "base_price": base_price,
            "investment": investment,
            "profit_target_usdt": profit_target_usdt,
            "profit_pct": profit_pct,
            # entry_* fields populated by _fill_pending_limit on fill.
            "entry_price": None,
            "entry_timestamp": None,
            "entry_time": None,
            "quantity": None,
            "max_drawdown": 0,
        }
        self.r.hset(VIRTUAL_POSITIONS_KEY, symbol, json.dumps(pos))
        print(f"📍 [LIMIT PLACED] {symbol} signal={signal_price} limit={limit_price:.6f} (-{limit_offset_pct}%) inv=${investment:.0f} TP=+{profit_pct}%")

    def _sweep_pending_orders(self):
        """Walk every PENDING_LIMIT order. Fill if price has reached the
        limit, cancel if 60s timeout has elapsed."""
        all_pos = self.r.hgetall(VIRTUAL_POSITIONS_KEY)
        now_ts = time.time()
        for symbol, raw in all_pos.items():
            try:
                pos = json.loads(raw)
            except Exception:
                continue
            if pos.get('status') != 'PENDING_LIMIT':
                continue

            elapsed = now_ts - pos.get('signal_timestamp', now_ts)

            if elapsed > LIMIT_ORDER_TIMEOUT_SECONDS:
                self.r.hdel(VIRTUAL_POSITIONS_KEY, symbol)
                print(f"⏰ [LIMIT EXPIRED] {symbol} — no fill in {int(elapsed)}s, cancelling")
                continue

            # Read current price from the analyzer's latest snapshot —
            # same source of truth as monitor_positions, so fill check
            # and exit check stay in sync.
            curr_price = self._current_price(symbol)
            if curr_price is None:
                continue

            if curr_price <= pos['limit_price']:
                self._fill_pending_limit(symbol, pos, curr_price, elapsed)

    def _current_price(self, symbol):
        """Best-effort current price from the analyzer's timeline."""
        try:
            raw = self.r.hget(ANALYSIS_020_KEY, symbol)
            if not raw:
                return None
            tl = json.loads(raw)
            if not tl:
                return None
            return tl[-1].get('price')
        except Exception:
            return None

    def _fill_pending_limit(self, symbol, pos, observed_price, elapsed):
        """Convert a PENDING_LIMIT into an OPEN position. Fill at the
        original limit price (conservative — real exchange fills the
        resting order at L when the market touches L)."""
        fill_price = pos['limit_price']
        now_ts = time.time()

        pos['status'] = 'OPEN'
        pos['entry_price'] = fill_price
        pos['entry_timestamp'] = now_ts
        pos['entry_time'] = datetime.now().strftime('%H:%M:%S')
        pos['quantity'] = pos['investment'] / fill_price
        pos['max_drawdown'] = 0  # restart tracking from fill, not from order placement

        self.r.hset(VIRTUAL_POSITIONS_KEY, symbol, json.dumps(pos))
        signal_price = pos.get('signal_price', fill_price)
        improvement = (signal_price - fill_price) / signal_price * 100 if signal_price else 0
        print(f"✅ [LIMIT FILLED] {symbol} fill={fill_price:.6f} (-{improvement:.2f}% vs signal {signal_price:.6f}) after {elapsed:.0f}s | observed={observed_price:.6f}")

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

        # PENDING_LIMIT positions are handled by _sweep_pending_orders;
        # their entry_price/timestamp are None until fill.
        if pos.get('status') == 'PENDING_LIMIT':
            return

        entry_price = pos['entry_price']
        elapsed = time.time() - pos['entry_timestamp']
        investment = pos.get('investment', DEFAULT_BUY_AMOUNT_USDT)
        profit_target_usdt = pos.get('profit_target_usdt', DEFAULT_PROFIT_TARGET_USDT)
        # profit_pct snapshot (saved at order placement so dashboard
        # mutations don't move the goalpost on a live trade).
        profit_pct = pos.get('profit_pct', 0)
        pnl_pct = (curr_price - entry_price) / entry_price
        pnl_usdt = investment * pnl_pct

        # Track Max Drawdown for learning
        if pnl_pct < pos['max_drawdown']:
            pos['max_drawdown'] = pnl_pct
            self.r.hset(VIRTUAL_POSITIONS_KEY, symbol, json.dumps(pos))

        exit_reason = None

        # 1. Take Profit — percentage target wins when profit_pct > 0;
        # otherwise fall back to the legacy USDT target (matches the
        # Java convention of "profitAmountUsdt overrides profitPct").
        tp_hit = (
            (pnl_pct >= profit_pct / 100.0) if profit_pct > 0
            else (pnl_usdt >= profit_target_usdt)
        )
        if tp_hit:
            exit_reason = "TAKE_PROFIT"

        # 2. Hard Stop Loss (Instant if loss > $1)
        elif pnl_usdt <= -MAX_VIRTUAL_LOSS_USDT:
            exit_reason = "HARD_STOP_LOSS"

        # 3. Strategy Exit (Instant if exhaustion detected)
        elif signal == "EXHAUSTION_EXIT":
            exit_reason = "STRATEGY_EXIT"

        # 4. Soft Stop Loss (Patience Logic: Wait 1h if loss between $0.50 and $1)
        elif pnl_usdt <= -SOFT_STOP_LOSS_USDT:
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

        # Long-term archive on the analyse Redis (best-effort, won't
        # block on a bad connection).
        archive_closed_trade(symbol, "VIRTUAL", pos)
        
        color = "🟢" if net_pnl_usdt > 0 else "🔴"
        print(f"{color} [VIRTUAL SELL] {symbol} | Net PnL: ${net_pnl_usdt:.4f} | Fees: ${total_fees:.4f} | Held: {int(pos['hold_duration'])}s")

if __name__ == "__main__":
    executor = VirtualScalpExecutor()
    executor.process_signals()
