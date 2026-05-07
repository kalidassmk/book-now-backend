import os
import redis
import json
import time
import uuid
from datetime import datetime

# --- CONFIGURATION ---
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
ANALYSIS_020_KEY = 'ANALYSIS_020_TIMELINE'
VIRTUAL_POSITIONS_KEY = 'VIRTUAL_POSITIONS:MICRO'
VIRTUAL_HISTORY_KEY = 'VIRTUAL_HISTORY:MICRO'
TRADING_CONFIG_KEY = 'TRADING_CONFIG'
BTC_REGIME_KEY = 'BTC_REGIME'

# Fallbacks if Redis trading config is missing/corrupt; kept aligned with
# booknow.config.trading_config.TradingConfig defaults.
DEFAULT_BUY_AMOUNT_USDT = 100.0
DEFAULT_PROFIT_TARGET_USDT = 0.50

# Risk / sizing constants. Comments are dollarised against a $100 position
# so the geometry is obvious — change the constants, not the comments,
# when the position size moves.
#
# Why these numbers (the 2026-05-07 redesign):
#   Old strategy: TP $0.20 gross, SL 0.3% with a 5-min "patience" window
#   that let losers drift to almost the $1 hard cap. Round-trip fees on
#   $100 are $0.20, so a "perfect" TP netted $0 and only overshoots paid.
#   Net result over 32 live paper trades: 50% win rate, profit factor
#   0.21, avg loss 4.7× avg win — structurally negative-EV.
#
# New geometry:
#   * Fee-aware TP: trigger at gross >= target + fees so TP nets the
#     target after fees rather than before.
#   * Strict 0.2% SL (no patience). Losses cap near 1× round-trip fee.
#   * Breakeven stop arms once a position prints +$0.10 net (the
#     "watermark"); after that, any pullback that erases ~all profit
#     exits at breakeven instead of riding back into a stop-out.
#   * Flat-trade timeout: a position that's drifted ±$0.05 around entry
#     for 5 min gets cut so the capital can rotate.
STOP_LOSS_PCT = 0.002              # 0.2% — strict, no patience window
MAX_VIRTUAL_LOSS_USDT = 1.0        # absolute hard cap (catastrophic)
FEE_RATE = 0.001                   # 0.1% per side (Binance spot taker)
BREAKEVEN_ARM_USDT = 0.10          # net profit watermark that arms BE stop
BREAKEVEN_TRIGGER_USDT = 0.0       # exit if net pnl falls back to 0 after armed
FLAT_EXIT_AFTER_SECONDS = 7200     # 2 h — was 5 min; bumped after the
                                   # TP target moved 0.20 → 0.50, since
                                   # a 0.7% move can take much longer
                                   # than 5 min to play out.
FLAT_EXIT_BAND_USDT = 0.05         # |net pnl| <= $0.05 counts as "flat"


def _round_trip_fee_estimate(investment, pnl_pct):
    """Closed-form round-trip fee for a position closing at pnl_pct.

    Same formula the close path uses (FEE_RATE * investment for buy +
    FEE_RATE * sell_value for sell). Exposed so the entry-side TP gate
    can target a NET profit instead of a gross one."""
    sell_value = investment * (1 + pnl_pct)
    return investment * FEE_RATE + sell_value * FEE_RATE


class VirtualScalpExecutor:
    def __init__(self):
        self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        print("🚀 [VIRTUAL SCALPER] Asymmetric R:R mode (TP=$0.40 net, SL=0.2% strict, BE-stop, flat-cut).")

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

    def _btc_regime_blocking(self):
        """Returns True if the BTC regime filter says new buys should be skipped.

        The analyzer writes BTC_REGIME = {pct_5m, ts, blocking} every
        ~5s. We treat missing or stale (>60s) data as non-blocking so a
        crashed analyzer doesn't paralyse the executor."""
        try:
            raw = self.r.get(BTC_REGIME_KEY)
            if not raw:
                return False
            data = json.loads(raw)
            if time.time() - data.get('ts', 0) > 60:
                return False
            return bool(data.get('blocking', False))
        except Exception:
            return False

    def process_signals(self):
        last_heartbeat = 0
        while True:
            try:
                now = time.time()
                all_analysis = self.r.hgetall(ANALYSIS_020_KEY)

                if now - last_heartbeat > 30:
                    active_pos_count = self.r.hlen(VIRTUAL_POSITIONS_KEY)
                    btc_blocked = self._btc_regime_blocking()
                    print(f"💓 [HEARTBEAT] Monitoring {len(all_analysis)} coins | Active Virtual Positions: {active_pos_count} | BTC blocking buys: {btc_blocked}")
                    last_heartbeat = now

                btc_blocked = self._btc_regime_blocking()

                for symbol, data_json in all_analysis.items():
                    try:
                        timeline = json.loads(data_json)
                    except Exception:
                        continue
                    if not timeline:
                        continue

                    last = timeline[-1]
                    signal = last.get('micro_signal', 'NEUTRAL')
                    curr_price = last.get('price', 0)
                    curr_vol = last.get('volume', 0)

                    if signal == 'SCALP_BUY_SIGNAL' and not btc_blocked:
                        if not self.r.hexists(VIRTUAL_POSITIONS_KEY, symbol):
                            self.open_virtual_position(symbol, curr_price, last)

                    self.monitor_positions(symbol, curr_price, signal, curr_vol)

                time.sleep(1)
            except Exception as e:
                print(f"❌ [VIRTUAL SCALPER ERROR] {e}")
                time.sleep(5)

    def open_virtual_position(self, symbol, price, snapshot):
        """Open a paper position. `snapshot` is the latest timeline entry from the
        analyzer — we copy a few fields into entry_context so post-hoc analysis
        can correlate winners/losers with entry conditions."""
        investment, profit_target_usdt = self._load_trading_config()
        base_price = self._fetch_base_price(symbol)

        # Snapshot the entry conditions the analyzer saw. This is what
        # backtests/learning loops will key off when we tune the entry
        # signal — without it we're flying blind. Keep small and JSON-safe.
        seq = snapshot.get('sequence_report', {}) or {}
        entry_context = {
            "daily_position": snapshot.get('daily_position'),
            "is_overheated": snapshot.get('is_overheated'),
            "vol_change_pct": seq.get('vol_change'),
            "price_impact_pct": seq.get('price_impact'),
            "price_trend_up": snapshot.get('price_trend_up'),
            "btc_pct_5m": snapshot.get('btc_pct_5m'),
            "confidence": snapshot.get('prediction_confidence'),
            # Falling-knife filter telemetry — these are the values the
            # analyzer's A1/A2/A4/A5 gates evaluated. If a future loss
            # has e.g. above_low_pct just above the 2% threshold, that
            # tells us the keystone filter needs tightening.
            "daily_change_pct": snapshot.get('daily_change_pct'),
            "from_high_pct":    snapshot.get('from_high_pct'),
            "above_low_pct":    snapshot.get('above_low_pct'),
            "sustained_drift_pct": snapshot.get('sustained_drift_pct'),
            "sustained_up":     snapshot.get('sustained_up'),
        }

        pos = {
            "id": str(uuid.uuid4())[:8],
            "symbol": symbol,
            "entry_price": price,
            "base_price": base_price,
            "entry_timestamp": time.time(),
            "entry_time": datetime.now().strftime('%H:%M:%S'),
            "max_drawdown": 0,
            "max_favorable_usdt": 0.0,   # net-pnl watermark for breakeven-stop logic
            "breakeven_armed": False,
            "status": "OPEN",
            "investment": investment,
            "profit_target_usdt": profit_target_usdt,
            "quantity": investment / price,
            "entry_context": entry_context,
        }
        self.r.hset(VIRTUAL_POSITIONS_KEY, symbol, json.dumps(pos))
        print(f"💰 [VIRTUAL BUY] {symbol} at {price} | base={base_price} | inv=${investment:.2f} | TP_net=${profit_target_usdt:.2f} | ctx={entry_context}")

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
        if not pos_raw:
            return
        try:
            pos = json.loads(pos_raw)
        except Exception:
            # Corrupt entry — drop it so it can't poison this loop forever.
            self.r.hdel(VIRTUAL_POSITIONS_KEY, symbol)
            return

        entry_price = pos['entry_price']
        elapsed = time.time() - pos['entry_timestamp']
        investment = pos.get('investment', DEFAULT_BUY_AMOUNT_USDT)
        profit_target_usdt = pos.get('profit_target_usdt', DEFAULT_PROFIT_TARGET_USDT)
        pnl_pct = (curr_price - entry_price) / entry_price
        gross_pnl_usdt = investment * pnl_pct

        # NET pnl is what the user actually keeps after fees — every exit
        # decision below is in net-USDT space so the geometry matches the
        # P&L the dashboard shows.
        net_pnl_usdt = gross_pnl_usdt - _round_trip_fee_estimate(investment, pnl_pct)

        dirty = False

        # Track Max Drawdown for learning (gross % is fine — we already
        # dollarise above for exit decisions).
        if pnl_pct < pos['max_drawdown']:
            pos['max_drawdown'] = pnl_pct
            dirty = True

        # Track watermark in NET-USDT — this is what arms the breakeven stop.
        if net_pnl_usdt > pos.get('max_favorable_usdt', 0.0):
            pos['max_favorable_usdt'] = net_pnl_usdt
            if net_pnl_usdt >= BREAKEVEN_ARM_USDT and not pos.get('breakeven_armed'):
                pos['breakeven_armed'] = True
            dirty = True

        if dirty:
            self.r.hset(VIRTUAL_POSITIONS_KEY, symbol, json.dumps(pos))

        exit_reason = None

        # Exit decisions — order matters. Most desirable outcomes first
        # (TP), then catastrophic protections (hard SL), then signal-based
        # exits, then BE-stop, then strict SL, then flat-cut.

        # 1. Take Profit — gross has to clear target + fees so the user
        # banks the *net* target rather than $0 after fees.
        tp_gross_target = profit_target_usdt + _round_trip_fee_estimate(investment, profit_target_usdt / investment)
        if gross_pnl_usdt >= tp_gross_target:
            exit_reason = "TAKE_PROFIT"

        # 2. Hard Stop Loss — catastrophic move; cuts before 0.2% if a
        # gap is large enough to skip past the soft SL in one tick.
        elif net_pnl_usdt <= -MAX_VIRTUAL_LOSS_USDT:
            exit_reason = "HARD_STOP_LOSS"

        # 3. Strategy Exit — analyzer detected exhaustion (volume rolling over).
        elif signal == "EXHAUSTION_EXIT":
            exit_reason = "STRATEGY_EXIT"

        # 4. Breakeven Stop — once we've banked some open profit, don't
        # let it roll back to a full stop-out. Exit on the first tick
        # that drops back to (or below) breakeven after arming.
        elif pos.get('breakeven_armed') and net_pnl_usdt <= BREAKEVEN_TRIGGER_USDT:
            exit_reason = "BREAKEVEN_STOP"

        # 5. Strict Stop Loss — no patience window. Loss is capped near
        # 1× round-trip fee so the avg loss can't explode into 4-5×
        # avg win like it did under the old patience logic.
        elif pnl_pct <= -STOP_LOSS_PCT:
            exit_reason = "SOFT_STOP_LOSS"

        # 6. Flat-trade timeout — a position drifting around entry for
        # 5 minutes is dead capital. Cut it so the engine can deploy
        # the slot to a fresh signal.
        elif elapsed >= FLAT_EXIT_AFTER_SECONDS and abs(net_pnl_usdt) <= FLAT_EXIT_BAND_USDT:
            exit_reason = "FLAT_TIMEOUT"

        if exit_reason:
            self.close_virtual_position(symbol, pos, curr_price, pnl_pct, exit_reason, curr_vol)

    def close_virtual_position(self, symbol, pos, exit_price, pnl_pct, reason, exit_vol):
        investment = pos.get('investment', DEFAULT_BUY_AMOUNT_USDT)
        # Real-world fees (0.1% per side). Mirrors _round_trip_fee_estimate
        # but kept inline here so the closed/persisted record carries the
        # exact paid amount.
        buy_fee = investment * FEE_RATE
        sell_value = investment * (1 + pnl_pct)
        sell_fee = sell_value * FEE_RATE
        total_fees = buy_fee + sell_fee

        net_pnl_usdt = (sell_value - investment) - total_fees

        pos['exit_price'] = exit_price
        pos['exit_time'] = datetime.now().strftime('%H:%M:%S')
        pos['exit_vol'] = exit_vol
        pos['pnl_pct'] = pnl_pct * 100
        pos['pnl_usdt'] = net_pnl_usdt
        pos['fees_paid'] = total_fees
        pos['reason'] = reason
        pos['status'] = "CLOSED"
        pos['hold_duration'] = time.time() - pos['entry_timestamp']

        self.r.lpush(VIRTUAL_HISTORY_KEY, json.dumps(pos))
        self.r.ltrim(VIRTUAL_HISTORY_KEY, 0, 99)
        self.r.hdel(VIRTUAL_POSITIONS_KEY, symbol)

        color = "🟢" if net_pnl_usdt > 0 else "🔴"
        print(f"{color} [VIRTUAL SELL] {symbol} ({reason}) | Net: ${net_pnl_usdt:+.4f} | Fees: ${total_fees:.4f} | MFE: ${pos.get('max_favorable_usdt', 0):.3f} | Held: {int(pos['hold_duration'])}s")


if __name__ == "__main__":
    executor = VirtualScalpExecutor()
    executor.process_signals()
