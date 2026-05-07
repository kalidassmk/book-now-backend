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
DEFAULT_PROFIT_TARGET_USDT = 0.25
DEFAULT_LIMIT_OFFSET_PCT   = 0.30   # buy this % below the signal price

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

# Limit-buy entries — when SCALP_BUY_SIGNAL fires, we don't take the
# market price. Instead we "place a limit order" (paper) at slightly
# below market and only fill when price comes back to it. Two payoffs:
#   * better entry → smaller move needed for TP
#   * implicit selectivity → fast pumps that never pull back are
#     skipped, which historically were the late-FOMO trades that lost
#
# The pending order is stored in VIRTUAL_POSITIONS_KEY with status
# "PENDING_LIMIT". A sweep each loop fills/expires/cancels it.
LIMIT_ORDER_TIMEOUT_SECONDS = 60   # cancel the limit if no fill in 60s


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
        print("🚀 [VIRTUAL SCALPER] Limit-entry mode (TP=$0.25 net, SL=0.2% strict, limit-buy below market, BE-stop, flat-cut).")

    def _load_trading_config(self):
        """Read live TradingConfig from Redis at position-open time.
        Returns (buy_amount_usdt, profit_target_usdt, limit_offset_pct).
        Falls back to defaults on any failure."""
        try:
            raw = self.r.get(TRADING_CONFIG_KEY)
            if raw:
                cfg = json.loads(raw)
                return (
                    float(cfg.get("buyAmountUsdt", DEFAULT_BUY_AMOUNT_USDT)),
                    float(cfg.get("profitAmountUsdt", DEFAULT_PROFIT_TARGET_USDT)),
                    float(cfg.get("limitBuyOffsetPct", DEFAULT_LIMIT_OFFSET_PCT)),
                )
        except Exception:
            pass
        return (DEFAULT_BUY_AMOUNT_USDT, DEFAULT_PROFIT_TARGET_USDT, DEFAULT_LIMIT_OFFSET_PCT)

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
                btc_blocked = self._btc_regime_blocking()

                if now - last_heartbeat > 30:
                    active_pos_count = self.r.hlen(VIRTUAL_POSITIONS_KEY)
                    print(f"💓 [HEARTBEAT] Monitoring {len(all_analysis)} coins | Active Virtual Positions: {active_pos_count} | BTC blocking buys: {btc_blocked}")
                    last_heartbeat = now

                # Sweep pending limit orders FIRST so a fill from this
                # round shows up before the per-symbol monitor tick.
                # Decoupled from the analysis loop so a coin that drops
                # out of PROFIT_REACHED_020 still has its pending order
                # cleaned up.
                self._sweep_pending_orders(btc_blocked)

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
                        # Skip if there's already any state (PENDING_LIMIT
                        # or OPEN) for this symbol. Prevents duplicate orders
                        # if the signal stays hot for several ticks.
                        if not self.r.hexists(VIRTUAL_POSITIONS_KEY, symbol):
                            self.place_limit_order(symbol, curr_price, last)

                    self.monitor_positions(symbol, curr_price, signal, curr_vol)

                time.sleep(1)
            except Exception as e:
                print(f"❌ [VIRTUAL SCALPER ERROR] {e}")
                time.sleep(5)

    def _build_entry_context(self, snapshot):
        """Pull the analyzer's diagnostic fields into a flat dict that gets
        persisted on every position. Same shape regardless of fill path."""
        seq = snapshot.get('sequence_report', {}) or {}
        return {
            "daily_position": snapshot.get('daily_position'),
            "is_overheated": snapshot.get('is_overheated'),
            "vol_change_pct": seq.get('vol_change'),
            "price_impact_pct": seq.get('price_impact'),
            "price_trend_up": snapshot.get('price_trend_up'),
            "btc_pct_5m": snapshot.get('btc_pct_5m'),
            "confidence": snapshot.get('prediction_confidence'),
            # Falling-knife filter telemetry — A1/A2/A4/A5 gate inputs.
            "daily_change_pct": snapshot.get('daily_change_pct'),
            "from_high_pct":    snapshot.get('from_high_pct'),
            "above_low_pct":    snapshot.get('above_low_pct'),
            "sustained_drift_pct": snapshot.get('sustained_drift_pct'),
            "sustained_up":     snapshot.get('sustained_up'),
        }

    def place_limit_order(self, symbol, signal_price, snapshot):
        """Replace the old market-buy with a paper limit order at offset%
        below signal price. Order sits in VIRTUAL_POSITIONS_KEY with
        status=PENDING_LIMIT until _sweep_pending_orders fills or
        cancels it."""
        investment, profit_target_usdt, limit_offset_pct = self._load_trading_config()
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
            # entry_* fields are filled in by _fill_pending_limit when the
            # order actually fills. Defaults so partial reads don't crash.
            "entry_price": None,
            "entry_timestamp": None,
            "entry_time": None,
            "quantity": None,
            "max_drawdown": 0,
            "max_favorable_usdt": 0.0,
            "breakeven_armed": False,
            "entry_context": self._build_entry_context(snapshot),
        }
        self.r.hset(VIRTUAL_POSITIONS_KEY, symbol, json.dumps(pos))
        print(f"📍 [LIMIT PLACED] {symbol} signal={signal_price} limit={limit_price:.6f} (-{limit_offset_pct}%) inv=${investment:.0f} TP_net=${profit_target_usdt:.2f}")

    def _sweep_pending_orders(self, btc_blocked):
        """Walk every PENDING_LIMIT order. Fill if price has reached the
        limit, cancel if expired or if the BTC regime has flipped to
        blocking since the order was placed."""
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
            if btc_blocked:
                # Original entry thesis assumed BTC was OK. If BTC has
                # since started dumping, the bullish setup that produced
                # the signal is no longer valid — drop the order rather
                # than fill into a coordinated dump.
                self.r.hdel(VIRTUAL_POSITIONS_KEY, symbol)
                print(f"🛑 [LIMIT CANCELLED] {symbol} — BTC regime turned blocking after {int(elapsed)}s")
                continue

            # Get current price for fill check. Use the tickers cache when
            # available; fall back to the most recent analysis snapshot
            # otherwise so the executor still works without WS.
            curr_price = self._current_price(symbol)
            if curr_price is None:
                continue

            if curr_price <= pos['limit_price']:
                self._fill_pending_limit(symbol, pos, curr_price, elapsed)

    def _current_price(self, symbol):
        """Best-effort current price. Used by the pending-order sweep so we
        don't have to wait for the analysis loop to tick."""
        # ANALYSIS_020_TIMELINE is updated every ~5s by the analyzer and
        # is the same source of truth the rest of this executor uses, so
        # reading the latest snapshot keeps fill-check and exit-check in
        # sync without bringing the WS cache into this process.
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
        # Reset watermarks now that the position is live. Drawdown/MFE
        # tracking starts at fill, not at order placement.
        pos['max_drawdown'] = 0
        pos['max_favorable_usdt'] = 0.0
        pos['breakeven_armed'] = False

        self.r.hset(VIRTUAL_POSITIONS_KEY, symbol, json.dumps(pos))
        # Record how good the fill was vs the signal price — this is the
        # key metric for tuning limit_offset_pct over time.
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
        if not pos_raw:
            return
        try:
            pos = json.loads(pos_raw)
        except Exception:
            # Corrupt entry — drop it so it can't poison this loop forever.
            self.r.hdel(VIRTUAL_POSITIONS_KEY, symbol)
            return

        # PENDING_LIMIT positions are handled by _sweep_pending_orders;
        # all the OPEN-position machinery below assumes a real entry_price
        # and would crash on the None placeholders left during pending.
        if pos.get('status') == 'PENDING_LIMIT':
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
