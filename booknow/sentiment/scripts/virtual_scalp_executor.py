import os
import redis
import json
import time
import uuid
from datetime import datetime
from decimal import Decimal, ROUND_DOWN

# Optional: ccxt only needed for live mode. Import is lazy so paper-only
# deployments without ccxt installed still run.
try:
    import ccxt
except ImportError:  # pragma: no cover
    ccxt = None  # type: ignore

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

        # Live-trading state. live_mode is hot-reloaded from TRADING_CONFIG
        # every loop iteration so an operator can flip paper↔live without
        # restarting. Default True — live trading is the intended mode.
        self.live_mode = True
        self.client = None
        self.symbol_filters = {}     # symbol -> {step_size, tick_size, min_notional}

        self._init_binance_client()
        print("🚀 [VIRTUAL SCALPER] Limit-buy + %-TP mode (1h patience).")
        print(f"   live_mode default = {self.live_mode}  client = {'ready' if self.client else 'unavailable'}")

    # ── Live-trading scaffolding ─────────────────────────────────────────

    def _init_binance_client(self):
        """One-shot ccxt client init from env. None on any failure;
        live mode degrades to paper instead of crashing the loop."""
        if ccxt is None:
            print("⚠️ [VIRTUAL] ccxt not installed — paper-only mode")
            return
        api_key = os.getenv("BINANCE_API_KEY")
        api_secret = os.getenv("BINANCE_SECRET_KEY")
        if not api_key or not api_secret:
            print("⚠️ [VIRTUAL] Binance API keys missing — live mode unavailable")
            return
        try:
            self.client = ccxt.binance({
                'apiKey': api_key,
                'secret': api_secret,
                'enableRateLimit': True,
                'options': {'defaultType': 'spot'},
            })
            print("✅ [VIRTUAL] Binance client ready")
        except Exception as e:
            print(f"❌ [VIRTUAL] Binance client init failed: {e}")
            self.client = None

    def _sync_live_mode(self):
        """Hot-reload live_mode flag from TRADING_CONFIG every iteration.
        Default True — operator must explicitly set
        ``virtualScalperLiveMode: false`` to pause live trading."""
        try:
            raw = self.r.get(TRADING_CONFIG_KEY)
            if raw:
                cfg = json.loads(raw)
                self.live_mode = bool(cfg.get("virtualScalperLiveMode", True))
        except Exception:
            pass

    def _is_live(self) -> bool:
        return self.client is not None and self.live_mode

    @staticmethod
    def _to_ccxt_symbol(symbol: str) -> str:
        """BTCUSDT → BTC/USDT for ccxt's market lookups."""
        if "/" in symbol:
            return symbol
        if symbol.endswith("USDT"):
            return f"{symbol[:-4]}/USDT"
        return symbol

    @staticmethod
    def _round_step(value: float, step: float) -> float:
        """Round *value* down to the nearest *step* (Binance lot size)."""
        if step <= 0:
            return value
        d = (Decimal(str(value)) / Decimal(str(step))).to_integral_value(rounding=ROUND_DOWN)
        return float(d * Decimal(str(step)))

    def _get_filters(self, symbol: str):
        """Cache (step_size, tick_size, min_notional) per symbol."""
        if symbol in self.symbol_filters:
            return self.symbol_filters[symbol]
        if self.client is None:
            return None
        try:
            ccxt_sym = self._to_ccxt_symbol(symbol)
            self.client.load_markets()
            market = self.client.market(ccxt_sym)
            f = {
                'step_size':    float(market.get('precision', {}).get('amount') or 0.00000001),
                'tick_size':    float(market.get('precision', {}).get('price')  or 0.00000001),
                'min_notional': float((market.get('limits', {}).get('cost') or {}).get('min') or 5.0),
            }
            # ccxt's "precision" can be returned as decimal-place count rather
            # than tick — convert to a step if so. (e.g. precision=8 → 1e-8.)
            if f['step_size'] >= 1:
                f['step_size'] = 10 ** -int(f['step_size'])
            if f['tick_size'] >= 1:
                f['tick_size'] = 10 ** -int(f['tick_size'])
            self.symbol_filters[symbol] = f
            return f
        except Exception as e:
            print(f"⚠️ [VIRTUAL] filter fetch failed for {symbol}: {e}")
            return None

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
                # Hot-reload live_mode at the top of every loop so config
                # toggles take effect within ~1 s.
                self._sync_live_mode()

                now = time.time()
                all_analysis = self.r.hgetall(ANALYSIS_020_KEY)

                if now - last_heartbeat > 30:
                    active_pos_count = self.r.hlen(VIRTUAL_POSITIONS_KEY)
                    mode = "LIVE" if self._is_live() else "PAPER"
                    print(f"💓 [HEARTBEAT] mode={mode} | Monitoring {len(all_analysis)} coins | Active Positions: {active_pos_count}")
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
        """Place a limit-buy at limitBuyOffsetPct below signal price.

        In paper mode (default fallback): just stages a row in
        VIRTUAL_POSITIONS_KEY with status=PENDING_LIMIT for the in-process
        fill simulator to handle.

        In live mode: also calls Binance create_limit_buy_order, stores
        the returned order_id and rounded qty/price on the row, then
        defers fill detection to _sweep_pending_orders which polls
        Binance for the order's actual status.
        """
        investment, profit_target_usdt, profit_pct, limit_offset_pct = self._load_trading_config()
        base_price = self._fetch_base_price(symbol)
        limit_price = signal_price * (1 - limit_offset_pct / 100.0)
        now_ts = time.time()

        pos = {
            "id": str(uuid.uuid4())[:8],
            "symbol": symbol,
            "status": "PENDING_LIMIT",
            "mode": "live" if self._is_live() else "paper",
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
            # Live-only fields (None in paper mode)
            "order_id": None,
            "live_limit_price": None,
            "live_qty": None,
        }

        # In live mode, place the real order BEFORE writing the position
        # row — if Binance rejects the order (insufficient balance, filter
        # mismatch, etc.) we don't want a stale PENDING_LIMIT row.
        if self._is_live():
            placed = self._place_real_limit_buy(symbol, limit_price, investment)
            if placed is None:
                # Real order failed → drop the trade entirely. Don't fall
                # back to paper, that would mask a misconfiguration.
                print(f"❌ [VIRTUAL] live limit-buy rejected for {symbol}; skipping signal")
                return
            pos["order_id"]         = placed["order_id"]
            pos["live_limit_price"] = placed["price"]
            pos["live_qty"]         = placed["qty"]

        self.r.hset(VIRTUAL_POSITIONS_KEY, symbol, json.dumps(pos))
        tag = "LIVE" if pos["mode"] == "live" else "PAPER"
        print(f"📍 [{tag} LIMIT] {symbol} signal={signal_price} limit={limit_price:.6f} (-{limit_offset_pct}%) inv=${investment:.0f} TP=+{profit_pct}%"
              + (f" order_id={pos['order_id']}" if pos["order_id"] else ""))

    def _place_real_limit_buy(self, symbol: str, limit_price: float, investment_usdt: float):
        """Round qty/price by symbol filters and place a Binance limit
        buy. Returns dict with order_id/price/qty on success, None on
        any failure (caller skips the trade)."""
        f = self._get_filters(symbol)
        if f is None:
            return None
        rounded_price = self._round_step(limit_price, f['tick_size'])
        if rounded_price <= 0:
            return None
        qty = self._round_step(investment_usdt / rounded_price, f['step_size'])
        notional = qty * rounded_price
        if qty <= 0 or notional < f['min_notional']:
            print(f"⚠️ [VIRTUAL] {symbol} below min_notional (${notional:.2f} < ${f['min_notional']}); skipping")
            return None
        try:
            ccxt_sym = self._to_ccxt_symbol(symbol)
            order = self.client.create_limit_buy_order(ccxt_sym, qty, rounded_price)
            return {
                "order_id": str(order.get("id") or ""),
                "price":    rounded_price,
                "qty":      qty,
            }
        except Exception as e:
            print(f"❌ [VIRTUAL] create_limit_buy_order failed for {symbol}: {e}")
            return None

    def _sweep_pending_orders(self):
        """Walk every PENDING_LIMIT order. Logic differs by mode:

        - paper: fill when the analyzer's last price ≤ limit_price;
          cancel after LIMIT_ORDER_TIMEOUT_SECONDS without fill.
        - live: poll Binance for the actual order state; on FILLED mark
          OPEN with the real fill price; on CANCELED/EXPIRED drop the
          position; on timeout cancel the order on Binance and drop.
        """
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
            mode = pos.get('mode', 'paper')

            if mode == 'live':
                self._sweep_pending_live(symbol, pos, elapsed)
                continue

            # ── Paper-mode lifecycle (original behaviour) ────────────
            if elapsed > LIMIT_ORDER_TIMEOUT_SECONDS:
                self.r.hdel(VIRTUAL_POSITIONS_KEY, symbol)
                print(f"⏰ [PAPER LIMIT EXPIRED] {symbol} — no fill in {int(elapsed)}s")
                continue

            curr_price = self._current_price(symbol)
            if curr_price is None:
                continue
            if curr_price <= pos['limit_price']:
                self._fill_pending_limit(symbol, pos, curr_price, elapsed)

    def _sweep_pending_live(self, symbol, pos, elapsed):
        """Query Binance for a live PENDING_LIMIT order's current state."""
        order_id = pos.get('order_id')
        if not order_id or self.client is None:
            # Misconfigured row — drop it so we don't loop forever.
            self.r.hdel(VIRTUAL_POSITIONS_KEY, symbol)
            print(f"⚠️ [LIVE LIMIT] {symbol} missing order_id/client — dropping row")
            return
        ccxt_sym = self._to_ccxt_symbol(symbol)

        # Hard timeout: cancel on Binance, drop locally.
        if elapsed > LIMIT_ORDER_TIMEOUT_SECONDS:
            try:
                self.client.cancel_order(order_id, ccxt_sym)
            except Exception as e:
                print(f"⚠️ [LIVE LIMIT] cancel failed for {symbol} ({order_id}): {e}")
            self.r.hdel(VIRTUAL_POSITIONS_KEY, symbol)
            print(f"⏰ [LIVE LIMIT EXPIRED] {symbol} — cancelled on Binance after {int(elapsed)}s")
            return

        try:
            order = self.client.fetch_order(order_id, ccxt_sym)
        except Exception as e:
            # Transient — try again next tick.
            print(f"⚠️ [LIVE LIMIT] fetch_order failed for {symbol}: {e}")
            return

        status = (order.get("status") or "").lower()  # 'open', 'closed', 'canceled'
        filled = float(order.get("filled") or 0.0)
        live_qty = float(pos.get("live_qty") or 0.0)

        if status == "closed" and filled > 0:
            # ccxt 'closed' for limit orders means fully filled.
            avg_price = float(order.get("average") or order.get("price") or pos.get("live_limit_price"))
            self._fill_pending_limit(symbol, pos, avg_price, elapsed, real_filled_qty=filled)
        elif status in ("canceled", "expired"):
            self.r.hdel(VIRTUAL_POSITIONS_KEY, symbol)
            print(f"❌ [LIVE LIMIT] {symbol} {status} on Binance — dropping position")
        # status == 'open' → keep waiting; partial fills are rare on
        # spot limits and are handled implicitly by the next tick.

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

    def _fill_pending_limit(self, symbol, pos, observed_price, elapsed, real_filled_qty=None):
        """Convert a PENDING_LIMIT into an OPEN position.

        Paper mode: fill at the original limit price (conservative —
        exchanges fill resting orders at L when the market touches L).

        Live mode: fill at the actual fill price returned by Binance,
        with the real filled quantity (may be slightly less than the
        requested live_qty if Binance applied stricter rounding).
        """
        if real_filled_qty is not None:
            # Live fill — use exchange-reported price/qty for accuracy.
            fill_price = observed_price
            quantity   = real_filled_qty
        else:
            # Paper fill — synthetic at the limit.
            fill_price = pos['limit_price']
            quantity   = pos['investment'] / fill_price

        now_ts = time.time()

        pos['status'] = 'OPEN'
        pos['entry_price'] = fill_price
        pos['entry_timestamp'] = now_ts
        pos['entry_time'] = datetime.now().strftime('%H:%M:%S')
        pos['quantity'] = quantity
        pos['max_drawdown'] = 0  # restart tracking from fill, not from order placement

        self.r.hset(VIRTUAL_POSITIONS_KEY, symbol, json.dumps(pos))
        signal_price = pos.get('signal_price', fill_price)
        improvement = (signal_price - fill_price) / signal_price * 100 if signal_price else 0
        tag = "LIVE" if pos.get("mode") == "live" else "PAPER"
        print(f"✅ [{tag} FILLED] {symbol} fill={fill_price:.6f} qty={quantity:.6f} "
              f"(-{improvement:.2f}% vs signal {signal_price:.6f}) after {elapsed:.0f}s")

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
        mode = pos.get("mode", "paper")

        # ── LIVE EXIT: place real market sell first; only finalize the
        #    accounting when the exchange confirms. If the sell fails we
        #    re-queue the position and bail so a future tick retries it.
        if mode == "live" and pos.get("quantity"):
            real_exit = self._place_real_market_sell(symbol, float(pos["quantity"]))
            if real_exit is None:
                # Don't delete the position — we still own the coin on
                # Binance. Surface the failure and try again next tick.
                print(f"⚠️ [LIVE EXIT] {symbol} sell failed; will retry next tick (reason was {reason})")
                return
            # Use the actual fill price from Binance for PnL math.
            exit_price = real_exit["price"]
            pnl_pct    = (exit_price - pos["entry_price"]) / pos["entry_price"]

        # Investment is the per-position snapshot taken at open (from TradingConfig).
        investment = pos.get('investment', DEFAULT_BUY_AMOUNT_USDT)
        # Real-world Binance spot fees: 0.10 % per side.
        buy_fee = investment * 0.001
        sell_value = investment * (1 + pnl_pct)
        sell_fee = sell_value * 0.001
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

        # Long-term archive on the analyse Redis (best-effort).
        kind = "VIRTUAL_LIVE" if mode == "live" else "VIRTUAL_PAPER"
        archive_closed_trade(symbol, kind, pos)

        color = "🟢" if net_pnl_usdt > 0 else "🔴"
        tag = "LIVE" if mode == "live" else "PAPER"
        print(f"{color} [{tag} SELL] {symbol} | Net PnL: ${net_pnl_usdt:.4f} | Fees: ${total_fees:.4f} | Held: {int(pos['hold_duration'])}s")

    def _place_real_market_sell(self, symbol: str, qty: float):
        """Best-effort market sell. Returns {price, qty} on success,
        None on failure so the caller can retry."""
        if self.client is None:
            return None
        try:
            ccxt_sym = self._to_ccxt_symbol(symbol)
            order = self.client.create_market_sell_order(ccxt_sym, qty)
            avg_price = float(order.get("average") or order.get("price") or 0.0)
            return {"price": avg_price, "qty": qty}
        except Exception as e:
            print(f"❌ [VIRTUAL] create_market_sell_order failed for {symbol}: {e}")
            return None

if __name__ == "__main__":
    executor = VirtualScalpExecutor()
    executor.process_signals()
