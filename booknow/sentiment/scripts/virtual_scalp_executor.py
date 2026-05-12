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

# Metrics collector — captures every signal/skip/buy/fill/exit into Redis.
# Optional import so older deploys without the helper still run.
try:
    from metrics_collector import make_collector
except Exception:
    make_collector = None  # type: ignore

# Laddered Recovery state machine.
try:
    import laddered_position as ladder  # type: ignore
except Exception:
    ladder = None  # type: ignore

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
# 2026-05-12 iter 11: $55/leg, $0.15 net target, Buy 3 disabled.
DEFAULT_BUY_AMOUNT_USDT = 50.0
DEFAULT_PROFIT_TARGET_USDT = 0.15
DEFAULT_PROFIT_PCT = 0.5             # % above entry → ≈$0.15 NET on $55 buy after 0.2% fees
DEFAULT_LIMIT_OFFSET_PCT = 0.15      # 0.15 % below signal

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

        # Falling-knife filter knobs. Hot-reloaded by _sync_live_mode().
        self.fk_enabled = True
        self.fk_max_24h = 8.0
        self.fk_max_1h_range = 6.0
        self.fk_overbought_skip = True
        self.fk_overbought_60m = 1.5

        # Post-pump-bleed filter (daily timeframe, added 2026-05-12 after
        # JTO loss). Mirrors the Fast Scalper algorithm. See spec in
        # _passes_post_pump_filter() below.
        self.pp_enabled = True
        self.pp_threshold_pct = 30.0
        self.pp_off_peak_min_pct = 10.0
        self.pp_min_days_since_peak = 2
        # 2026-05-12 iter 13 tuning: 14 → 15 days pump window, 7 → 10 days
        # baseline (mirrors Fast Scalper for parity).
        self.pp_lookback_days = 15
        self.pp_baseline_days = 10
        self._d1_cache: dict = {}
        self._d1_cache_ttl_sec = 600

        # Stop-loss USDT — when 0/negative, SOFT_STOP_LOSS_USDT and
        # MAX_VIRTUAL_LOSS_USDT below are bypassed (Option B patient hold).
        self.stop_loss_usdt = 0.0

        # Fast-drop-without-volume filter (Pattern C). Mirrors Fast Scalper.
        self.fd_enabled = True
        self.fd_detect_minutes = 3
        self.fd_threshold_pct = 0.5
        self.fd_vol_surge_mult = 2.0

        # Laddered Recovery (paper or live, mirrors Fast Scalper).
        # 2026-05-12 iter 11: $55/leg, 2-leg ladder (Buy 3 disabled).
        self.ladder_enabled = False
        self.max_concurrent_ladders = 1
        self.single_coin_mode = True
        self.ladder_buy1_size = 50.0
        self.ladder_buy2_size = 50.0
        self.ladder_buy3_size = 0.0
        self.ladder_buy2_offset_pct = 0.5
        self.ladder_buy3_offset_pct = 1.0
        self.ladder_tp_from_avg_pct = 0.6
        self.ladder_target_net_usdt = 0.15
        self.ladder_fee_rate_per_side = 0.00075
        self.ladder_hard_stop_pct = 1.0
        self.ladder_buy1_offset_pct = 0.15  # 0.15 % below signal
        self.ladder_cooldown_seconds = 14400  # 4 hours

        # Metrics collector — same Redis as everything else.
        self.metrics = make_collector(self.r, enabled=True) if make_collector else None

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
        """Hot-reload live_mode flag + falling-knife filter knobs from
        TRADING_CONFIG every iteration. Default True — operator must
        explicitly set ``virtualScalperLiveMode: false`` to pause live.
        Also picks up stopLossUsdt: when 0/negative, the SOFT/HARD stops
        are bypassed entirely (Option B "patient hold")."""
        try:
            raw = self.r.get(TRADING_CONFIG_KEY)
            if raw:
                cfg = json.loads(raw)
                self.live_mode = bool(cfg.get("virtualScalperLiveMode", True))
                self.fk_enabled = bool(cfg.get("fallingKnifeFilterEnabled", True))
                self.fk_max_24h = float(cfg.get("maxChange24hPct", 8.0))
                self.fk_max_1h_range = float(cfg.get("maxRange1hPct", 6.0))
                self.fk_overbought_skip = bool(cfg.get("overboughtSkipEnabled", True))
                self.fk_overbought_60m = float(cfg.get("overbought60mPct", 1.5))
                # Post-pump-bleed (daily) filter knobs (added 2026-05-12).
                self.pp_enabled = bool(cfg.get("postPumpFilterEnabled", True))
                self.pp_threshold_pct = float(cfg.get("postPumpThresholdPct", 30.0))
                self.pp_off_peak_min_pct = float(cfg.get("postPumpOffPeakMinPct", 10.0))
                self.pp_min_days_since_peak = int(cfg.get("postPumpMinDaysSincePeak", 2))
                self.pp_lookback_days = int(cfg.get("postPumpLookbackDays", 15))
                self.pp_baseline_days = int(cfg.get("postPumpBaselineDays", 10))
                # Stop-loss config (0 = disabled, matches Fast Scalper).
                self.stop_loss_usdt = float(cfg.get("stopLossUsdt", 0.0))
                # Fast-drop-without-volume filter knobs.
                self.fd_enabled = bool(cfg.get("fastDropFilterEnabled", True))
                self.fd_detect_minutes = int(cfg.get("fastDropDetectMinutes", 3))
                self.fd_threshold_pct = float(cfg.get("fastDropThresholdPct", 0.5))
                self.fd_vol_surge_mult = float(cfg.get("volSurgeThresholdMultiplier", 2.0))
                # Laddered Recovery knobs.
                self.ladder_enabled = bool(cfg.get("ladderedRecoveryEnabled", False))
                self.max_concurrent_ladders = int(cfg.get("maxConcurrentLadders", 1))
                self.single_coin_mode = bool(cfg.get("singleCoinModeEnabled", False))
                self.ladder_buy1_size = float(cfg.get("ladderBuy1SizeUsdt", 50.0))
                self.ladder_buy2_size = float(cfg.get("ladderBuy2SizeUsdt", 50.0))
                self.ladder_buy3_size = float(cfg.get("ladderBuy3SizeUsdt", 0.0))
                self.ladder_buy2_offset_pct = float(cfg.get("ladderBuy2OffsetPct", 0.5))
                self.ladder_buy3_offset_pct = float(cfg.get("ladderBuy3OffsetPct", 1.0))
                self.ladder_tp_from_avg_pct = float(cfg.get("ladderTpFromAvgPct", 0.6))
                self.ladder_target_net_usdt = float(cfg.get("ladderTargetNetProfitUsdt", 0.15))
                self.ladder_fee_rate_per_side = float(cfg.get("ladderFeeRatePerSide", 0.00075))
                self.ladder_hard_stop_pct = float(cfg.get("ladderHardStopBelowBuy3Pct", 1.0))
                self.ladder_buy1_offset_pct = float(cfg.get("ladderBuy1OffsetPct", 0.15))
                self.ladder_cooldown_seconds = int(cfg.get("ladderCooldownSeconds", 14400))
                if self.metrics is not None:
                    self.metrics.enabled = bool(cfg.get("metricsEnabled", True))
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

    # ── Paper Laddered Recovery (multi-ladder) ──────────────────────────
    # Mirrors Fast Scalper's logic but uses simulated fills (price ≤ limit
    # = filled). Separate Redis hash so paper + real state never collide.
    PAPER_LADDERS_HASH = "VIRTUAL:LADDER_STATES"

    def _paper_ladder_count(self) -> int:
        try:
            return self.r.hlen(self.PAPER_LADDERS_HASH) or 0
        except Exception:
            return 0

    def _paper_ladder_can_open(self) -> bool:
        return self._paper_ladder_count() < self.max_concurrent_ladders

    def _paper_ladder_load(self, symbol: str):
        try:
            raw = self.r.hget(self.PAPER_LADDERS_HASH, symbol)
            if not raw: return None
            return ladder.LadderState.from_dict(json.loads(raw))
        except Exception:
            return None

    def _paper_ladder_save(self, state):
        try:
            if state.state == ladder.CLOSED:
                self.r.hdel(self.PAPER_LADDERS_HASH, state.symbol)
            else:
                self.r.hset(self.PAPER_LADDERS_HASH, state.symbol, json.dumps(state.to_dict()))
        except Exception:
            pass

    def _paper_ladder_clear(self, symbol: str):
        try:
            self.r.hdel(self.PAPER_LADDERS_HASH, symbol)
        except Exception:
            pass

    def _paper_ladder_load_all(self):
        try:
            raw = self.r.hgetall(self.PAPER_LADDERS_HASH)
        except Exception:
            return []
        out = []
        for sym, val in (raw or {}).items():
            try:
                out.append(ladder.LadderState.from_dict(json.loads(val)))
            except Exception:
                try: self.r.hdel(self.PAPER_LADDERS_HASH, sym)
                except Exception: pass
        return out

    def _paper_ladder_active_for_symbol(self, symbol: str) -> bool:
        return self._paper_ladder_load(symbol) is not None

    def _paper_ladder_start(self, symbol, signal_price, features=None):
        """Start a ladder. Real-money path uses Binance (when live_mode=True);
        paper path simulates instant fill. Both share the same state machine."""
        if self._is_live() and self.client is not None:
            self._ladder_start_live(symbol, signal_price, features=features)
        else:
            self._ladder_start_paper(symbol, signal_price, features=features)

    def _ladder_start_paper(self, symbol, signal_price, features=None):
        """Paper start: simulate instant buy 1 fill at signal price."""
        now_ms = int(time.time() * 1000)
        buy1_qty = self.ladder_buy1_size / max(signal_price, 1e-12)
        state = ladder.LadderState(
            symbol=symbol, signal_price=signal_price, signal_ts=now_ms,
            state=ladder.ACTIVE_1,
            buy_1=ladder.Leg(
                label="buy_1", target_price=signal_price,
                size_usdt=self.ladder_buy1_size,
                qty_filled=buy1_qty * 0.999,
                fill_price=signal_price, fill_ts=now_ms, status="filled",
            ),
        )
        # 2026-05-11 iter 6: reference price = Buy 1 fill price (= signal
        # in paper since no spread). Keeps semantics identical to live.
        ref_price = state.buy_1.fill_price if state.buy_1 else signal_price
        state.buy_2 = ladder.Leg(
            label="buy_2",
            target_price=ref_price * (1 - self.ladder_buy2_offset_pct / 100.0),
            size_usdt=self.ladder_buy2_size, status="pending",
        )
        state.buy_3 = ladder.Leg(
            label="buy_3",
            target_price=ref_price * (1 - self.ladder_buy3_offset_pct / 100.0),
            size_usdt=self.ladder_buy3_size, status="pending",
        )
        state.tp_target_price = ladder.tp_price(state.weighted_avg(), self._effective_tp_pct())
        self._paper_ladder_save(state)
        print(f"🪜 [paper-ladder] {symbol} buy 1 filled @ {signal_price} "
              f"buy2@{state.buy_2.target_price:.6g} buy3@{state.buy_3.target_price:.6g} "
              f"tp@{state.tp_target_price:.6g}")
        if self.metrics is not None:
            audit = self._compute_audit_snapshot(symbol, signal_price, signal_price)
            self.metrics.buy_placed(
                symbol, signal_price, self.ladder_buy1_size,
                features=features, order_type="virtual_ladder_buy_1_paper",
                **audit,
            )
            self.metrics.fill_recorded(symbol, signal_price, state.buy_1.qty_filled)

    def _compute_audit_snapshot(self, symbol, signal_price, buy1_limit_price):
        """Same logic as Fast Scalper's helper — but sync (Virtual uses sync ccxt).
        Captures pre_signal_price (5m back), pre_signal_price_2 (10m back),
        past 15m extremes, Buy 2 limit, three target-sell prices, and tags
        scalper_origin = 'VIRTUAL'."""
        out = {
            "signal_price": signal_price,
            "buy_1_limit_price": buy1_limit_price,
            "pre_signal_price": None,
            "pre_signal_price_2": None,
            "past_15min_low": None,
            "past_15min_high": None,
            "buy_2_limit_price": None,
            "target_sell_005": None,
            "target_sell_010": None,
            "target_sell_015": None,
            "scalper_origin": "VIRTUAL",
        }
        if self.client is not None:
            try:
                ccxt_sym = self._to_ccxt_symbol(symbol)
                candles = self.client.fetch_ohlcv(ccxt_sym, "1m", limit=20)
                if candles and len(candles) >= 5:
                    pre1 = candles[-6] if len(candles) >= 6 else candles[0]
                    out["pre_signal_price"] = float(pre1[4] or 0)
                    if len(candles) >= 11:
                        pre2 = candles[-11]
                        out["pre_signal_price_2"] = float(pre2[4] or 0)
                    last_15 = candles[-15:] if len(candles) >= 15 else candles
                    highs = [float(c[2] or 0) for c in last_15 if float(c[2] or 0) > 0]
                    lows  = [float(c[3] or 0) for c in last_15 if float(c[3] or 0) > 0]
                    if highs: out["past_15min_high"] = max(highs)
                    if lows:  out["past_15min_low"]  = min(lows)
            except Exception as e:
                print(f"audit 15m candles failed for {symbol}: {e}")
        try:
            out["buy_2_limit_price"] = signal_price * (1 - self.ladder_buy2_offset_pct / 100.0)
        except Exception:
            pass
        fee_2 = 2 * self.ladder_fee_rate_per_side
        try:
            buy_size = self.ladder_buy1_size or 1.0
            for net, key in ((0.05, "target_sell_005"), (0.10, "target_sell_010"), (0.15, "target_sell_015")):
                tp_pct = (net / buy_size) + fee_2
                out[key] = buy1_limit_price * (1 + tp_pct)
        except Exception:
            pass
        return out

    def _has_sufficient_usdt(self, required: float) -> bool:
        """Pre-flight balance check (sync). Returns True if we can proceed."""
        if self.client is None: return True
        try:
            bal = self.client.fetch_balance()
            free = float((bal.get('USDT') or {}).get('free') or 0)
        except Exception as e:
            print(f"⚠️ [v-ladder] balance fetch failed ({e}); proceeding")
            return True
        # 2026-05-12 iter 13: margin 10% → 3% (mirrors Fast Scalper).
        # Fees on $100 ladder are ~$0.20 net; 3% = $3 buffer is plenty.
        margin = required * 0.03
        if free < required + margin:
            print(f"⚠️ [v-ladder] insufficient USDT: free=${free:.4f} need=${required:.4f} (+margin)")
            return False
        return True

    def _handle_external_cancel_live(self, state, who: str):
        """Manual cancel detected — cancel remaining orders, free slot,
        set cooldown. Do NOT auto-sell any held qty (operator's call)."""
        print(f"🛑 [v-ladder] {state.symbol} {who} cancelled externally; releasing slot + cooldown")
        ccxt_sym = self._to_ccxt_symbol(state.symbol)
        for leg in (state.buy_1, state.buy_2, state.buy_3):
            if leg and leg.order_id and leg.status not in ("filled", "cancelled"):
                try: self.client.cancel_order(leg.order_id, ccxt_sym)
                except Exception: pass
                leg.status = "cancelled"
                leg.order_id = None
        if state.tp_order_id:
            try: self.client.cancel_order(state.tp_order_id, ccxt_sym)
            except Exception: pass
            state.tp_order_id = None
        state.state = ladder.CLOSED
        state.exit_reason = "manual_cancel"
        state.closed_ts = int(time.time() * 1000)
        if self.metrics is not None:
            try:
                self.metrics.exit_recorded(state.symbol, state.weighted_avg() or 0,
                                           0, reason="manual_cancel", pnl_usdt=0)
            except Exception:
                pass
        self._paper_ladder_clear(state.symbol)
        ladder.set_cooldown(self.r, state.symbol, self.ladder_cooldown_seconds)

    def _ladder_start_live(self, symbol, signal_price, features=None):
        """Live start: place Buy 1 as MARKET (default) so Buy 2/3 limits
        can be placed in the same call. Falls back to aggressive-limit-
        at-ask when ladderBuy1UseMarketOrder is False."""
        f = self._get_filters(symbol)
        if f is None:
            print(f"⚠️ [v-ladder] {symbol} missing filters; skipping")
            return
        # Pre-flight balance check: full 3-leg total
        total_needed = (self.ladder_buy1_size
                        + self.ladder_buy2_size + self.ladder_buy3_size)
        if not self._has_sufficient_usdt(total_needed):
            print(f"⏸️  [v-ladder] {symbol} skipped — funds short for full ladder")
            return
        ccxt_sym = self._to_ccxt_symbol(symbol)
        tick = f['tick_size'] or 0.00000001
        now_ms = int(time.time() * 1000)

        # Determine routing: offset > 0 takes priority over market flag
        cfg = {}
        try:
            raw = self.r.get(TRADING_CONFIG_KEY)
            if raw: cfg = json.loads(raw)
        except Exception:
            pass
        use_market = bool(cfg.get("ladderBuy1UseMarketOrder", True))
        buy1_offset = float(cfg.get("ladderBuy1OffsetPct", 0.15))

        # If offset > 0: place LIMIT BUY at signal × (1 - offset/100)
        if buy1_offset > 0:
            buy1_price = self._round_step(signal_price * (1 - buy1_offset / 100.0), tick)
            buy1_qty = self._round_step(self.ladder_buy1_size / max(buy1_price, 1e-12), f['step_size'])
            if buy1_qty * buy1_price < f['min_notional']:
                print(f"⚠️ [v-ladder] {symbol} buy 1 notional too low")
                return
            try:
                placed = self.client.create_limit_buy_order(ccxt_sym, buy1_qty, buy1_price)
            except Exception as e:
                print(f"❌ [v-ladder] {symbol} limit buy failed: {e}")
                return
            order_id = str(placed.get("id") or "")
            if not order_id:
                print(f"⚠️ [v-ladder] {symbol} buy 1 returned no order id")
                return
            state = ladder.LadderState(
                symbol=symbol, signal_price=signal_price, signal_ts=now_ms,
                state=ladder.PENDING_BUY_1,
                buy_1=ladder.Leg(label="buy_1", target_price=buy1_price,
                                 size_usdt=self.ladder_buy1_size, order_id=order_id),
            )
            self._paper_ladder_save(state)
            print(f"🪜 [v-ladder] {symbol} buy 1 LIMIT @ {buy1_price} "
                  f"(-{buy1_offset}% from signal {signal_price}) qty={buy1_qty}")
            if self.metrics is not None:
                audit = self._compute_audit_snapshot(symbol, signal_price, buy1_price)
                self.metrics.buy_placed(
                    symbol, buy1_price, self.ladder_buy1_size,
                    features=features, order_type="virtual_ladder_buy_1_offset_limit",
                    offset_pct=buy1_offset, **audit,
                )
            return

        if use_market:
            # MARKET PATH — fast-path: legs 2/3 + TP go on the book immediately
            buy1_qty = self._round_step(self.ladder_buy1_size / max(signal_price, 1e-12), f['step_size'])
            if buy1_qty * signal_price < f['min_notional']:
                print(f"⚠️ [v-ladder] {symbol} buy 1 notional too low")
                return
            try:
                placed = self.client.create_market_buy_order(ccxt_sym, buy1_qty)
            except Exception as e:
                print(f"❌ [v-ladder] {symbol} market buy failed: {e}")
                return
            filled_qty = float(placed.get("filled") or buy1_qty)
            fill_price = float(placed.get("average") or placed.get("price") or signal_price)
            if filled_qty <= 0:
                print(f"⚠️ [v-ladder] {symbol} market buy returned filled=0")
                return
            base_qty = self._round_step(filled_qty * 0.999, f['step_size'])
            state = ladder.LadderState(
                symbol=symbol, signal_price=signal_price, signal_ts=now_ms,
                state=ladder.ACTIVE_1,
                buy_1=ladder.Leg(
                    label="buy_1", target_price=fill_price,
                    size_usdt=self.ladder_buy1_size, order_id=str(placed.get("id") or ""),
                    qty_filled=base_qty, fill_price=fill_price, fill_ts=now_ms,
                    status="filled",
                ),
            )
            print(f"🪜 [v-ladder] {symbol} buy 1 MARKET filled qty={base_qty} @ {fill_price}")
            if self.metrics is not None:
                audit = self._compute_audit_snapshot(symbol, signal_price, fill_price)
                self.metrics.buy_placed(
                    symbol, fill_price, self.ladder_buy1_size,
                    features=features, order_type="virtual_ladder_buy_1_market",
                    **audit,
                )
                self.metrics.fill_recorded(symbol, fill_price, base_qty)
            self._ladder_place_legs_after_buy1_live(state, f, tick)
            self._paper_ladder_save(state)
            return

        # LIMIT PATH (legacy) — wait for fill before placing legs 2/3
        try:
            book = self.client.fetch_order_book(ccxt_sym, limit=5)
            best_ask = float(book["asks"][0][0]) if book.get("asks") else signal_price
        except Exception:
            best_ask = signal_price
        buy1_price = self._round_step(best_ask, tick)
        buy1_qty = self._round_step(self.ladder_buy1_size / max(buy1_price, 1e-12), f['step_size'])
        if buy1_qty * buy1_price < f['min_notional']:
            print(f"⚠️ [v-ladder] {symbol} buy 1 notional too low")
            return
        try:
            placed = self.client.create_limit_buy_order(ccxt_sym, buy1_qty, buy1_price)
        except Exception as e:
            print(f"❌ [v-ladder] {symbol} create_limit_buy_order failed: {e}")
            return
        order_id = str(placed.get("id") or "")
        if not order_id:
            print(f"⚠️ [v-ladder] {symbol} buy 1 returned no order id")
            return
        state = ladder.LadderState(
            symbol=symbol, signal_price=signal_price, signal_ts=now_ms,
            state=ladder.PENDING_BUY_1,
            buy_1=ladder.Leg(label="buy_1", target_price=buy1_price,
                             size_usdt=self.ladder_buy1_size, order_id=order_id),
        )
        self._paper_ladder_save(state)
        print(f"🪜 [v-ladder] {symbol} buy 1 LIMIT placed @ {buy1_price} qty={buy1_qty}")
        if self.metrics is not None:
            audit = self._compute_audit_snapshot(symbol, signal_price, buy1_price)
            self.metrics.buy_placed(
                symbol, buy1_price, self.ladder_buy1_size,
                features=features, order_type="virtual_ladder_buy_1_limit",
                **audit,
            )

    def _ladder_place_legs_after_buy1_live(self, state, f, tick):
        """Shared helper: places Buy 2/3 limits + TP once buy 1 is filled.
        Called by both the market fast-path and the polling slow-path.

        2026-05-11 iter 6: reference price is Buy 1's actual fill (was
        signal_price). Market orders pay the spread; offsets measured
        from the real entry price are more accurate for DCA logic."""
        symbol = state.symbol
        ccxt_sym = self._to_ccxt_symbol(symbol)
        ref_price = (state.buy_1.fill_price if state.buy_1 and state.buy_1.fill_price
                     else state.signal_price)
        buy2_price = self._round_step(ref_price * (1 - self.ladder_buy2_offset_pct / 100.0), tick)
        buy3_price = self._round_step(ref_price * (1 - self.ladder_buy3_offset_pct / 100.0), tick)
        buy2_qty = self._round_step(self.ladder_buy2_size / max(buy2_price, 1e-12), f['step_size'])
        buy3_qty = self._round_step(self.ladder_buy3_size / max(buy3_price, 1e-12), f['step_size'])

        buy2_oid = buy3_oid = None
        if buy2_qty * buy2_price >= f['min_notional']:
            try:
                o2 = self.client.create_limit_buy_order(ccxt_sym, buy2_qty, buy2_price)
                buy2_oid = str(o2.get('id') or '')
            except Exception as e:
                print(f"⚠️ [v-ladder] {symbol} buy 2 placement failed: {e}")
        if buy3_qty * buy3_price >= f['min_notional']:
            try:
                o3 = self.client.create_limit_buy_order(ccxt_sym, buy3_qty, buy3_price)
                buy3_oid = str(o3.get('id') or '')
            except Exception as e:
                print(f"⚠️ [v-ladder] {symbol} buy 3 placement failed: {e}")

        state.buy_2 = ladder.Leg(label="buy_2", target_price=buy2_price,
                                 size_usdt=self.ladder_buy2_size, order_id=buy2_oid)
        state.buy_3 = ladder.Leg(label="buy_3", target_price=buy3_price,
                                 size_usdt=self.ladder_buy3_size, order_id=buy3_oid)
        self._ladder_place_tp_live(state, state.total_qty(), f, tick)
        print(f"🪜 [v-ladder] {symbol} legs placed buy2@{buy2_price} buy3@{buy3_price} "
              f"tp@{state.tp_target_price:.6g}")

    def _paper_ladder_tick(self, curr_price_by_symbol):
        """Drive all paper ladders forward — called each loop iteration."""
        states = self._paper_ladder_load_all()
        if not states: return
        for state in states:
            if state.state == ladder.CLOSED: continue
            sym = state.symbol
            last = curr_price_by_symbol.get(sym) or self._current_price(sym) or 0
            if last <= 0: continue
            try:
                self._paper_ladder_tick_one(state, last)
            except Exception as exc:
                print(f"⚠️ [paper-ladder] tick failed for {sym}: {exc}")

    def _paper_ladder_tick_one(self, state, last):
        """Dispatch: live mode polls Binance, paper mode simulates."""
        if self._is_live() and self.client is not None and self._state_is_live(state):
            self._ladder_tick_one_live(state, last)
        else:
            self._ladder_tick_one_paper(state, last)

    def _state_is_live(self, state) -> bool:
        """A state is 'live' if any leg has an order_id (real Binance order)."""
        for leg in (state.buy_1, state.buy_2, state.buy_3):
            if leg and leg.order_id:
                return True
        return False

    def _ladder_tick_one_paper(self, state, last):
        """Per-ladder state transitions — paper mode."""
        sym = state.symbol
        now_ms = int(time.time() * 1000)

        # TP check
        if state.tp_target_price > 0 and last >= state.tp_target_price:
            self._paper_ladder_close(state, last, ladder.EXIT_TP)
            return

        # Hard stop (only when buy 3 filled)
        if state.state == ladder.ACTIVE_3 and state.hard_stop_price > 0 and last <= state.hard_stop_price:
            self._paper_ladder_close(state, last, ladder.EXIT_STOP)
            return

        # ACTIVE_1: watch buy 2 and buy 3 limits
        if state.state == ladder.ACTIVE_1:
            if state.buy_2 and state.buy_2.status == "pending" and last <= state.buy_2.target_price:
                qty = state.buy_2.size_usdt / max(state.buy_2.target_price, 1e-12)
                state.buy_2.qty_filled = qty * 0.999
                state.buy_2.fill_price = state.buy_2.target_price
                state.buy_2.fill_ts = now_ms
                state.buy_2.status = "filled"
                # Cancel buy 3 per operator rule
                if state.buy_3:
                    state.buy_3.status = "cancelled"
                state.tp_target_price = ladder.tp_price(state.weighted_avg(), self._effective_tp_pct())
                state.state = ladder.ACTIVE_2
                print(f"📥 [paper-ladder] {sym} buy 2 filled @ {state.buy_2.target_price:.6g} "
                      f"avg={state.weighted_avg():.6g} new TP={state.tp_target_price:.6g}; buy 3 cancelled")
                if self.metrics is not None:
                    self.metrics.fill_recorded(sym, state.buy_2.target_price, state.buy_2.qty_filled)
            elif state.buy_3 and state.buy_3.status == "pending" and last <= state.buy_3.target_price:
                # Gap-down: buy 3 fills first
                qty = state.buy_3.size_usdt / max(state.buy_3.target_price, 1e-12)
                state.buy_3.qty_filled = qty * 0.999
                state.buy_3.fill_price = state.buy_3.target_price
                state.buy_3.fill_ts = now_ms
                state.buy_3.status = "filled"
                state.tp_target_price = ladder.tp_price(state.weighted_avg(), self._effective_tp_pct())
                state.hard_stop_price = ladder.hard_stop_price(
                    state.buy_3.target_price, self.ladder_hard_stop_pct
                )
                state.state = ladder.ACTIVE_3
                print(f"📥 [paper-ladder] {sym} buy 3 filled (gap) @ {state.buy_3.target_price:.6g} "
                      f"avg={state.weighted_avg():.6g} hard_stop={state.hard_stop_price:.6g}")
                if self.metrics is not None:
                    self.metrics.fill_recorded(sym, state.buy_3.target_price, state.buy_3.qty_filled)

        elif state.state == ladder.ACTIVE_2:
            if state.buy_3 and state.buy_3.status == "pending" and last <= state.buy_3.target_price:
                # Race condition: cancel didn't beat fill. Honour the fill.
                qty = state.buy_3.size_usdt / max(state.buy_3.target_price, 1e-12)
                state.buy_3.qty_filled = qty * 0.999
                state.buy_3.fill_price = state.buy_3.target_price
                state.buy_3.fill_ts = now_ms
                state.buy_3.status = "filled"
                state.tp_target_price = ladder.tp_price(state.weighted_avg(), self._effective_tp_pct())
                state.hard_stop_price = ladder.hard_stop_price(
                    state.buy_3.target_price, self.ladder_hard_stop_pct
                )
                state.state = ladder.ACTIVE_3
                print(f"📥 [paper-ladder] {sym} buy 3 race-filled @ {state.buy_3.target_price:.6g}")
                if self.metrics is not None:
                    self.metrics.fill_recorded(sym, state.buy_3.target_price, state.buy_3.qty_filled)

        # Underwater tracking for TBE
        if state.filled_legs():
            avg = state.weighted_avg()
            if last < avg:
                if state.below_avg_started_ts == 0:
                    state.below_avg_started_ts = now_ms
            else:
                if state.below_avg_started_ts > 0:
                    state.total_underwater_ms += (now_ms - state.below_avg_started_ts)
                    state.below_avg_started_ts = 0
                    state.recovered_to_break_even = True

        self._paper_ladder_save(state)

    # ── Live-mode per-ladder transitions (real Binance polling) ──────────
    def _ladder_tick_one_live(self, state, last):
        sym = state.symbol
        ccxt_sym = self._to_ccxt_symbol(sym)
        f = self._get_filters(sym)
        if f is None: return
        tick = f.get('tick_size') or 0.00000001
        now_ms = int(time.time() * 1000)

        # 1. PENDING_BUY_1 (slow-path limit): poll Buy 1, then place legs
        if state.state == ladder.PENDING_BUY_1 and state.buy_1 and state.buy_1.order_id:
            try:
                o = self.client.fetch_order(state.buy_1.order_id, ccxt_sym)
            except Exception:
                return
            status = (o.get('status') or '').lower()
            if status in ('canceled', 'cancelled', 'expired'):
                self._handle_external_cancel_live(state, "buy 1")
                return
            if status != 'closed':
                return
            filled_qty = float(o.get('filled') or 0)
            fill_price = float(o.get('average') or o.get('price') or state.buy_1.target_price)
            if filled_qty <= 0:
                self._paper_ladder_clear(sym)
                return
            base_qty = self._round_step(filled_qty * 0.999, f['step_size'])
            state.buy_1.qty_filled = base_qty
            state.buy_1.fill_price = fill_price
            state.buy_1.fill_ts = now_ms
            state.buy_1.status = "filled"
            state.state = ladder.ACTIVE_1
            self._ladder_place_legs_after_buy1_live(state, f, tick)
            self._paper_ladder_save(state)
            print(f"✅ [v-ladder] {sym} buy 1 (limit) filled qty={base_qty} @ {fill_price}")
            if self.metrics is not None:
                self.metrics.fill_recorded(sym, fill_price, base_qty)
            return

        # 2. ACTIVE_*: poll TP and remaining buy orders
        # TP check first
        if state.tp_order_id:
            try:
                tp_o = self.client.fetch_order(state.tp_order_id, ccxt_sym)
                tp_status = (tp_o.get('status') or '').lower()
                if tp_status == 'closed':
                    exit_price = float(tp_o.get('average') or tp_o.get('price') or state.tp_target_price)
                    self._paper_ladder_close(state, exit_price, ladder.EXIT_TP)
                    return
                if tp_status in ('canceled', 'cancelled', 'expired'):
                    self._handle_external_cancel_live(state, "TP")
                    return
            except Exception:
                pass

        # In ACTIVE_1: watch buy 2 and buy 3
        if state.state == ladder.ACTIVE_1:
            if state.buy_2 and state.buy_2.order_id:
                try:
                    o2 = self.client.fetch_order(state.buy_2.order_id, ccxt_sym)
                except Exception:
                    o2 = None
                o2_status = (o2.get('status') or '').lower() if o2 else ''
                if o2_status in ('canceled', 'cancelled', 'expired'):
                    self._handle_external_cancel_live(state, "buy 2")
                    return
                if o2 and o2_status == 'closed':
                    qty = float(o2.get('filled') or 0)
                    price = float(o2.get('average') or o2.get('price') or state.buy_2.target_price)
                    if qty > 0:
                        state.buy_2.qty_filled = self._round_step(qty * 0.999, f['step_size'])
                        state.buy_2.fill_price = price
                        state.buy_2.fill_ts = now_ms
                        state.buy_2.status = "filled"
                        # Cancel buy 3
                        if state.buy_3 and state.buy_3.order_id:
                            try: self.client.cancel_order(state.buy_3.order_id, ccxt_sym)
                            except Exception: pass
                            state.buy_3.status = "cancelled"
                            state.buy_3.order_id = None
                        self._ladder_refresh_tp_live(state, f, tick)
                        state.state = ladder.ACTIVE_2
                        self._paper_ladder_save(state)
                        print(f"📥 [v-ladder] {sym} buy 2 filled @ {price} avg={state.weighted_avg():.6g} "
                              f"new TP={state.tp_target_price:.6g}; buy 3 cancelled")
                        if self.metrics is not None:
                            self.metrics.fill_recorded(sym, price, state.buy_2.qty_filled)
                        return
            # Buy 3 gap-fill check
            if state.buy_3 and state.buy_3.order_id:
                try:
                    o3 = self.client.fetch_order(state.buy_3.order_id, ccxt_sym)
                except Exception:
                    o3 = None
                o3_status = (o3.get('status') or '').lower() if o3 else ''
                if o3_status in ('canceled', 'cancelled', 'expired'):
                    state.buy_3.status = "cancelled"
                    state.buy_3.order_id = None
                    self._paper_ladder_save(state)
                    print(f"⚠️  [v-ladder] {sym} buy 3 cancelled externally; continuing")
                    return
                if o3 and o3_status == 'closed':
                    qty = float(o3.get('filled') or 0)
                    price = float(o3.get('average') or o3.get('price') or state.buy_3.target_price)
                    if qty > 0:
                        state.buy_3.qty_filled = self._round_step(qty * 0.999, f['step_size'])
                        state.buy_3.fill_price = price
                        state.buy_3.fill_ts = now_ms
                        state.buy_3.status = "filled"
                        self._ladder_refresh_tp_live(state, f, tick)
                        state.hard_stop_price = ladder.hard_stop_price(price, self.ladder_hard_stop_pct, tick)
                        state.state = ladder.ACTIVE_3
                        self._paper_ladder_save(state)
                        print(f"📥 [v-ladder] {sym} buy 3 gap-filled @ {price} hard_stop={state.hard_stop_price:.6g}")
                        if self.metrics is not None:
                            self.metrics.fill_recorded(sym, price, state.buy_3.qty_filled)
                        return

        elif state.state == ladder.ACTIVE_3:
            # Hard-stop check using live ticker
            if state.hard_stop_price > 0 and last > 0 and last <= state.hard_stop_price:
                print(f"🛡️ [v-ladder] {sym} HARD STOP @ {last:.6g} threshold={state.hard_stop_price:.6g}")
                if state.tp_order_id:
                    try: self.client.cancel_order(state.tp_order_id, ccxt_sym)
                    except Exception: pass
                qty = state.total_qty()
                try:
                    book = self.client.fetch_order_book(ccxt_sym, limit=5)
                    bid = float(book['bids'][0][0]) if book.get('bids') else last
                    bid = self._round_step(bid, tick)
                    sell = self.client.create_limit_sell_order(ccxt_sym, qty, bid)
                    exit_price = float(sell.get('average') or sell.get('price') or bid)
                except Exception as e:
                    print(f"⚠️ [v-ladder] hard-stop sell failed for {sym}: {e}")
                    exit_price = last
                self._paper_ladder_close(state, exit_price, ladder.EXIT_STOP)
                return

        # Underwater tracking
        if state.filled_legs() and last > 0:
            avg = state.weighted_avg()
            if last < avg:
                if state.below_avg_started_ts == 0:
                    state.below_avg_started_ts = now_ms
            else:
                if state.below_avg_started_ts > 0:
                    state.total_underwater_ms += (now_ms - state.below_avg_started_ts)
                    state.below_avg_started_ts = 0
                    state.recovered_to_break_even = True
        self._paper_ladder_save(state)

    def _effective_tp_pct(self) -> float:
        """Same semantics as Fast Scalper: dollar-target wins when set."""
        if self.ladder_target_net_usdt > 0:
            return ladder.required_tp_pct_for_net_profit(
                self.ladder_buy1_size,
                self.ladder_target_net_usdt,
                self.ladder_fee_rate_per_side,
            )
        return self.ladder_tp_from_avg_pct

    def _ladder_place_tp_live(self, state, qty_total, f, tick):
        """Place a LIMIT SELL at avg × (1 + tp_pct) — real Binance."""
        avg = state.weighted_avg()
        tp_p = ladder.tp_price(avg, self._effective_tp_pct(), tick)
        ccxt_sym = self._to_ccxt_symbol(state.symbol)
        try:
            placed = self.client.create_limit_sell_order(ccxt_sym, qty_total, tp_p)
            state.tp_order_id = str(placed.get('id') or '')
            state.tp_target_price = tp_p
        except Exception as e:
            print(f"⚠️ [v-ladder] {state.symbol} TP placement failed: {e}")
            state.tp_order_id = None
            state.tp_target_price = tp_p

    def _ladder_refresh_tp_live(self, state, f, tick):
        """Cancel old TP + place fresh TP at updated avg."""
        ccxt_sym = self._to_ccxt_symbol(state.symbol)
        if state.tp_order_id:
            try: self.client.cancel_order(state.tp_order_id, ccxt_sym)
            except Exception: pass
        qty_total = state.total_qty()
        self._ladder_place_tp_live(state, qty_total, f, tick)

    def _paper_ladder_close(self, state, exit_price, reason):
        """Finalise a ladder — cancel pending legs, record metrics, clear."""
        # When in live mode, cancel any still-pending Buy 2 / Buy 3 limit
        # orders on Binance so they don't fill after we've already exited.
        if self._is_live() and self.client is not None:
            ccxt_sym = self._to_ccxt_symbol(state.symbol)
            for leg in (state.buy_2, state.buy_3):
                if leg and leg.order_id and leg.status not in ("filled", "cancelled"):
                    try:
                        self.client.cancel_order(leg.order_id, ccxt_sym)
                        leg.status = "cancelled"
                        leg.order_id = None
                        print(f"🚫 [v-ladder] {state.symbol} cancelled pending {leg.label} on close")
                    except Exception:
                        pass

        if state.below_avg_started_ts > 0:
            state.total_underwater_ms += int(time.time() * 1000) - state.below_avg_started_ts
            state.below_avg_started_ts = 0
        state.state = ladder.CLOSED
        state.exit_reason = reason
        state.closed_ts = int(time.time() * 1000)
        summary = ladder.summarise_closed_trade(state, exit_price)

        if self.metrics is not None:
            avg = summary["weighted_avg"]
            net = summary["net_pnl_usdt"]
            if reason == ladder.EXIT_TP:
                self.metrics.tp_hit(state.symbol, avg, exit_price)
            self.metrics.exit_recorded(state.symbol, avg, exit_price,
                                       reason=f"paper_{reason}", pnl_usdt=net)

        try:
            date = datetime.now().strftime("%Y-%m-%d")
            key = f"METRICS:LADDER_PAPER:{date}"
            self.r.lpush(key, json.dumps(summary))
            self.r.ltrim(key, 0, 999)
            self.r.expire(key, 30 * 24 * 3600)
        except Exception:
            pass

        try:
            archive_closed_trade(state.symbol, "VIRTUAL_LADDER", {
                "entry_price": summary["weighted_avg"],
                "exit_price": exit_price,
                "qty": summary["qty"],
                "investment": summary["invested_usdt"],
                "pnl_usdt": summary["net_pnl_usdt"],
                "pnl_pct": (
                    (exit_price - summary["weighted_avg"]) / summary["weighted_avg"] * 100.0
                ) if summary["weighted_avg"] else 0.0,
                "fees_paid": summary["fees_usdt"],
                "reason": f"VIRTUAL_LADDER_{reason.upper()}",
                "exit_time": datetime.now().strftime("%H:%M:%S"),
            })
        except Exception:
            pass

        color = "🟢" if summary["net_pnl_usdt"] > 0 else "🔴"
        print(f"{color}🪜 [paper-ladder] {state.symbol} CLOSED reason={reason} "
              f"net=${summary['net_pnl_usdt']:+.4f} buys_filled={summary['buys_filled']} "
              f"rer_recovered={summary['rer_recovered']} tbe_min={summary['tbe_minutes']:.1f}")
        self._paper_ladder_clear(state.symbol)
        # Per-coin cooldown to prevent immediate re-entry
        ladder.set_cooldown(self.r, state.symbol, self.ladder_cooldown_seconds)

    def passes_falling_knife(self, symbol, timeline):
        """Apply the same 3 falling-knife rules used by the Fast Scalper,
        but using the recent ANALYSIS_020_TIMELINE timeline points instead
        of a Binance ticker. Returns ``(ok, features_dict)``.

        Heuristic: timeline stores per-tick price+volume snapshots. We use
        the last ~60 points as a 1h proxy and the entire timeline as a
        24h proxy. Falls open (returns ok=True) if data is too sparse.

        2026-05-11 iter 9: also enforces the 24h-volume floor (Fast Scalper
        applies this in evaluate_entry; mirroring here for parity)."""
        if not self.fk_enabled or not timeline:
            return True, None

        # 24h volume gate — uses Binance ticker (cached briefly).
        try:
            min_vol = float(self.r.get('TRADING_CONFIG') and __import__('json').loads(self.r.get('TRADING_CONFIG') or '{}').get('minVol24hUsd', 2_000_000) or 2_000_000)
        except Exception:
            min_vol = 2_000_000
        if self.client is not None and min_vol > 0:
            try:
                tk = self.client.fetch_ticker(self._to_ccxt_symbol(symbol))
                vol_24h = float(tk.get('quoteVolume') or 0)
                if vol_24h < min_vol:
                    reason = f"24h vol ${vol_24h/1_000_000:.2f}M < ${min_vol/1_000_000:.0f}M floor"
                    if self.metrics is not None:
                        self.metrics.signal_skipped(symbol, "low_volume_24h", reason,
                                                    {"symbol": symbol, "vol_24h_usd": vol_24h})
                    return False, {"symbol": symbol, "vol_24h_usd": vol_24h, "reason": reason}
            except Exception:
                pass

        try:
            prices = [float(t.get("price", 0) or 0) for t in timeline]
            prices = [p for p in prices if p > 0]
            if len(prices) < 10:
                return True, None

            curr = prices[-1]
            window = prices[-60:] if len(prices) >= 60 else prices
            hi_1h = max(window); lo_1h = min(window)
            range_1h_pct = (hi_1h - lo_1h) / lo_1h * 100 if lo_1h else 0
            change_1h_pct = (curr - window[0]) / window[0] * 100 if window[0] else 0
            origin = prices[0]
            change_24h_pct = (curr - origin) / origin * 100 if origin else 0

            # Pre-signal USDT-volume baseline = avg per-tick (price × vol)
            # over the last 60 ticks. Used by the fast-drop filter to
            # decide whether a quick down-move is real capitulation.
            usdt_vols = []
            for t in timeline[-60:]:
                p = float(t.get("price") or 0); v = float(t.get("volume") or 0)
                if p > 0 and v > 0:
                    usdt_vols.append(p * v)
            pre_vol_baseline_usdt = (sum(usdt_vols) / len(usdt_vols)) if usdt_vols else 0.0

            features = {
                "symbol": symbol, "price": curr,
                "change_24h_pct": change_24h_pct,
                "change_1h_pct": change_1h_pct,
                "range_1h_pct": range_1h_pct,
                "high_1h": hi_1h, "low_1h": lo_1h,
                "pre_vol_baseline_usdt": pre_vol_baseline_usdt,
            }

            if change_24h_pct > self.fk_max_24h:
                reason = f"24h change {change_24h_pct:+.2f}% > {self.fk_max_24h}%"
                if self.metrics is not None:
                    self.metrics.signal_skipped(symbol, "pump_24h", reason, features)
                return False, features

            if range_1h_pct > self.fk_max_1h_range:
                reason = f"1h range {range_1h_pct:.2f}% > {self.fk_max_1h_range}%"
                if self.metrics is not None:
                    self.metrics.signal_skipped(symbol, "volatile_1h", reason, features)
                return False, features

            if (self.fk_overbought_skip and change_24h_pct > 0
                    and change_1h_pct > self.fk_overbought_60m):
                reason = (f"overbought (24h {change_24h_pct:+.2f}% AND "
                          f"60m {change_1h_pct:+.2f}%)")
                if self.metrics is not None:
                    self.metrics.signal_skipped(symbol, "overbought", reason, features)
                return False, features

            return True, features
        except Exception:
            return True, None

    def _fetch_d1_klines(self, symbol):
        """Cached fetch of daily klines (sync). One Binance REST call per
        symbol per 10 minutes. Returns None if data unavailable."""
        if self.client is None:
            return None
        now = time.time()
        cached = self._d1_cache.get(symbol)
        if cached and (now - cached["_ts"]) < self._d1_cache_ttl_sec:
            return cached["data"]
        try:
            ccxt_sym = self._to_ccxt_symbol(symbol)
            limit = self.pp_lookback_days + self.pp_baseline_days + 2
            data = self.client.fetch_ohlcv(ccxt_sym, "1d", limit=limit)
        except Exception:
            return None
        if data:
            self._d1_cache[symbol] = {"_ts": now, "data": data}
        return data

    def _passes_post_pump_filter(self, symbol):
        """Daily-timeframe post-pump-bleed filter (mirrors Fast Scalper).

        The 24h/1h falling-knife filter cannot see multi-day pumps. This
        filter looks at the last (lookback + baseline) daily candles to
        catch coins that pumped hard a few days ago and are now bleeding
        off the peak — JTO is the canonical case (2026-05-12 loss).

        Returns (ok, features_dict). On any data error returns (True, None)
        so a Binance hiccup does not silently disable the filter.
        """
        if not self.pp_enabled:
            return True, None
        data = self._fetch_d1_klines(symbol)
        if not data or len(data) < (self.pp_lookback_days + 2):
            return True, None
        try:
            closes = [float(c[4]) for c in data]
            highs  = [float(c[2]) for c in data]
        except Exception:
            return True, None

        L = self.pp_lookback_days
        B = self.pp_baseline_days
        current_price = closes[-1]
        ma7 = sum(closes[-7:]) / 7 if len(closes) >= 7 else current_price

        pump_window = highs[-(L + 1):-1]
        if not pump_window:
            return True, None
        peak = max(pump_window)
        peak_idx = pump_window.index(peak)
        days_since_peak = (len(pump_window) - 1) - peak_idx

        baseline_slice = closes[-(L + 1 + B):-(L + 1)]
        if not baseline_slice:
            return True, None
        baseline = sum(baseline_slice) / len(baseline_slice)
        if baseline <= 0:
            return True, None

        pump_pct = (peak - baseline) / baseline * 100.0
        off_peak_pct = (peak - current_price) / peak * 100.0 if peak > 0 else 0.0

        features = {
            "filter": "post_pump_bleed",
            "current_price": round(current_price, 8),
            "ma7": round(ma7, 8),
            "peak_14d": round(peak, 8),
            "baseline_7d_pre_pump": round(baseline, 8),
            "pump_pct": round(pump_pct, 2),
            "off_peak_pct": round(off_peak_pct, 2),
            "days_since_peak": int(days_since_peak),
        }

        rejected = (
            pump_pct        >= self.pp_threshold_pct       and
            off_peak_pct    >= self.pp_off_peak_min_pct    and
            current_price   <  ma7                          and
            days_since_peak >= self.pp_min_days_since_peak
        )
        if rejected:
            reason = (f"post-pump bleed: pumped +{pump_pct:.0f}% to {peak:.4f} "
                      f"{days_since_peak}d ago, now {current_price:.4f} "
                      f"(-{off_peak_pct:.1f}% off peak, < MA7 {ma7:.4f})")
            print(f"📉 [{symbol}] virtual skip post_pump_bleed: {reason}")
            if self.metrics is not None:
                self.metrics.signal_skipped(symbol, "post_pump_bleed", reason, features)
            return False, features
        return True, features

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

                # Drive paper-ladder state machines (if enabled). Build a
                # quick {symbol: latest_price} map so each ladder doesn't
                # need to refetch.
                if self.ladder_enabled and ladder is not None:
                    px_map = {}
                    for symbol, data_json in all_analysis.items():
                        try:
                            tl = json.loads(data_json)
                            if tl: px_map[symbol] = float(tl[-1].get("price") or 0)
                        except Exception:
                            pass
                    try:
                        self._paper_ladder_tick(px_map)
                    except Exception as exc:
                        print(f"⚠️ paper-ladder tick failed: {exc}")

                for symbol, data_json in all_analysis.items():
                    timeline = json.loads(data_json)
                    if not timeline: continue

                    last = timeline[-1]
                    signal = last.get('micro_signal', 'NEUTRAL')
                    curr_price = last.get('price', 0)
                    curr_vol = last.get('volume', 0)

                    if signal == 'SCALP_BUY_SIGNAL':
                        # Laddered Recovery (paper or live): multi-coin
                        # gate + 3-tier averaging-down. Falls back to the
                        # legacy single-limit path when ladder mode is off.
                        if self.ladder_enabled and ladder is not None:
                            if ladder.is_active_anywhere(self.r, symbol):
                                pass  # already in flight (either scalper)
                            elif ladder.is_on_cooldown(self.r, symbol):
                                pass  # on per-coin cooldown
                            elif ladder.count_total_active(self.r) >= self.max_concurrent_ladders:
                                pass  # GLOBAL cap reached across both scalpers
                            else:
                                ok, features = self.passes_falling_knife(symbol, timeline)
                                if self.metrics is not None:
                                    self.metrics.signal_evaluated(
                                        symbol, curr_price, features=features,
                                        decision="pass" if ok else "skipped",
                                    )
                                if ok:
                                    # Daily-timeframe post-pump gate (2026-05-12).
                                    pp_ok, _pp_features = self._passes_post_pump_filter(symbol)
                                    if pp_ok:
                                        self._paper_ladder_start(symbol, curr_price, features=features)
                                else:
                                    print(f"🔪 [{symbol}] virtual buy skipped by filter")
                        # Legacy single-limit path: still honoured when
                        # ladder is off.
                        elif not self.r.hexists(VIRTUAL_POSITIONS_KEY, symbol):
                            ok, features = self.passes_falling_knife(symbol, timeline)
                            if self.metrics is not None:
                                self.metrics.signal_evaluated(
                                    symbol, curr_price, features=features,
                                    decision="pass" if ok else "skipped",
                                )
                            if ok:
                                # Daily-timeframe post-pump gate (2026-05-12).
                                pp_ok, _pp_features = self._passes_post_pump_filter(symbol)
                                if pp_ok:
                                    self.place_limit_order(symbol, curr_price, features=features)
                            else:
                                print(f"🔪 [{symbol}] virtual buy skipped by filter")

                    self.monitor_positions(symbol, curr_price, signal, curr_vol)

                time.sleep(1)
            except Exception as e:
                print(f"❌ [VIRTUAL SCALPER ERROR] {e}")
                time.sleep(5)

    def place_limit_order(self, symbol, signal_price, features=None):
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
            # Pre-signal market features that passed the falling-knife filter
            "features": features or {},
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

        Both modes also run the fast-drop pattern check (Pattern C):
        if price falls past the threshold within the detection window
        AND volume isn't surging, cancel the order — we don't want to
        fill into a coin that's bleeding without capitulation.
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

            # Fast-drop check applies to both modes — cancel before fill.
            if self._fast_drop_should_cancel(symbol, pos, elapsed):
                self._cancel_pending_due_to_fast_drop(symbol, pos)
                continue

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

    def _fast_drop_should_cancel(self, symbol, pos, elapsed) -> bool:
        """Return True iff the limit-buy should be cancelled because the
        coin is falling fast WITHOUT a volume surge (Pattern C)."""
        if not self.fd_enabled:
            return False
        if elapsed > self.fd_detect_minutes * 60:
            return False
        baseline = float((pos.get('features') or {}).get('pre_vol_baseline_usdt') or 0)
        if baseline <= 0:
            return False
        signal_price = float(pos.get('signal_price') or 0)
        if signal_price <= 0:
            return False
        curr = self._current_price(symbol)
        if not curr or curr <= 0:
            return False
        drop_pct = (curr - signal_price) / signal_price * 100
        if drop_pct > -self.fd_threshold_pct:
            return False  # not a fast drop yet

        # Volume estimate from the latest analyser tick — same source
        # as _current_price uses, so consistent.
        try:
            raw = self.r.hget(ANALYSIS_020_KEY, symbol.replace('/', ''))
            if not raw:
                return False
            tl = json.loads(raw)
            tail = tl[-12:] if len(tl) >= 12 else tl   # ~last 12 ticks
            tail_usdt = []
            for t in tail:
                p = float(t.get('price') or 0); v = float(t.get('volume') or 0)
                if p > 0 and v > 0:
                    tail_usdt.append(p * v)
            if not tail_usdt:
                return False
            recent_per_tick = sum(tail_usdt) / len(tail_usdt)
        except Exception:
            return False

        ratio = recent_per_tick / baseline if baseline > 0 else 0
        return ratio < self.fd_vol_surge_mult

    def _update_trajectory_metrics(self, symbol, pos, curr_price, curr_vol):
        """Persist running BtmDrop% / MaxRise% / vol-ratio into the
        per-coin OUTCOME hash so the dashboard can render a live
        trajectory. Best-effort (Redis hiccups never crash the loop)."""
        if self.metrics is None or not self.metrics.enabled:
            return
        try:
            entry = float(pos.get('entry_price') or 0)
            if entry <= 0 or curr_price <= 0:
                return
            now_pct = (curr_price - entry) / entry * 100

            from datetime import datetime as _dt
            date = _dt.utcnow().strftime("%Y-%m-%d")
            key = f"METRICS:OUTCOME:{date}:{symbol.replace('/', '')}"
            prev = self.r.hmget(key, 'bottom_pct', 'max_pct', 'pre_vol_baseline_usdt')
            prev_btm = float(prev[0]) if prev[0] else 0.0
            prev_max = float(prev[1]) if prev[1] else 0.0
            baseline = float(prev[2]) if prev[2] else float(
                (pos.get('features') or {}).get('pre_vol_baseline_usdt') or 0
            )

            updates = {
                'now_pct': round(now_pct, 4),
                'last_tick_ts': int(time.time() * 1000),
            }
            if now_pct < prev_btm:
                updates['bottom_pct'] = round(now_pct, 4)
                updates['bottom_ts'] = int(time.time() * 1000)
            if now_pct > prev_max:
                updates['max_pct'] = round(now_pct, 4)
                updates['max_ts'] = int(time.time() * 1000)

            # Vol-1m proxy: most recent tick volume × price.
            if baseline > 0 and curr_vol > 0:
                vol_usdt = curr_price * curr_vol
                updates['vol_1m_usdt'] = round(vol_usdt, 2)
                updates['vol_ratio'] = round(vol_usdt / baseline, 3)
                # Stamp baseline once so trajectory can read it later.
                if not prev[2]:
                    updates['pre_vol_baseline_usdt'] = round(baseline, 2)

            self.r.hset(key, mapping=updates)
            self.r.expire(key, 30 * 24 * 3600)
        except Exception:
            pass

    def _cancel_pending_due_to_fast_drop(self, symbol, pos):
        """Cancel the resting LIMIT (live) and clear the row."""
        order_id = pos.get('order_id')
        ccxt_sym = self._to_ccxt_symbol(symbol)
        if order_id and self.client is not None:
            try:
                self.client.cancel_order(order_id, ccxt_sym)
            except Exception as e:
                print(f"⚠️ [FAST-DROP CANCEL] {symbol} cancel failed: {e}")
        self.r.hdel(VIRTUAL_POSITIONS_KEY, symbol)
        print(f"🔪🩸 [FAST-DROP CANCEL] {symbol} cancelled before fill (no vol surge)")
        if self.metrics is not None:
            self.metrics.signal_skipped(
                symbol, "fast_drop_no_volume",
                "fast drop within detection window without volume surge",
                pos.get('features') or {},
            )

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
            # Per-fill audit log — same as Fast Scalper.
            for i, fl in enumerate(order.get('fills') or order.get('trades') or []):
                fl_qty = fl.get('qty') or fl.get('amount')
                fl_pr  = fl.get('price')
                fl_fee = (fl.get('fee') or {}).get('cost') if isinstance(fl.get('fee'), dict) else fl.get('commission')
                fl_cur = (fl.get('fee') or {}).get('currency') if isinstance(fl.get('fee'), dict) else fl.get('commissionAsset')
                print(f"   fill[{i}] qty={fl_qty} @ {fl_pr} fee={fl_fee} {fl_cur}")
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
        with the real filled quantity. Same fee/qty trap as Fast
        Scalper: Binance's `filled` is GROSS-of-fees. The 0.1 % spot
        fee is taken from the base asset received, so we apply a
        0.999 safety floor (and round down by step_size) before any
        downstream sell uses the qty. Without this the OCO leg would
        be rejected with "insufficient balance".
        """
        if real_filled_qty is not None:
            # Live fill — apply fee-safety floor + step_size rounding.
            fill_price = observed_price
            f = self._get_filters(symbol) if self.client is not None else None
            step = (f or {}).get('step_size', 0.00000001) or 0.00000001
            quantity = self._round_step(real_filled_qty * 0.999, step)
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
        pos['oco_list_id'] = None    # populated below if live + OCO succeeds

        # In live mode, place an OCO sell on Binance so TP/SL fire
        # inside the matching engine — same protection Fast Scalper has.
        # If placement fails (filter, balance, network) we fall back to
        # the polling-based exit logic in monitor_positions.
        if pos.get('mode') == 'live' and self.client is not None and quantity > 0:
            list_id = self._place_oco_sell(symbol, quantity, fill_price, pos.get('profit_pct') or DEFAULT_PROFIT_PCT)
            pos['oco_list_id'] = list_id

        self.r.hset(VIRTUAL_POSITIONS_KEY, symbol, json.dumps(pos))
        signal_price = pos.get('signal_price', fill_price)
        improvement = (signal_price - fill_price) / signal_price * 100 if signal_price else 0
        tag = "LIVE" if pos.get("mode") == "live" else "PAPER"
        oco_tag = f" oco={pos['oco_list_id']}" if pos.get('oco_list_id') else (" oco=FAILED→polling" if pos.get('mode') == 'live' else "")
        print(f"✅ [{tag} FILLED] {symbol} fill={fill_price:.6f} qty={quantity:.6f} "
              f"(-{improvement:.2f}% vs signal {signal_price:.6f}) after {elapsed:.0f}s{oco_tag}")

        if self.metrics is not None:
            order_type = ("virtual_live" if pos.get("mode") == "live" else "virtual_paper")
            self.metrics.buy_placed(symbol, fill_price, pos.get('investment') or 0,
                                    features=pos.get('features') or {},
                                    order_type=order_type,
                                    offset_pct=pos.get('limit_offset_pct') or 0)
            self.metrics.fill_recorded(symbol, fill_price, quantity)

    # ── OCO helpers (mirror the Fast Scalper pattern) ────────────────────

    def _place_oco_sell(self, symbol, qty, entry_price, profit_pct):
        """Place a Binance OCO sell — LIMIT TP + STOP_LOSS_LIMIT SL.

        Returns the orderListId on success, None on any failure (caller
        falls back to polling-based exits).

            TP = entry × (1 + profit_pct / 100)              ← +0.5 % default
            SL_trig = entry × (1 - MAX_VIRTUAL_LOSS_USDT /
                                   pos['investment'])         ← -$1 hard stop
            SL_lim  = SL_trig × 0.998                         ← 0.2 % slack
        """
        if self.client is None:
            return None
        try:
            f = self._get_filters(symbol)
            if not f:
                return None
            tick = f.get('tick_size', 0.00000001) or 0.00000001
            tp_price   = self._round_step(entry_price * (1 + profit_pct / 100.0), tick)
            sl_loss_pct = MAX_VIRTUAL_LOSS_USDT / DEFAULT_BUY_AMOUNT_USDT  # ratio of investment
            sl_trigger = self._round_step(entry_price * (1 - sl_loss_pct), tick)
            sl_limit   = self._round_step(sl_trigger * 0.998, tick)

            if not (tp_price > entry_price > sl_trigger > sl_limit > 0):
                print(f"⚠️ [OCO] {symbol} bad price triplet "
                      f"entry={entry_price} tp={tp_price} sl_trig={sl_trigger} sl_lim={sl_limit} — skipping")
                return None

            params = {
                'symbol':                 symbol.replace('/', ''),
                'side':                   'SELL',
                'quantity':               str(qty),
                'price':                  str(tp_price),
                'stopPrice':              str(sl_trigger),
                'stopLimitPrice':         str(sl_limit),
                'stopLimitTimeInForce':   'GTC',
            }
            result = self.client.private_post_order_oco(params)
            list_id = result.get('orderListId')
            if list_id is None:
                print(f"⚠️ [OCO] {symbol} response missing orderListId: {result}")
                return None
            print(f"📋 [OCO] {symbol} placed list={list_id} TP={tp_price} SL={sl_trigger}/{sl_limit}")
            return str(list_id)
        except Exception as e:
            print(f"⚠️ [OCO] placement failed for {symbol}: {e}")
            return None

    def _check_oco_status(self, symbol, pos):
        """Poll OCO list status. Returns True if the position has been
        closed by the exchange (one leg filled, the other auto-cancelled)."""
        list_id = pos.get('oco_list_id')
        if not list_id or self.client is None:
            return False
        try:
            result = self.client.private_get_orderlist({'orderListId': list_id})
        except Exception as e:
            # Transient — try again next tick.
            return False
        list_status = (result.get('listOrderStatus') or '').upper()
        if list_status != 'ALL_DONE':
            return False

        print(f"✅ [OCO] {symbol} resolved by exchange — list={list_id}")
        # Best-effort archive so dashboards see the close. Compute net
        # PnL from entry→avg-fill via the leg orders if we can.
        try:
            avg_exit_price = pos.get('entry_price') or 0.0
            for leg in result.get('orders', []) or []:
                child = self.client.fetch_order(leg.get('orderId'), self._to_ccxt_symbol(symbol))
                if child.get('status') == 'closed':
                    avg_exit_price = float(child.get('average') or child.get('price') or avg_exit_price)
                    break
            entry_price = float(pos.get('entry_price') or avg_exit_price)
            qty = float(pos.get('quantity') or 0.0)
            buy_value  = entry_price * qty
            sell_value = avg_exit_price * qty
            buy_fee    = buy_value * 0.001
            sell_fee   = sell_value * 0.001
            net_pnl    = (sell_value - buy_value) - (buy_fee + sell_fee)
            archived = dict(pos)
            archived['exit_price'] = avg_exit_price
            archived['pnl_usdt'] = net_pnl
            archived['fees_paid'] = buy_fee + sell_fee
            archived['reason'] = 'OCO_FILLED'
            archive_closed_trade(symbol, "VIRTUAL_LIVE", archived)
        except Exception:
            pass

        self.r.hdel(VIRTUAL_POSITIONS_KEY, symbol)
        return True

    def _cancel_oco(self, symbol, list_id):
        """Best-effort OCO cancel — absorbs failures so the caller can
        always continue to a market sell."""
        if not list_id or self.client is None:
            return
        try:
            self.client.private_delete_orderlist({
                'symbol':       symbol.replace('/', ''),
                'orderListId':  list_id,
            })
            print(f"🚫 [OCO] cancelled list={list_id} for {symbol}")
        except Exception as e:
            print(f"⚠️ [OCO] cancel failed for {symbol} list={list_id}: {e}")

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

        # If we placed an OCO sell at fill time, check it first. When the
        # exchange resolves the OCO (TP or SL leg fills), we drop the
        # local row and skip the polling logic — Binance already exited.
        if pos.get('oco_list_id'):
            if self._check_oco_status(symbol, pos):
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

        # Live trajectory metrics for the dashboard
        self._update_trajectory_metrics(symbol, pos, curr_price, curr_vol)

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

        # 2. Hard Stop Loss (Instant if loss > $1) — DISABLED when
        # stopLossUsdt <= 0 (Option B patient-hold mode).
        elif self.stop_loss_usdt > 0 and pnl_usdt <= -MAX_VIRTUAL_LOSS_USDT:
            exit_reason = "HARD_STOP_LOSS"

        # 3. Strategy Exit (Instant if exhaustion detected)
        elif signal == "EXHAUSTION_EXIT":
            exit_reason = "STRATEGY_EXIT"

        # 4. Soft Stop Loss (Patience Logic: Wait 1h if loss between
        # $0.50 and $1) — also DISABLED when stopLossUsdt <= 0.
        elif self.stop_loss_usdt > 0 and pnl_usdt <= -SOFT_STOP_LOSS_USDT:
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
            # Cancel any active OCO BEFORE the market sell so a TP/SL
            # leg can't fire in parallel and oversell what we own.
            list_id = pos.get('oco_list_id')
            if list_id:
                self._cancel_oco(symbol, list_id)

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

        if self.metrics is not None:
            entry_price = pos.get('entry_price', exit_price)
            if reason == "TAKE_PROFIT":
                latency_min = pos.get('hold_duration', 0) / 60.0
                self.metrics.tp_hit(symbol, entry_price, exit_price, latency_min=latency_min)
            self.metrics.exit_recorded(symbol, entry_price, exit_price,
                                       reason=reason.lower(), pnl_usdt=net_pnl_usdt)

        color = "🟢" if net_pnl_usdt > 0 else "🔴"
        tag = "LIVE" if mode == "live" else "PAPER"
        print(f"{color} [{tag} SELL] {symbol} | Net PnL: ${net_pnl_usdt:.4f} | Fees: ${total_fees:.4f} | Held: {int(pos['hold_duration'])}s")

    def _place_real_market_sell(self, symbol: str, qty: float):
        """Wrapper kept for API compatibility — internally now uses an
        aggressive-limit-at-bid sell instead of a market sell. Returns
        {price, qty} on success, None on failure."""
        return self._place_real_aggressive_limit_sell(symbol, qty, max_wait_sec=5, retries=3)

    def _place_real_aggressive_limit_sell(self, symbol: str, qty: float,
                                          max_wait_sec: int = 5, retries: int = 3):
        """Synchronous version of the aggressive-limit-sell pattern
        (matches Fast Scalper's async helper, including the
        partial-fill safety from the AAVE/PENGU bug).

        Each attempt re-fetches the actual free balance for the base
        asset and caps the order qty at that — so a partial fill on
        attempt N doesn't cause attempt N+1 to be rejected with
        "insufficient balance" (which would orphan the unsold
        remainder in the wallet).
        """
        if self.client is None:
            return None

        ccxt_sym = self._to_ccxt_symbol(symbol)
        base_asset = ccxt_sym.split('/')[0].upper()
        f = self._get_filters(symbol) or {}
        step_size = f.get('step_size') or 0.00000001

        accumulated_filled = 0.0
        last_avg_price = 0.0
        remaining = qty

        for attempt in range(1, retries + 1):
            try:
                # 1) Re-anchor qty to actual wallet balance (handles
                #    partial fills from earlier attempts).
                bal = self.client.fetch_balance()
                free = float((bal.get(base_asset) or {}).get('free') or 0)
                usable = self._round_step(min(remaining, free), step_size)
                if usable <= 0:
                    print(f"✅ [LIMIT-SELL] {symbol} nothing left to sell (free={free:.6f}) — done")
                    break

                book = self.client.fetch_order_book(ccxt_sym, limit=5)
                bids = book.get('bids') or []
                if not bids:
                    print(f"⚠️ [LIMIT-SELL] {symbol} attempt {attempt}: empty bid book — retrying")
                    time.sleep(1)
                    continue
                bid_price = float(bids[0][0])
                print(f"⚡ [VIRTUAL] {symbol} attempt {attempt}/{retries}: LIMIT SELL {usable} @ {bid_price} (free={free:.6f})")
                order = self.client.create_limit_sell_order(ccxt_sym, usable, bid_price)
                order_id = order.get('id')
                if not order_id:
                    print(f"⚠️ [LIMIT-SELL] {symbol} placement returned no order id")
                    continue

                deadline = time.time() + max_wait_sec
                final = None
                while time.time() < deadline:
                    time.sleep(1)
                    o = self.client.fetch_order(order_id, ccxt_sym)
                    if (o.get('status') or '').lower() == 'closed':
                        final = o
                        break

                if final is not None:
                    just_filled = float(final.get('filled') or usable)
                    avg = float(final.get('average') or bid_price)
                    accumulated_filled += just_filled
                    last_avg_price = avg
                    print(f"✅ [LIMIT-SELL] {symbol} filled @ {avg} "
                          f"(this attempt: {just_filled}, total: {accumulated_filled})")
                    return {'price': avg, 'qty': accumulated_filled}

                # Capture any partial fill before cancelling.
                try:
                    last = self.client.fetch_order(order_id, ccxt_sym)
                    partial = float(last.get('filled') or 0)
                    if partial > 0:
                        accumulated_filled += partial
                        last_avg_price = float(last.get('average') or bid_price)
                        remaining = max(0.0, remaining - partial)
                        print(f"⏱  [LIMIT-SELL] {symbol} attempt {attempt} partial-filled {partial}, "
                              f"remaining={remaining}")
                except Exception:
                    pass
                try:
                    self.client.cancel_order(order_id, ccxt_sym)
                except Exception:
                    pass
                print(f"⏱  [LIMIT-SELL] {symbol} attempt {attempt} timed out — cancelling and retrying")
            except Exception as e:
                print(f"⚠️ [LIMIT-SELL] {symbol} attempt {attempt} error: {e}")
                time.sleep(1)

        if accumulated_filled > 0:
            print(f"⚠️ [LIMIT-SELL] {symbol} exhausted retries with partial fills "
                  f"({accumulated_filled} of {qty}) — returning partial result")
            return {'price': last_avg_price, 'qty': accumulated_filled}
        return None

if __name__ == "__main__":
    executor = VirtualScalpExecutor()
    executor.process_signals()
