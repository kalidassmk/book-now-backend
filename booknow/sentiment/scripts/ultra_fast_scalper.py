import asyncio
import os
import time
import pandas as pd
# import ta  <-- Removed to solve ModuleNotFoundError
import logging
import json
import ccxt.async_support as ccxt
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
# from dotenv import load_dotenv  <-- Removed to solve ModuleNotFoundError

from booknow.util.trade_archive import archive_closed_trade

# iter 73/74 — shared pre-buy gates (check-coin + USDT cooldown +
# orderbook depth).  Same pipeline R1/R2/R3/Pattern Bot/PumpRider use.
from pre_buy_gates import run_all_gates as _run_pre_buy_gates

# WebSocket-backed kline cache replaces fetch_ohlcv polling.
# When unavailable (older deployments), fall back to CCXT REST.
try:
    from klines_ws_cache import KlinesCache  # type: ignore
except Exception:
    KlinesCache = None  # type: ignore

# Falling-knife filter (skip top-of-pump buys). Ships with the scalper
# but is import-safe — older deploys without the helper still run.
try:
    from falling_knife_filter import compute_features as _fk_compute_features
    from falling_knife_filter import evaluate as _fk_evaluate
except Exception:
    _fk_compute_features = None  # type: ignore
    _fk_evaluate = None  # type: ignore

# Metrics collector — captures every signal/skip/buy/fill/exit into Redis
# for the new dashboard metrics page.
try:
    from metrics_collector import make_collector
except Exception:
    make_collector = None  # type: ignore

# Laddered Recovery state machine
try:
    import laddered_position as ladder  # type: ignore
except Exception:
    ladder = None  # type: ignore

def manual_load_dotenv(path):
    if not os.path.exists(path): return
    with open(path, 'r') as f:
        for line in f:
            if '=' in line and not line.startswith('#'):
                k, v = line.strip().split('=', 1)
                os.environ[k] = v.strip('"').strip("'")

# ─── LOAD ENVIRONMENT ────────────────────────────────────────────────────────
# Load keys from the shared dashboard .env
dotenv_path = os.path.join(os.path.dirname(__file__), "..", "dashboard", ".env")
manual_load_dotenv(dotenv_path)

API_KEY    = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_SECRET_KEY") # JS uses SECRET_KEY, Python typically SECRET

# ─── CONFIGURATION ───────────────────────────────────────────────────────────
EMA_FAST       = 9
EMA_MID        = 21
EMA_SLOW       = 50
RSI_PERIOD     = 7
VOL_PERIOD     = 20

# Redis Keys
# CONFIG_KEY now points at TRADING_CONFIG — the same key the dashboard
# UI writes to. Previously this was "booknow:config" which lived in
# isolation, so toggling autoBuyEnabled in the UI never affected the
# Fast Scalper. Single source of truth now.
CONFIG_KEY     = "TRADING_CONFIG"
SYMBOL_LIST_KEY = "SYMBOLS:ACTIVE"
SIGNAL_PREFIX  = "SCALPER:SIGNAL:"
POSITIONS_KEY  = "SCALPER:POSITIONS" # Local positions for the scalper

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("MultiScalper")

class MultiSymbolScalper:
    def __init__(self):
        self.client = None
        self.redis = None
        self.is_running = True
        # iter 22: gate trading on a successful sync_config. False until
        # the first strict Redis read populates every required key.
        self.config_loaded: bool = False
        self.symbols = []
        self.active_positions = {} # symbol -> {buy_price, qty}

        # Dynamic Settings (Aligned with User Micro-Trend Strategy)
        # auto_enabled defaults to True — auto buy/sell is the intended
        # operational mode. Explicitly set "autoBuyEnabled": false in
        # booknow:config to pause without restarting the process.
        # 2026-05-12 iter 12: $50/leg, 2-leg ladder. Max $100/ladder.
        # Target $0.15 net per Buy 1 trade.
        self.auto_enabled = True
        self.buy_amount_usdt = 48.0
        self.profit_target_usdt = 0.15
        self.stop_loss_usdt = 0.0

        # Market-context filters (derived from yesterday's P&L analysis).
        # Hot-reloaded from TRADING_CONFIG so thresholds can be tuned
        # without redeploy. See evaluate_entry() for how they're applied.
        self.min_change_24h_pct = -1.0    # skip falling-knife coins (24h trend)
        self.min_range_24h_pct  = 5.0     # skip too-quiet coins (TP unlikely)
        self.min_vol_24h_usd    = 2_000_000  # liquidity floor

        # Limit-buy entry tunables (replaces the old market-buy path).
        # Operator can switch between market and limit by setting
        # limitBuyOffsetPct to 0 (or negative) → market buy fallback.
        # 0.65 % matches the 2026-05-10 Option B backtest sweet spot.
        self.limit_buy_offset_pct = 0.65     # default 0.65 % below signal price
        self.limit_buy_timeout_sec = 60      # cancel if not filled

        # Falling-knife filter (added 2026-05-10). Hot-reloaded from
        # TRADING_CONFIG so the operator can tune live.
        self.fk_enabled = True
        self.fk_max_change_24h_pct = 8.0
        self.fk_max_range_1h_pct = 6.0
        self.fk_overbought_skip = True
        self.fk_overbought_60m_pct = 1.5

        # Post-pump-bleed filter (daily-timeframe, added 2026-05-12 after
        # JTO loss). Catches coins that pumped 30%+ in the last 14 days
        # and are now bleeding off the peak — a multi-day downtrend that
        # the 1m/24h falling-knife filter cannot see. See the algorithm
        # spec in _passes_post_pump_filter().
        self.pp_enabled = True
        self.pp_threshold_pct = 30.0          # pump = +30% over pre-pump baseline
        self.pp_off_peak_min_pct = 10.0       # current ≥ 10% below pump peak
        self.pp_min_days_since_peak = 2       # peak was at least 2 days ago
        # 2026-05-12 iter 13 tuning: 14 → 15 days pump window (operator
        # asked for the upper end of a 10–15 day scan), 7 → 10 days
        # baseline (more stable "normal price" anchor — a 7-day baseline
        # can be skewed by a mini-rally just before the real pump).
        self.pp_lookback_days = 15            # window to scan for pump
        self.pp_baseline_days = 10            # pre-pump baseline window
        # iter 21: MA7 gate is OFF by default — was disabling the filter
        # on real post-pump cases due to single-day bounces above MA7.
        self.pp_require_below_ma7 = False
        self._d1_cache: dict = {}             # symbol -> {data, ts}
        self._d1_cache_ttl_sec = 600          # 10 min — daily candle, fine

        # ── Near-top pump filter (iter 38, 2026-05-15) ───────────────────
        # Catches buys placed within X% of the 24h high after a pump.
        # Defaults: only fires when 24h up ≥ 5% AND price within 2% of
        # 24h high. Both gates required — a +8% / -4% retrace setup is
        # still allowed (healthy pullback). Forensic case: QNT/USDT
        # 2026-05-15 +7.46% / -0.82% from high → would have been
        # blocked. No extra Binance calls — reuses falling-knife
        # features dict already computed at this point in the pipeline.
        self.ntp_enabled = True
        self.ntp_min_24h_change_pct = 5.0
        self.ntp_max_from_high_pct = 2.0

        # ── DIP-BUY mean-reversion entry (iter180, 2026-06-17) ───────────
        # ROOT-CAUSE FIX for ROSEUSDT (-6.6%) and the 18-coin 06-17 basket:
        # the legacy evaluate_entry was a MOMENTUM BREAKOUT strategy — it
        # required `close > prev_high` (buy a fresh high), RSI 60-82
        # (already hot) and stacked up-EMAs. By design it bought strength
        # = bought tops. ROSE was bought on its 02:00 spike top.
        #
        # Operator's redesign (2026-06-17): "price too down → buy the dip,
        # but only AFTER it has fully come down and shows it won't fall
        # again (bottom confirmation), then buy the first up-move."
        #
        # When dip_buy_enabled, evaluate_entry runs the mean-reversion
        # logic below instead. Set dipBuyEnabled=false in TRADING_CONFIG to
        # revert to the old momentum path (kept intact). Thresholds are
        # hot-reloaded via cfg.get (non-strict — missing keys keep these
        # safe defaults so the bot never refuses to trade).
        self.dip_buy_enabled        = True
        self.dip_lookback_bars      = 30     # 1m bars to define the recent swing (30 min)
        self.dip_min_drop_pct       = 1.5    # must be ≥1.5% below the recent-window high
        self.dip_max_drop_pct       = 8.0    # but NOT a free-fall >8% in the window (knife)
        self.dip_rsi_oversold       = 35.0   # RSI must have dipped to/below this (sold off)
        self.dip_max_24h_drop_pct   = 12.0   # reject coins down >12% on 24h (death spiral)
        self.dip_max_24h_pump_pct   = 12.0   # reject coins up >12% on 24h (parabolic/blow-off)
        self.dip_hard_stop_pct      = 2.5    # protective % stop for dip positions (poll path)

        # Fast-drop-without-volume filter (Pattern C). Watches price +
        # volume during the limit-buy wait window and cancels the order
        # if the coin is bleeding without capitulation volume.
        self.fd_enabled = True
        self.fd_detect_minutes = 3
        self.fd_threshold_pct = 0.5
        self.fd_vol_surge_mult = 2.0

        # Trend-reversal exit (EMA-9 < EMA-21). When False the bot won't
        # panic-exit a held position on a momentum reversal — relies on
        # the +1% TP / patient hold strategy. 2026-05-11 P&L analysis
        # showed most "panic exits" hit TP later if held.
        self.trend_reversal_exit_enabled = True

        # Laddered Recovery — 1 ladder concurrent, 2-leg averaging-down.
        # 2026-05-12 iter 15: $48/leg (operator wants higher capture per
        # trade — $96/ladder fits wallet with 3% funds margin).
        self.ladder_enabled = False
        self.max_concurrent_ladders = 1
        self.single_coin_mode = True
        self.ladder_buy1_size = 48.0
        self.ladder_buy2_size = 48.0
        self.ladder_buy3_size = 0.0       # Buy 3 disabled
        self.ladder_buy2_offset_pct = 0.5
        self.ladder_buy3_offset_pct = 1.0
        self.ladder_tp_from_avg_pct = 0.6
        self.ladder_target_net_usdt = 0.15      # iter 42: 0.20→0.15 (faster TP)
        self.ladder_fee_rate_per_side = 0.00075 # 0.075% (BNB discount)
        self.ladder_hard_stop_pct = 1.0
        self.ladder_buy1_market = True
        self.ladder_buy1_offset_pct = 0.15  # 0.15 % below signal
        self.ladder_cooldown_seconds = 14400   # 4 hours

        # ── Time-based ladder exit (added 2026-05-12 iter 14) ─────────
        # Operator was manually panic-selling underwater positions because
        # the bot has no time-based exit — a ladder could sit underwater
        # indefinitely waiting for TP. That manual panic-sell is the
        # single biggest source of realized loss (not visible in metrics
        # because it shows up as "manual_cancel" → pnl_usdt=0).
        #
        # Strategy: cap the underwater hold time. Two exit paths:
        #   (a) Break-even recovery: as soon as price returns to >=
        #       weighted_avg × (1 + 2× fee_rate) after going underwater,
        #       exit immediately at market. Locks in ~$0 net instead of
        #       waiting hours for full TP.
        #   (b) Hard time exit: after ladder_max_hold_seconds, force-sell
        #       at market regardless of price. Caps maximum hold time.
        self.ladder_time_exit_enabled = True
        self.ladder_max_hold_seconds = 14400        # 4h max hold
        self.ladder_breakeven_exit_enabled = True
        self.ladder_breakeven_buffer_pct = 0.50     # iter 36: 0.20→0.50 covers fees (0.15%) + slippage (~0.30%)

        # ── Hard stop from avg (iter 37, 2026-05-15) ───────────────────
        # Caps worst-case realised loss per ladder. Existing hard stop
        # only fires in ACTIVE_3 (Buy 3 filled), but Buy 3 is disabled
        # so it never engages. With $96 deployed and no floor, a
        # downtrending ladder bleeds until break-even-recovery or
        # time-exit catches it — both can lock in heavy losses.
        # 1.5% from avg ⇒ ~$1.50 worst-case on full $96 ladder.
        # Iter 39 (2026-05-15): kept as a SECONDARY safety net beneath
        # the smarter liquidity-death exit — fires only when the smart
        # exit is disabled or its catastrophic floor (2.5%) hasn't yet
        # been hit. See ``_check_liquidity_death`` below.
        self.ladder_hard_stop_from_avg_enabled = True
        self.ladder_hard_stop_from_avg_pct = 1.5

        # ── Liquidity-Death adaptive exit (iter 39, 2026-05-15) ────────
        # Replaces "fixed price drop = sell" with a multi-factor
        # detector. Strongest predictor of "won't bounce" from the
        # QNT/HBAR/FLOKI forensic was 5-min volume / pre-signal-vol
        # baseline. Defaults below capture that with three tiers:
        #   • Catastrophic (drop ≥ 2.5%)            — instant exit
        #   • Liquidity-death score (drop ≥ 0.3 +)  — exit on factor combo
        #   • Stagnation (held ≥ 60min, dead vol)   — exit dead capital
        self.ld_enabled = True
        self.ld_catastrophic_drop_pct = 2.5
        self.ld_min_hold_min = 10
        self.ld_min_drop_pct = 0.3
        self.ld_lookback_min = 10
        self.ld_vol_collapse_threshold = 0.7
        self.ld_lower_lows_threshold = 0.55
        self.ld_red_share_threshold = 0.60
        self.ld_exit_score_threshold = 6
        self.ld_stagnation_hold_min = 60
        self.ld_stagnation_max_drop_pct = 1.0

        # ── Post-Buy-2 Careful Monitor (iter 41, 2026-05-15) ────────────
        # When Buy 2 fills, switch to a specialised monitor with a grace
        # period, tighter break-even, quick-profit lock-in, and a tighter
        # hard stop. Avoids panic-selling right after averaging down,
        # but caps worst case more aggressively than ACTIVE_1.
        self.a2_enabled = True
        self.a2_grace_minutes = 5
        self.a2_quick_profit_pct = 0.2
        self.a2_tight_breakeven_buffer_pct = 0.15
        self.a2_patience_minutes = 20
        self.a2_no_recovery_drop_pct = 0.5
        self.a2_hard_stop_pct = 1.5

        # ── Volatility-Adaptive Entry + TP (iter 43, 2026-05-15) ────────
        # At signal time, pick Buy 1/2 offsets and TP target based on
        # range_1h_pct from features. Catches the "we bought the top"
        # pattern by demanding a deeper entry on volatile coins.
        self.ae_enabled = True
        self.ae_tier_calm_max = 1.0
        self.ae_tier_normal_max = 2.0
        self.ae_tier_volatile_max = 4.0
        self.ae_buy1_calm = 0.15
        self.ae_buy1_normal = 0.30
        self.ae_buy1_volatile = 0.70
        self.ae_buy1_xvolatile = 1.50
        self.ae_buy2_calm = 0.50
        self.ae_buy2_normal = 0.80
        self.ae_buy2_volatile = 1.50
        self.ae_buy2_xvolatile = 2.50
        self.ae_tp_calm = 0.15
        self.ae_tp_normal = 0.20
        self.ae_tp_volatile = 0.30
        self.ae_tp_xvolatile = 0.50

        # ── Macro-Top Exhaustion Filter (iter 44, 2026-05-15) ───────────
        # Catches PENDLE-style top-of-pump entries that the existing
        # post-pump-bleed filter misses (because the coin hasn't crashed
        # yet, just showing distribution). All 3 must fire to SKIP:
        #   30d return >= X% AND within Y% of 30d high AND red_days >= Z
        self.mt_enabled = True
        self.mt_min_return_pct = 50.0
        self.mt_within_high_pct = 90.0
        self.mt_min_red_days_in_7 = 3

        # ── Volatility Regime Filter (iter 45, 2026-05-15) ──────────────
        # Catches the MLN pattern — recently crashed coin in extreme
        # volatility regime. Blocks if max 5d daily range > 20% OR any
        # day in last 5 was a ≥-15% crash.
        self.vr_enabled = True
        self.vr_max_daily_range_pct = 20.0
        self.vr_big_crash_pct = 15.0
        self.vr_lookback_days = 5

        # ── Market Stress Exit (iter 46, 2026-05-15) ────────────────────
        # Earlier mid-trade exit than iter39's 2.5% catastrophic when
        # broader market signals confirm a downward drift. Targets the
        # LINK pattern (BTC-led drop with volume capitulation).
        self.ms_enabled = True
        self.ms_min_hold_min = 30
        self.ms_min_drop_pct = 1.0   # iter 46 tuned: 0.5 → 1.0 (no false positives)
        self.ms_btc_weakness_pct = 0.5
        self.ms_btc_lookback_min = 30
        self.ms_vol_spike_mult = 5.0
        self.ms_red_share_threshold = 0.7

        # ── Buy 2 staleness cancel (iter 37, 2026-05-15) ───────────────
        # If Buy 2 LIMIT doesn't fill within N minutes of Buy 1, cancel
        # it. The retrace we were averaging-down for never came, and
        # filling Buy 2 in a sustained downtrend deepens the hole. Quick
        # retraces (FLOKI: 2 min) are winners; slow drifts (HBAR: 21 min)
        # are losers.
        self.ladder_buy2_staleness_enabled = True
        self.ladder_buy2_staleness_minutes = 10

        # Trailing-TP (iter 15) — once static TP target is reached we
        # cancel the limit TP and trail the running peak. Sells when price
        # retraces by ladder_trailing_tp_pct% from peak. Captures bigger
        # moves than the fixed TP could.
        self.ladder_trailing_tp_enabled = True
        self.ladder_trailing_tp_pct = 0.5

        # Pending-pump-dump cancel (iter 16, 2026-05-13). Cancels a
        # resting Buy 1 LIMIT when price pumps above limit then crashes
        # back — saves us from filling into a falling knife. Details in
        # _maybe_cancel_pending_on_pump_dump().
        self.pending_pump_dump_enabled = True
        self.pending_pump_threshold_pct = 0.5
        self.pending_dump_from_peak_pct = 0.5
        self.pending_min_age_seconds = 60

        # Metrics collector — bound after Redis connects.
        self.metrics = None

        # Filter cache
        self.filters = {} # symbol -> {step_size, tick_size, min_notional}

        # WebSocket-backed kline buffer. One multiplexed connection feeds
        # 1m candles for every active symbol; reads are O(1) and free.
        # Cold-start (first few seconds) falls back to CCXT REST.
        self.klines_cache = (
            KlinesCache(intervals=("1m",), buffer_size=120)
            if KlinesCache is not None else None
        )
        self._klines_cache_started = False

    async def initialize(self):
        log.info("🚀 Initializing Multi-Symbol Scalper Engine...")
        
        # 1. Redis Connection
        try:
            import redis
            self.redis = redis.Redis(
                host=os.getenv("REDIS_HOST", "127.0.0.1"),
                port=int(os.getenv("REDIS_PORT", "6379")),
                decode_responses=True,
            )
            self.redis.ping()
            log.info("🔗 Connected to Redis.")
        except Exception as e:
            log.error(f"❌ Redis Connection Failed: {e}")

        # Metrics collector — uses the same Redis as everything else.
        if make_collector is not None and self.redis is not None:
            self.metrics = make_collector(self.redis, enabled=True)
            log.info("📈 Metrics collector enabled.")

        # 2. Fetch API Keys (Redis Priority -> Env Fallback)
        redis_key = None
        redis_secret = None
        if self.redis:
            try:
                redis_key = self.redis.get("BINANCE_API_KEY")
                redis_secret = self.redis.get("BINANCE_SECRET_KEY")
                if redis_key and redis_secret:
                    log.info("🔑 API Credentials loaded from Redis.")
            except Exception:
                pass

        final_key = redis_key or API_KEY
        final_secret = redis_secret or API_SECRET

        if not final_key or not final_secret:
            log.error("❌ API Keys missing! Check Redis or dashboard/.env file.")
            exit(1)

        self.client = ccxt.binance({
            'apiKey': final_key,
            'secret': final_secret,
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })
        
        # 3. Load Symbols
        await self.refresh_symbols()
        log.info(f"📊 Initialized with {len(self.symbols)} USDT pairs from Redis.")

        # 4. Restore active positions from Redis. Without this, every
        # container restart (deploy/OOM/crash) loses track of coins we
        # actually own — they sit on Binance with no exit logic until
        # someone manually sells. Reconcile each restored row against
        # Binance so we drop entries whose OCO already filled while the
        # bot was offline.
        await self._restore_positions_from_redis()

    # ── Position persistence ─────────────────────────────────────────────
    # active_positions is the in-memory dict the rest of the loop reads
    # and writes. We mirror every change to the SCALPER:POSITIONS hash
    # in Redis so a restart can rebuild the dict from durable storage.

    def _persist_position(self, symbol: str) -> None:
        """Write a single position row to Redis. No-op if redis is down
        or the symbol isn't in the in-memory dict."""
        if not self.redis or symbol not in self.active_positions:
            return
        try:
            self.redis.hset(POSITIONS_KEY, symbol, json.dumps(self.active_positions[symbol]))
        except Exception as e:
            log.warning(f"position persist failed for {symbol}: {e}")

    def _drop_persisted_position(self, symbol: str) -> None:
        """Remove a position row from Redis. Called immediately before
        the in-memory `del` so the two stores never disagree."""
        if not self.redis:
            return
        try:
            self.redis.hdel(POSITIONS_KEY, symbol)
        except Exception as e:
            log.warning(f"position drop failed for {symbol}: {e}")

    async def _restore_positions_from_redis(self) -> None:
        """Rehydrate active_positions on boot. For positions that had
        an OCO list id, query Binance to see if it already resolved —
        any ALL_DONE list means the position is already closed on the
        exchange and the local row should be discarded."""
        if not self.redis:
            return
        try:
            raw = self.redis.hgetall(POSITIONS_KEY)
        except Exception as e:
            log.warning(f"position restore: hgetall failed: {e}")
            return
        if not raw:
            log.info("📦 No persisted positions to restore.")
            return

        restored, dropped = 0, 0
        for symbol, payload in raw.items():
            try:
                pos = json.loads(payload)
            except (TypeError, ValueError):
                continue

            list_id = pos.get('oco_list_id')
            if list_id:
                # If the OCO already resolved while we were down, drop it.
                try:
                    result = await self.client.private_get_orderlist({'orderListId': list_id})
                    if (result.get('listOrderStatus') or '').upper() == 'ALL_DONE':
                        log.info(f"📦 [restore] {symbol} OCO {list_id} already ALL_DONE — dropping")
                        self._drop_persisted_position(symbol)
                        dropped += 1
                        continue
                except Exception as e:
                    # Transient — keep the position and let the monitor
                    # loop figure it out next tick.
                    log.warning(f"[restore] {symbol} OCO check failed (keeping): {e}")

            self.active_positions[symbol] = pos
            restored += 1

        log.info(f"📦 Restored {restored} position(s) from Redis (dropped {dropped} already closed).")

    async def refresh_symbols(self):
        """Fetch the latest Top USDT pairs + Fast Movers from Redis."""
        if not self.redis: return
        try:
            # 1. Base symbols from SYMBOLS:ACTIVE
            raw = self.redis.get(SYMBOL_LIST_KEY)
            base_symbols = []
            if raw:
                base_symbols = json.loads(raw)

            # 2. Add Fast Movers dynamically from various Redis keys
            fast_keys = [
                "FAST_MOVE", "LT2MIN_0>3", "ULTRA_FAST0>2", "ULTRA_FAST2>3",
                "ULTRA_FAST0>3", "ULTRA_FAST>3<5", "SUPER_FAST>2<3", "ULTRA_SUPER_FAST>5<7"
            ]
            fast_movers = set()
            for key in fast_keys:
                try:
                    # These are usually hashes
                    fm = self.redis.hkeys(key)
                    if fm:
                        for s in fm:
                            # Convert to standard format (e.g. BTCUSDT -> BTC/USDT)
                            if s.endswith("USDT"):
                                fast_movers.add(f"{s[:-4]}/{s[-4:]}")
                            else:
                                fast_movers.add(s)
                except Exception:
                    pass

            # Combine and deduplicate
            combined = list(set(base_symbols) | fast_movers)
            
            # CCXT uses slashes (BTC/USDT)
            self.symbols = [s if "/" in s else f"{s[:-4]}/{s[-4:]}" for s in combined]
            
            # Pre-fetch filters for new symbols
            for sym in self.symbols[:100]: # Fetch filters for top 100
                if sym not in self.filters:
                    await self.fetch_filters(sym)
                    
            log.info(f"📊 Symbol Refresh: {len(self.symbols)} pairs ({len(fast_movers)} from Fast Scan)")
        except Exception as e:
            log.error(f"⚠️ Symbol refresh failed: {e}")

    async def fetch_filters(self, symbol):
        """Fetch trading rules for a symbol."""
        try:
            # In CCXT, load markets first
            if not self.client.markets:
                await self.client.load_markets()
            
            market = self.client.market(symbol)
            if not market: return
            
            self.filters[symbol] = {
                "step_size": float(market['limits']['amount']['min'] or 0.0001),
                "tick_size": float(market['precision']['price'] or 0.00001),
                "min_notional": float(market['limits']['cost']['min'] or 10.0)
            }
        except Exception:
            pass

    # iter 22 (2026-05-13): list of every config key the bot needs from
    # Redis. sync_config refuses to populate self.* unless ALL of these
    # are present. Operator's hard rule: "always read from Redis, never
    # use anything in memory" — the bot will not trade if any value is
    # missing rather than falling back to a hardcoded stale default
    # (which is what caused the NEIRO/USDT TP-at-+0.29% bug).
    _REQUIRED_CONFIG_KEYS = (
        "autoBuyEnabled", "buyAmountUsdt", "profitPct", "profitAmountUsdt",
        "stopLossUsdt",
        "minChange24hPct", "minRange24hPct", "minVol24hUsd",
        "limitBuyOffsetPct", "limitBuyTimeoutSec",
        "fallingKnifeFilterEnabled", "maxChange24hPct", "maxRange1hPct",
        "overboughtSkipEnabled", "overbought60mPct",
        "postPumpFilterEnabled", "postPumpThresholdPct",
        "postPumpOffPeakMinPct", "postPumpMinDaysSincePeak",
        "postPumpLookbackDays", "postPumpBaselineDays",
        "postPumpRequireBelowMa7",
        "fastDropFilterEnabled", "fastDropDetectMinutes",
        "fastDropThresholdPct", "volSurgeThresholdMultiplier",
        "trendReversalExitEnabled",
        "ladderedRecoveryEnabled", "maxConcurrentLadders",
        "singleCoinModeEnabled",
        "ladderBuy1SizeUsdt", "ladderBuy2SizeUsdt", "ladderBuy3SizeUsdt",
        "ladderBuy2OffsetPct", "ladderBuy3OffsetPct",
        "ladderTpFromAvgPct", "ladderTargetNetProfitUsdt",
        "ladderFeeRatePerSide", "ladderHardStopBelowBuy3Pct",
        "ladderBuy1UseMarketOrder", "ladderBuy1OffsetPct",
        "ladderCooldownSeconds",
        "ladderTimeExitEnabled", "ladderMaxHoldSeconds",
        "ladderBreakevenExitEnabled", "ladderBreakevenBufferPct",
        # iter 37 (2026-05-15)
        "ladderHardStopFromAvgEnabled", "ladderHardStopFromAvgPct",
        "ladderBuy2StalenessEnabled", "ladderBuy2StalenessMinutes",
        "ladderTrailingTpEnabled", "ladderTrailingTpPct",
        "pendingPumpDumpCancelEnabled", "pendingPumpThresholdPct",
        "pendingDumpFromPeakPct", "pendingMinAgeSeconds",
        # iter 38 (2026-05-15)
        "nearTopPumpFilterEnabled", "nearTopPumpMin24hChangePct",
        "nearTopPumpMaxFromHighPct",
        # iter 39 (2026-05-15) — liquidity-death adaptive exit
        "liquidityDeathExitEnabled", "liquidityDeathCatastrophicDropPct",
        "liquidityDeathMinHoldMin", "liquidityDeathMinDropPct",
        "liquidityDeathLookbackMin", "liquidityDeathVolCollapseThreshold",
        "liquidityDeathLowerLowsThreshold", "liquidityDeathRedShareThreshold",
        "liquidityDeathExitScoreThreshold", "liquidityDeathStagnationHoldMin",
        "liquidityDeathStagnationMaxDropPct",
        # iter 41 (2026-05-15) — Post-Buy-2 careful monitor
        "active2MonitorEnabled", "active2GracePeriodMinutes",
        "active2QuickProfitPct", "active2TightBreakevenBufferPct",
        "active2PatienceMinutes", "active2NoRecoveryDropPct",
        "active2HardStopPct",
        # iter 43 (2026-05-15) — Volatility-Adaptive Entry + TP
        "adaptiveEntryEnabled",
        "adaptiveTierCalmMaxPct", "adaptiveTierNormalMaxPct", "adaptiveTierVolatileMaxPct",
        "adaptiveBuy1OffsetCalm", "adaptiveBuy1OffsetNormal",
        "adaptiveBuy1OffsetVolatile", "adaptiveBuy1OffsetXVolatile",
        "adaptiveBuy2OffsetCalm", "adaptiveBuy2OffsetNormal",
        "adaptiveBuy2OffsetVolatile", "adaptiveBuy2OffsetXVolatile",
        "adaptiveTpTargetCalm", "adaptiveTpTargetNormal",
        "adaptiveTpTargetVolatile", "adaptiveTpTargetXVolatile",
        # iter 44 — Macro-Top Exhaustion Filter
        "macroTopFilterEnabled", "macroTopMinReturnPct",
        "macroTopWithinHighPct", "macroTopMinRedDaysIn7",
        # iter 45 — Volatility Regime Filter
        "volRegimeFilterEnabled", "volRegimeMaxDailyRangePct",
        "volRegimeBigCrashPct", "volRegimeLookbackDays",
        # iter 46 — Market Stress Exit
        "marketStressExitEnabled", "marketStressMinHoldMin",
        "marketStressMinDropPct", "marketStressBtcWeaknessPct",
        "marketStressBtcLookbackMin", "marketStressVolSpikeMult",
        "marketStressRedShareThreshold",
        "metricsEnabled",
    )

    async def sync_config(self):
        """STRICT Redis-only read of every config field.

        iter 22 design rules (operator request):
          - Read fresh from Redis on every loop iteration (no caching of
            stale memory across cycles).
          - NO fallback defaults — if Redis returns nothing, or the JSON
            is corrupted, or any required key is missing, raise.
          - Caller (the main loop) catches the exception, logs it,
            clears the `config_loaded` flag, and skips the cycle. The
            bot will not place ANY new orders until Redis is healthy.

        Why so strict: the prior `cfg.get(key, hardcoded_default)` style
        let stale code defaults silently leak into live trading whenever
        a Redis read returned None or the container's in-memory state
        diverged from Redis. That's what caused NEIRO/USDT 2026-05-13
        to place a TP at +0.29% when current Redis said +0.567%. With
        strict mode any misconfiguration fails LOUDLY instead of
        silently mis-sizing trades.
        """
        if not self.redis:
            self.config_loaded = False
            raise RuntimeError("sync_config called before Redis connected")
        raw = self.redis.get(CONFIG_KEY)
        if not raw:
            self.config_loaded = False
            raise RuntimeError(f"TRADING_CONFIG key missing in Redis — bot refuses to trade")
        cfg = json.loads(raw)  # raises on corrupted JSON — caller catches
        missing = [k for k in self._REQUIRED_CONFIG_KEYS if k not in cfg]
        if missing:
            self.config_loaded = False
            raise RuntimeError(f"TRADING_CONFIG missing required keys: {missing}")

        # All required keys present — populate state. Direct cfg[key]
        # access intentionally (no .get with defaults).
        self.auto_enabled    = bool(cfg["autoBuyEnabled"])
        self.buy_amount_usdt = float(cfg["buyAmountUsdt"])

        # Profit target: profitPct wins when > 0, else fall through to
        # explicit USDT. Both are required keys so an operator-set "0"
        # is a deliberate choice, not a missing field.
        profit_pct = float(cfg["profitPct"])
        if profit_pct > 0:
            self.profit_target_usdt = self.buy_amount_usdt * profit_pct / 100.0
        else:
            self.profit_target_usdt = float(cfg["profitAmountUsdt"])

        self.stop_loss_usdt = float(cfg["stopLossUsdt"])
        self.min_change_24h_pct = float(cfg["minChange24hPct"])
        self.min_range_24h_pct  = float(cfg["minRange24hPct"])
        self.min_vol_24h_usd    = float(cfg["minVol24hUsd"])
        self.limit_buy_offset_pct = float(cfg["limitBuyOffsetPct"])
        self.limit_buy_timeout_sec = int(cfg["limitBuyTimeoutSec"])

        # iter180 DIP-BUY overrides — NON-STRICT (cfg.get) so a frontend
        # that hasn't shipped these keys yet keeps the safe __init__
        # defaults instead of the bot refusing to trade.
        def _cfg_bool(key, cur):
            v = cfg.get(key)
            return bool(v) if v is not None else cur
        def _cfg_float(key, cur):
            v = cfg.get(key)
            try:
                return float(v) if v is not None else cur
            except (TypeError, ValueError):
                return cur
        def _cfg_int(key, cur):
            v = cfg.get(key)
            try:
                return int(v) if v is not None else cur
            except (TypeError, ValueError):
                return cur
        self.dip_buy_enabled      = _cfg_bool("dipBuyEnabled", self.dip_buy_enabled)
        self.dip_lookback_bars    = _cfg_int("dipLookbackBars", self.dip_lookback_bars)
        self.dip_min_drop_pct     = _cfg_float("dipMinDropPct", self.dip_min_drop_pct)
        self.dip_max_drop_pct     = _cfg_float("dipMaxDropPct", self.dip_max_drop_pct)
        self.dip_rsi_oversold     = _cfg_float("dipRsiOversold", self.dip_rsi_oversold)
        self.dip_max_24h_drop_pct = _cfg_float("dipMax24hDropPct", self.dip_max_24h_drop_pct)
        self.dip_max_24h_pump_pct = _cfg_float("dipMax24hPumpPct", self.dip_max_24h_pump_pct)
        self.dip_hard_stop_pct    = _cfg_float("dipHardStopPct", self.dip_hard_stop_pct)

        self.fk_enabled = bool(cfg["fallingKnifeFilterEnabled"])
        self.fk_max_change_24h_pct = float(cfg["maxChange24hPct"])
        self.fk_max_range_1h_pct = float(cfg["maxRange1hPct"])
        self.fk_overbought_skip = bool(cfg["overboughtSkipEnabled"])
        self.fk_overbought_60m_pct = float(cfg["overbought60mPct"])

        self.pp_enabled = bool(cfg["postPumpFilterEnabled"])
        self.pp_threshold_pct = float(cfg["postPumpThresholdPct"])
        self.pp_off_peak_min_pct = float(cfg["postPumpOffPeakMinPct"])
        self.pp_min_days_since_peak = int(cfg["postPumpMinDaysSincePeak"])
        self.pp_lookback_days = int(cfg["postPumpLookbackDays"])
        self.pp_baseline_days = int(cfg["postPumpBaselineDays"])
        self.pp_require_below_ma7 = bool(cfg["postPumpRequireBelowMa7"])

        self.fd_enabled = bool(cfg["fastDropFilterEnabled"])
        self.fd_detect_minutes = int(cfg["fastDropDetectMinutes"])
        self.fd_threshold_pct = float(cfg["fastDropThresholdPct"])
        self.fd_vol_surge_mult = float(cfg["volSurgeThresholdMultiplier"])

        self.trend_reversal_exit_enabled = bool(cfg["trendReversalExitEnabled"])

        self.ladder_enabled = bool(cfg["ladderedRecoveryEnabled"])
        self.max_concurrent_ladders = int(cfg["maxConcurrentLadders"])
        self.single_coin_mode = bool(cfg["singleCoinModeEnabled"])
        self.ladder_buy1_size = float(cfg["ladderBuy1SizeUsdt"])
        self.ladder_buy2_size = float(cfg["ladderBuy2SizeUsdt"])
        self.ladder_buy3_size = float(cfg["ladderBuy3SizeUsdt"])
        self.ladder_buy2_offset_pct = float(cfg["ladderBuy2OffsetPct"])
        self.ladder_buy3_offset_pct = float(cfg["ladderBuy3OffsetPct"])
        self.ladder_tp_from_avg_pct = float(cfg["ladderTpFromAvgPct"])
        self.ladder_target_net_usdt = float(cfg["ladderTargetNetProfitUsdt"])
        self.ladder_fee_rate_per_side = float(cfg["ladderFeeRatePerSide"])
        self.ladder_hard_stop_pct = float(cfg["ladderHardStopBelowBuy3Pct"])
        self.ladder_buy1_market = bool(cfg["ladderBuy1UseMarketOrder"])
        self.ladder_buy1_offset_pct = float(cfg["ladderBuy1OffsetPct"])
        self.ladder_cooldown_seconds = int(cfg["ladderCooldownSeconds"])
        self.ladder_time_exit_enabled = bool(cfg["ladderTimeExitEnabled"])
        self.ladder_max_hold_seconds = int(cfg["ladderMaxHoldSeconds"])
        self.ladder_breakeven_exit_enabled = bool(cfg["ladderBreakevenExitEnabled"])
        self.ladder_breakeven_buffer_pct = float(cfg["ladderBreakevenBufferPct"])
        # iter 37 (2026-05-15)
        self.ladder_hard_stop_from_avg_enabled = bool(cfg["ladderHardStopFromAvgEnabled"])
        self.ladder_hard_stop_from_avg_pct = float(cfg["ladderHardStopFromAvgPct"])
        self.ladder_buy2_staleness_enabled = bool(cfg["ladderBuy2StalenessEnabled"])
        self.ladder_buy2_staleness_minutes = int(cfg["ladderBuy2StalenessMinutes"])
        # iter 39 (2026-05-15) — liquidity-death adaptive exit
        self.ld_enabled = bool(cfg["liquidityDeathExitEnabled"])
        self.ld_catastrophic_drop_pct = float(cfg["liquidityDeathCatastrophicDropPct"])
        self.ld_min_hold_min = int(cfg["liquidityDeathMinHoldMin"])
        self.ld_min_drop_pct = float(cfg["liquidityDeathMinDropPct"])
        self.ld_lookback_min = int(cfg["liquidityDeathLookbackMin"])
        self.ld_vol_collapse_threshold = float(cfg["liquidityDeathVolCollapseThreshold"])
        self.ld_lower_lows_threshold = float(cfg["liquidityDeathLowerLowsThreshold"])
        self.ld_red_share_threshold = float(cfg["liquidityDeathRedShareThreshold"])
        self.ld_exit_score_threshold = int(cfg["liquidityDeathExitScoreThreshold"])
        self.ld_stagnation_hold_min = int(cfg["liquidityDeathStagnationHoldMin"])
        self.ld_stagnation_max_drop_pct = float(cfg["liquidityDeathStagnationMaxDropPct"])
        # iter 41 (2026-05-15) — Post-Buy-2 careful monitor
        self.a2_enabled = bool(cfg["active2MonitorEnabled"])
        self.a2_grace_minutes = int(cfg["active2GracePeriodMinutes"])
        self.a2_quick_profit_pct = float(cfg["active2QuickProfitPct"])
        self.a2_tight_breakeven_buffer_pct = float(cfg["active2TightBreakevenBufferPct"])
        self.a2_patience_minutes = int(cfg["active2PatienceMinutes"])
        self.a2_no_recovery_drop_pct = float(cfg["active2NoRecoveryDropPct"])
        self.a2_hard_stop_pct = float(cfg["active2HardStopPct"])
        # iter 43 — Volatility-Adaptive Entry + TP
        self.ae_enabled = bool(cfg["adaptiveEntryEnabled"])
        self.ae_tier_calm_max = float(cfg["adaptiveTierCalmMaxPct"])
        self.ae_tier_normal_max = float(cfg["adaptiveTierNormalMaxPct"])
        self.ae_tier_volatile_max = float(cfg["adaptiveTierVolatileMaxPct"])
        self.ae_buy1_calm = float(cfg["adaptiveBuy1OffsetCalm"])
        self.ae_buy1_normal = float(cfg["adaptiveBuy1OffsetNormal"])
        self.ae_buy1_volatile = float(cfg["adaptiveBuy1OffsetVolatile"])
        self.ae_buy1_xvolatile = float(cfg["adaptiveBuy1OffsetXVolatile"])
        self.ae_buy2_calm = float(cfg["adaptiveBuy2OffsetCalm"])
        self.ae_buy2_normal = float(cfg["adaptiveBuy2OffsetNormal"])
        self.ae_buy2_volatile = float(cfg["adaptiveBuy2OffsetVolatile"])
        self.ae_buy2_xvolatile = float(cfg["adaptiveBuy2OffsetXVolatile"])
        self.ae_tp_calm = float(cfg["adaptiveTpTargetCalm"])
        self.ae_tp_normal = float(cfg["adaptiveTpTargetNormal"])
        self.ae_tp_volatile = float(cfg["adaptiveTpTargetVolatile"])
        self.ae_tp_xvolatile = float(cfg["adaptiveTpTargetXVolatile"])
        # iter 44 — Macro-Top Exhaustion Filter
        self.mt_enabled = bool(cfg["macroTopFilterEnabled"])
        self.mt_min_return_pct = float(cfg["macroTopMinReturnPct"])
        self.mt_within_high_pct = float(cfg["macroTopWithinHighPct"])
        self.mt_min_red_days_in_7 = int(cfg["macroTopMinRedDaysIn7"])
        # iter 45 — Volatility Regime Filter
        self.vr_enabled = bool(cfg["volRegimeFilterEnabled"])
        self.vr_max_daily_range_pct = float(cfg["volRegimeMaxDailyRangePct"])
        self.vr_big_crash_pct = float(cfg["volRegimeBigCrashPct"])
        self.vr_lookback_days = int(cfg["volRegimeLookbackDays"])
        # iter 46 — Market Stress Exit
        self.ms_enabled = bool(cfg["marketStressExitEnabled"])
        self.ms_min_hold_min = int(cfg["marketStressMinHoldMin"])
        self.ms_min_drop_pct = float(cfg["marketStressMinDropPct"])
        self.ms_btc_weakness_pct = float(cfg["marketStressBtcWeaknessPct"])
        self.ms_btc_lookback_min = int(cfg["marketStressBtcLookbackMin"])
        self.ms_vol_spike_mult = float(cfg["marketStressVolSpikeMult"])
        self.ms_red_share_threshold = float(cfg["marketStressRedShareThreshold"])
        self.ladder_trailing_tp_enabled = bool(cfg["ladderTrailingTpEnabled"])
        self.ladder_trailing_tp_pct = float(cfg["ladderTrailingTpPct"])
        self.pending_pump_dump_enabled = bool(cfg["pendingPumpDumpCancelEnabled"])
        self.pending_pump_threshold_pct = float(cfg["pendingPumpThresholdPct"])
        self.pending_dump_from_peak_pct = float(cfg["pendingDumpFromPeakPct"])
        self.pending_min_age_seconds = int(cfg["pendingMinAgeSeconds"])

        # iter 38 (2026-05-15): near-top pump filter
        self.ntp_enabled = bool(cfg["nearTopPumpFilterEnabled"])
        self.ntp_min_24h_change_pct = float(cfg["nearTopPumpMin24hChangePct"])
        self.ntp_max_from_high_pct = float(cfg["nearTopPumpMaxFromHighPct"])

        if self.metrics is not None:
            self.metrics.enabled = bool(cfg["metricsEnabled"])

        self.config_loaded = True

    def _update_trajectory_metrics(self, symbol, pos, curr_row):
        """Persist post-fill trajectory features to METRICS:OUTCOME so the
        dashboard can render a live BtmDrop% / MaxRise% / Vol1m-ratio per
        coin. Best-effort — silently no-ops if metrics are off or Redis
        misbehaves."""
        if self.metrics is None or not self.metrics.enabled or not self.redis:
            return
        try:
            buy_price = float(pos.get('buy_price') or 0)
            if buy_price <= 0:
                return
            high = float(curr_row.get('high', curr_row.get('close')) or 0)
            low = float(curr_row.get('low', curr_row.get('close')) or 0)
            close = float(curr_row.get('close') or 0)
            vol_base = float(curr_row.get('vol') or 0)

            from datetime import datetime as _dt
            date = _dt.utcnow().strftime("%Y-%m-%d")
            outcome_key = f"METRICS:OUTCOME:{date}:{symbol.replace('/', '')}"

            # Read existing extrema; only widen them.
            prev = self.redis.hmget(outcome_key, 'bottom_pct', 'max_pct', 'pre_vol_baseline_usdt')
            prev_btm = float(prev[0]) if prev[0] else 0.0
            prev_max = float(prev[1]) if prev[1] else 0.0
            pre_baseline = float(prev[2]) if prev[2] else 0.0

            new_btm = (low - buy_price) / buy_price * 100 if low > 0 else prev_btm
            new_max = (high - buy_price) / buy_price * 100 if high > 0 else prev_max
            now_pct = (close - buy_price) / buy_price * 100 if close > 0 else 0

            updates = {
                'now_pct':  round(now_pct, 4),
                'now_price': round(close, 10),       # NEW: live price for dashboard
                'last_tick_ts': int(time.time() * 1000),
            }
            if low > 0 and new_btm < prev_btm:
                updates['bottom_pct'] = round(new_btm, 4)
                updates['bottom_ts'] = int(time.time() * 1000)
                updates['lowest_since_buy'] = round(low, 10)   # NEW: absolute price
            if high > 0 and new_max > prev_max:
                updates['max_pct'] = round(new_max, 4)
                updates['max_ts'] = int(time.time() * 1000)
                updates['highest_since_buy'] = round(high, 10)  # NEW: absolute price

            # Vol-1m / pre-baseline ratio (proxy for capitulation strength).
            if pre_baseline > 0 and close > 0 and vol_base > 0:
                vol_usdt = close * vol_base
                ratio = vol_usdt / pre_baseline
                updates['vol_1m_usdt'] = round(vol_usdt, 2)
                updates['vol_ratio'] = round(ratio, 3)

            self.redis.hset(outcome_key, mapping=updates)
            # Keep TTL aligned with metrics_collector's 30-day window.
            self.redis.expire(outcome_key, 30 * 24 * 3600)
        except Exception as exc:
            log.debug(f"trajectory metrics update failed for {symbol}: {exc}")

    async def passes_falling_knife(self, symbol):
        """Returns (ok, features_dict). When ``ok`` is False the buy must
        be skipped — the filter found a top-of-pump / overbought / volatile
        setup. On any error we fall back to ``ok=True`` to avoid a Binance
        hiccup silently disabling the filter."""
        if _fk_compute_features is None or _fk_evaluate is None:
            return True, None
        if not self.fk_enabled:
            return True, None
        try:
            features = await _fk_compute_features(self.client, symbol)
            if features is None:
                return True, None
            verdict = _fk_evaluate(
                features,
                enabled=True,
                max_change_24h_pct=self.fk_max_change_24h_pct,
                max_range_1h_pct=self.fk_max_range_1h_pct,
                overbought_skip=self.fk_overbought_skip,
                overbought_60m_pct=self.fk_overbought_60m_pct,
            )
            if not verdict.passed:
                log.info(f"🔪 [{symbol}] skipped by filter ({verdict.rule}): {verdict.reason}")
                if self.metrics is not None:
                    self.metrics.signal_skipped(symbol, verdict.rule, verdict.reason,
                                                features.to_dict())
                return False, features.to_dict()
            return True, features.to_dict()
        except Exception as exc:
            log.debug(f"falling_knife fetch failed for {symbol}: {exc}")
            return True, None

    def _passes_near_top_pump_filter(self, symbol: str, features: dict):
        """Iter 38 near-top pump gate. Reuses features already computed
        by ``passes_falling_knife`` — zero extra Binance calls.

        Pattern from QNT/USDT 2026-05-15 forensic:
          • 24h change      = +7.46%   (under fallingKnife maxChange24hPct=12)
          • from 24h high   = -0.82%   (we bought 0.82% below the day's peak)
          • Bought at $78.17 — outcome: unrealised loss, panic-cancelled.

        Logic: BOTH conditions must be true for the SKIP to fire:
          1. ``change_24h_pct >= ntp_min_24h_change_pct``
          2. ``from_24h_high_pct >= -ntp_max_from_high_pct``  (i.e.
             current price is within ntp_max_from_high_pct % of the
             24h high — no healthy retrace yet).

        Returns True if the buy is allowed, False if it must be skipped.
        On missing/None features, returns True (cannot decide → defer to
        other gates).
        """
        if not self.ntp_enabled:
            return True
        if not features:
            return True
        try:
            change_24h = float(features.get("change_24h_pct") or 0.0)
            from_high = float(features.get("from_24h_high_pct") or 0.0)
        except (TypeError, ValueError):
            return True

        first_gate = change_24h >= self.ntp_min_24h_change_pct
        # from_high is typically negative (e.g. -0.82% = 0.82% below
        # 24h peak). "Within max_from_high_pct of the peak" means
        # from_high >= -max_from_high_pct.
        second_gate = from_high >= -self.ntp_max_from_high_pct
        if first_gate and second_gate:
            reason = (
                f"near top of 24h pump: 24h_change=+{change_24h:.2f}% "
                f"(≥ {self.ntp_min_24h_change_pct:.1f}%) AND "
                f"from_24h_high={from_high:.2f}% "
                f"(within {self.ntp_max_from_high_pct:.1f}% of peak)"
            )
            log.info(f"🚧 [{symbol}] skipped by near_top_pump: {reason}")
            if self.metrics is not None:
                try:
                    self.metrics.signal_skipped(
                        symbol, "near_top_pump", reason, features,
                    )
                except Exception:
                    pass
            return False
        return True

    async def _passes_macro_top_filter(self, symbol: str):
        """iter 44 (2026-05-15) — Macro-Top Exhaustion Filter.

        Catches the PENDLE/USDT 2026-05-14 pattern: a coin that rallied
        massively (+84% in 30d) is currently near the top of that rally
        (within ~7% of 30d high), and is showing 4 red days in last 7
        (distribution). The existing post-pump-bleed filter misses this
        because it requires the coin to ALREADY have crashed off-peak.

        Reuses the daily-kline cache (10 min TTL). All 3 must fire:
          • 30d_return  >= mt_min_return_pct
          • buy_price / 30d_high * 100  >= mt_within_high_pct
          • red_days_in_last_7  >= mt_min_red_days_in_7

        Returns (True, None) when the buy is allowed.
        Returns (False, features_dict) when the buy must be skipped.
        On any kline-fetch error returns (True, None) — fail open.
        """
        if not self.mt_enabled:
            return True, None
        candles = await self._fetch_d1_klines(symbol)
        if not candles or len(candles) < 8:
            return True, None  # not enough data → fail open
        # Use last 30 candles (excluding any partial today candle)
        candles = candles[-30:]
        try:
            closes = [float(k[4]) for k in candles]
            opens  = [float(k[1]) for k in candles]
            highs  = [float(k[2]) for k in candles]
        except Exception:
            return True, None
        if closes[0] <= 0:
            return True, None
        last_close = closes[-1]
        h30 = max(highs)
        ret30 = (last_close / closes[0] - 1) * 100
        within_hi = (last_close / h30) * 100 if h30 > 0 else 0
        red7 = sum(1 for o, cl in zip(opens[-7:], closes[-7:]) if cl < o)

        block = (ret30 >= self.mt_min_return_pct
                 and within_hi >= self.mt_within_high_pct
                 and red7 >= self.mt_min_red_days_in_7)
        if not block:
            return True, None

        reason = (f"macro-top exhaustion: 30d_return={ret30:+.1f}% "
                  f"(>= {self.mt_min_return_pct}%) AND "
                  f"within_30d_high={within_hi:.1f}% "
                  f"(>= {self.mt_within_high_pct}%) AND "
                  f"red_days_in_7={red7} (>= {self.mt_min_red_days_in_7})")
        feats = {
            "filter": "macro_top",
            "30d_return_pct": round(ret30, 2),
            "30d_high": h30,
            "within_30d_high_pct": round(within_hi, 2),
            "red_days_in_7": red7,
            "last_close": last_close,
        }
        log.info(f"🪦 [{symbol}] skipped by macro_top: {reason}")
        if self.metrics is not None:
            try:
                self.metrics.signal_skipped(symbol, "macro_top", reason, feats)
            except Exception:
                pass
        return False, feats

    async def _passes_vol_regime_filter(self, symbol: str):
        """iter 45 (2026-05-15) — Volatility Regime Filter.

        Catches the MLN/USDT 2026-05-15 pattern: a coin recently
        crashed (-28% on 5/13) and now in an extreme volatility
        regime. Daily ranges of 28%/28%/46% in 3 consecutive days
        mean any entry is a gamble — even iter43's deeper offset got
        filled and then plunged 9.46%.

        Blocks when EITHER:
          1. Max daily range over last N days > vr_max_daily_range_pct
          2. Any daily candle in last N days had close ≤ open × (1-vr_big_crash_pct/100)

        Reuses _d1_cache. Returns (True, None) when allowed, (False, feats)
        when blocked. Fails open on any error.
        """
        if not self.vr_enabled:
            return True, None
        candles = await self._fetch_d1_klines(symbol)
        if not candles or len(candles) < 3:
            return True, None
        recent = candles[-self.vr_lookback_days:]
        try:
            highs = [float(k[2]) for k in recent]
            lows  = [float(k[3]) for k in recent]
            opens = [float(k[1]) for k in recent]
            closes= [float(k[4]) for k in recent]
        except Exception:
            return True, None
        if any(l <= 0 for l in lows) or any(o <= 0 for o in opens):
            return True, None

        # 1. max daily range
        ranges = [(h - l) / l * 100 for h, l in zip(highs, lows)]
        max_range = max(ranges) if ranges else 0
        # 2. worst daily change
        daily_changes = [(c - o) / o * 100 for o, c in zip(opens, closes)]
        worst_day = min(daily_changes) if daily_changes else 0

        block_range = max_range > self.vr_max_daily_range_pct
        block_crash = worst_day <= -self.vr_big_crash_pct
        if not (block_range or block_crash):
            return True, None

        triggers = []
        if block_range:
            triggers.append(f"max_5d_range={max_range:.1f}% > {self.vr_max_daily_range_pct}%")
        if block_crash:
            triggers.append(f"worst_day={worst_day:+.1f}% <= -{self.vr_big_crash_pct}%")
        reason = "vol regime: " + " AND ".join(triggers)
        feats = {
            "filter": "vol_regime",
            "max_5d_range_pct": round(max_range, 2),
            "worst_5d_day_pct": round(worst_day, 2),
        }
        log.info(f"🌪️ [{symbol}] skipped by vol_regime: {reason}")
        if self.metrics is not None:
            try:
                self.metrics.signal_skipped(symbol, "vol_regime", reason, feats)
            except Exception:
                pass
        return False, feats

    async def _fetch_d1_klines(self, symbol: str):
        """Cached fetch of daily klines. One Binance REST call per symbol
        per 10 minutes — cheap enough at signal-rate (~50/day)."""
        now = time.time()
        cached = self._d1_cache.get(symbol)
        if cached and (now - cached["_ts"]) < self._d1_cache_ttl_sec:
            return cached["data"]
        try:
            limit = self.pp_lookback_days + self.pp_baseline_days + 2
            data = await self.client.fetch_ohlcv(symbol, "1d", limit=limit)
        except Exception:
            return None
        if data:
            self._d1_cache[symbol] = {"_ts": now, "data": data}
        return data

    async def _passes_post_pump_filter(self, symbol: str):
        """Daily-timeframe post-pump-bleed filter (added 2026-05-12).

        The 24h/1h falling-knife filter cannot see multi-day pumps. JTO
        pumped from ~$0.32 → $0.70 (+118%) about a week ago, then bled
        back to $0.50. Every shorter-timeframe filter saw a calm market
        and the bot bought into the downtrend — small but recurring loss.

        Algorithm (operates on the last (lookback + baseline) daily candles):

          1. Split history into two windows:
               baseline_window = closes[-(L+B) : -L]   # B days before the pump
               pump_window     = highs[-L : -1]        # L recent days, excludes today
          2. baseline = mean(baseline_window)
          3. peak     = max(pump_window)
          4. pump_pct = (peak - baseline) / baseline * 100
          5. days_since_peak = (L - 1) - peak_index_in_pump_window
          6. off_peak_pct  = (peak - current_price) / peak * 100
          7. ma7 = mean(closes[-7:])

          REJECT iff all of these hold:
             pump_pct          >= pp_threshold_pct       (recent big pump)
             off_peak_pct      >= pp_off_peak_min_pct    (already off the peak)
             current_price     <  ma7                    (below short-term avg)
             days_since_peak   >= pp_min_days_since_peak (peak is not today)

        Returns (ok, features_dict). On any data error returns (True, None)
        so a Binance hiccup does not silently disable the filter.
        """
        if not self.pp_enabled:
            return True, None
        data = await self._fetch_d1_klines(symbol)
        if not data or len(data) < (self.pp_lookback_days + 2):
            return True, None  # not enough history (e.g. new listing)
        try:
            closes = [float(c[4]) for c in data]
            highs  = [float(c[2]) for c in data]
        except Exception:
            return True, None

        L = self.pp_lookback_days
        B = self.pp_baseline_days
        current_price = closes[-1]
        # MA7 from the most recent 7 daily closes.
        ma7 = sum(closes[-7:]) / 7 if len(closes) >= 7 else current_price

        # Pump window: last L days EXCLUDING today (so an in-progress pump
        # doesn't get flagged as "past peak").
        pump_window = highs[-(L + 1):-1]
        if not pump_window:
            return True, None
        peak = max(pump_window)
        peak_idx = pump_window.index(peak)              # 0..L-1
        days_since_peak = (len(pump_window) - 1) - peak_idx  # 0..L-1

        # Baseline window: B days BEFORE the pump window.
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

        # iter 21 (2026-05-13): MA7 gate is now OPT-IN. The original 4-gate
        # design used `current < MA7` as a "still trending down" safety,
        # but in practice a single-day bounce of $0.0001 above MA7 was
        # disabling the whole filter on clear post-pump patterns. JASMY
        # 2026-05-13 — pumped +37% to $0.00783 peak, dropped to $0.00695
        # (-11% off peak), but $0.00011 above MA7 — bot bought it.
        # Sanity backtest: dropping the gate adds JASMY + SEI to blocks
        # (correctly), no false positives on BTC/ETH/SOL/XRP/ATOM/NEAR.
        gates_met = (
            pump_pct        >= self.pp_threshold_pct       and
            off_peak_pct    >= self.pp_off_peak_min_pct    and
            days_since_peak >= self.pp_min_days_since_peak
        )
        if gates_met and self.pp_require_below_ma7:
            gates_met = current_price < ma7
        rejected = gates_met
        if rejected:
            ma7_note = f", < MA7 {ma7:.4f}" if self.pp_require_below_ma7 else ""
            reason = (f"post-pump bleed: pumped +{pump_pct:.0f}% to {peak:.4f} "
                      f"{days_since_peak}d ago, now {current_price:.4f} "
                      f"(-{off_peak_pct:.1f}% off peak{ma7_note})")
            log.info(f"📉 [{symbol}] skipped by post_pump_bleed: {reason}")
            if self.metrics is not None:
                self.metrics.signal_skipped(symbol, "post_pump_bleed", reason, features)
            return False, features
        return True, features

    async def get_indicators(self, symbol: str):
        """Fetch data and compute indicators for a symbol."""
        try:
            api_symbol = symbol.replace("/", "")

            # Hot path: read 1m candles from the in-memory WebSocket buffer.
            # If the cache isn't warm for this symbol yet, register it and
            # serve the very first request from REST while the seed lands.
            df = None
            if self.klines_cache is not None:
                if not self._klines_cache_started:
                    await self.klines_cache.start()
                    self._klines_cache_started = True
                await self.klines_cache.ensure(api_symbol, ["1m"])
                if self.klines_cache.has(api_symbol, "1m"):
                    cached_df = self.klines_cache.get_klines(api_symbol, "1m", 60)
                    if not cached_df.empty:
                        df = cached_df.rename(columns={
                            "timestamp": "time", "volume": "vol",
                        })[["time", "open", "high", "low", "close", "vol"]].copy()

            if df is None:
                # Cold start (or cache disabled) — single REST seed request.
                klines = await self.client.fetch_ohlcv(symbol, timeframe='1m', limit=60)
                df = pd.DataFrame(klines, columns=['time', 'open', 'high', 'low', 'close', 'vol'])

            # The cache returns pandas-typed columns already, but a REST seed
            # comes back as Python ints/floats — astype is safe in both cases
            # for columns that are already float, and idempotent.
            for col in ('open', 'high', 'low', 'close', 'vol'):
                df[col] = df[col].astype(float)

            # Replacement for 'ta' library logic using pure pandas
            df['ema9']  = df['close'].ewm(span=EMA_FAST, adjust=False).mean()
            df['ema21'] = df['close'].ewm(span=EMA_MID, adjust=False).mean()
            df['ema50'] = df['close'].ewm(span=EMA_SLOW, adjust=False).mean()
            
            # Pure Pandas RSI (Standard Wilder's Method)
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=RSI_PERIOD).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=RSI_PERIOD).mean()
            rs = gain / loss
            df['rsi'] = 100 - (100 / (1 + rs))
            df['v_avg'] = df['vol'].rolling(window=VOL_PERIOD).mean()
            return df
        except Exception:
            return None

    def _evaluate_entry_dip(self, symbol, df, btc_df, ticker_24h=None):
        """iter180 DIP-BUY (mean-reversion). Buys AFTER a sell-off has
        bottomed and the FIRST up-move prints — never on a breakout.

        Replaces the momentum 'buy new high' logic that bought ROSEUSDT on
        its 02:00 spike top (-6.6%). Gate chain (ALL must pass):

          0. BTC not crashing               (macro safety — kept)
          1. DIP depth: price ≥ dip_min_drop_pct below the recent-window
             high (the coin has actually sold off)
          2. NOT a free-fall: drop ≤ dip_max_drop_pct in the window
             (don't catch a knife mid-plunge)
          3. OVERSOLD: RSI dipped to/below dip_rsi_oversold in the window
          4. TURNING UP: RSI now rising again and back ≥ oversold line
          5. BOTTOM FORMED: the current bar is NOT a new window low
             (a higher low = sellers stepping back)
          6. UP-MOVE TRIGGER: green candle that either closes above the
             prior bar's high OR reclaims ema9 (the first bounce)
          7. 24h sanity: enough volume + range to bounce, and 24h change
             neither collapsing (< -dip_max_24h_drop_pct) nor parabolic
             (> dip_max_24h_pump_pct)
        """
        if df is None or btc_df is None or len(df) < 30:
            return False, None
        try:
            curr = df.iloc[-1]
            prev = df.iloc[-2]
            btc  = btc_df.iloc[-1]
            n = max(5, int(self.dip_lookback_bars))
            win = df.iloc[-n:]
            recent_high = float(win['high'].max())
            recent_low  = float(win['low'].min())
            close = float(curr['close'])
            open_ = float(curr['open'])
            low   = float(curr['low'])
            ema9  = float(curr['ema9'])
            drop_from_high = ((close - recent_high) / recent_high * 100.0) if recent_high > 0 else 0.0

            rsi_win = df['rsi'].iloc[-n:].dropna()
            rsi_min = float(rsi_win.min()) if not rsi_win.empty else 50.0
            rsi_now = float(curr['rsi']) if pd.notna(curr['rsi']) else 50.0
            rsi_prev = float(prev['rsi']) if pd.notna(prev['rsi']) else rsi_now

            # 0. BTC trend filter (must not be crashing)
            btc_ok = bool(btc['close'] > btc['ema21'])
            # 1. sold off enough
            dip_ok = drop_from_high <= -self.dip_min_drop_pct
            # 2. but not a free-fall (knife)
            not_knife = drop_from_high >= -self.dip_max_drop_pct
            # 3. & 4. oversold then turning up
            was_oversold = rsi_min <= self.dip_rsi_oversold
            rsi_turning = (rsi_now > rsi_prev) and (rsi_now >= self.dip_rsi_oversold)
            # 5. bottom formed — current bar isn't a fresh low
            bottom_formed = low > recent_low
            # 6. first up-move trigger
            bullish = close > open_
            up_trigger = bullish and ((close > float(prev['high'])) or (close > ema9))

            # 7. 24h sanity
            ticker_24h_ok = True
            change_24h = range_24h = vol_24h = 0.0
            if ticker_24h:
                change_24h = float(ticker_24h.get('percentage') or 0)
                high = float(ticker_24h.get('high') or 0)
                low24 = float(ticker_24h.get('low') or 0)
                range_24h = ((high - low24) / low24 * 100) if low24 > 0 else 0
                vol_24h = float(ticker_24h.get('quoteVolume') or 0)
                ticker_24h_ok = (
                    vol_24h >= self.min_vol_24h_usd and
                    range_24h >= self.min_range_24h_pct and
                    change_24h >= -self.dip_max_24h_drop_pct and
                    change_24h <= self.dip_max_24h_pump_pct
                )

            matrix = {
                "strategy": "dip_buy",
                "btc_stable": btc_ok,
                "dip_depth_ok": bool(dip_ok),
                "not_knife": bool(not_knife),
                "was_oversold": bool(was_oversold),
                "rsi_turning_up": bool(rsi_turning),
                "bottom_formed": bool(bottom_formed),
                "up_trigger": bool(up_trigger),
                "ticker_24h": bool(ticker_24h_ok),
                "drop_from_high_pct": round(drop_from_high, 2),
                "rsi_min": round(rsi_min, 1),
                "rsi_now": round(rsi_now, 1),
                "change_24h_pct": round(change_24h, 2),
                "range_24h_pct": round(range_24h, 2),
                "vol_24h_m_usd": round(vol_24h / 1_000_000, 2),
            }
            passed = (btc_ok and dip_ok and not_knife and was_oversold
                      and rsi_turning and bottom_formed and up_trigger
                      and ticker_24h_ok)
            return passed, matrix
        except Exception as exc:
            log.debug(f"dip evaluate_entry failed for {symbol}: {exc}")
            return False, None

    def evaluate_entry(self, symbol, df, btc_df, ticker_24h=None):
        """Entry router. iter180: when ``dip_buy_enabled`` (default), use
        the mean-reversion DIP-BUY strategy. Otherwise fall through to the
        legacy Micro-Trend Momentum Acceleration breakout logic (kept for
        a one-flag revert via dipBuyEnabled=false).

        ticker_24h is an optional dict with Binance's 24h ticker fields
        (percentage, high, low, quoteVolume). Pre-fetched by the caller
        in async context. None disables the 24h filter for this call.
        """
        if self.dip_buy_enabled:
            return self._evaluate_entry_dip(symbol, df, btc_df, ticker_24h)
        if df is None or btc_df is None or len(df) < 20: return False, None

        curr = df.iloc[-1]
        prev = df.iloc[-2]
        btc  = btc_df.iloc[-1]

        # BTC Trend Filter (Must not be crashing)
        btc_ok = btc['close'] > btc['ema21']

        # 1. TREND: EMAs are stacked (9 > 21 > 50)
        uptrend = curr['ema9'] > curr['ema21'] > curr['ema50']

        # 2. ACCELERATION: Current price breaking above previous high (Micro-breakout)
        breakout = curr['close'] > prev['high']

        # 3. MOMENTUM: RSI is strong but not exhausted
        rsi_ok = 60 < curr['rsi'] < 82

        # 4. VELOCITY: Volume is surging
        vol_ok = curr['vol'] > (curr['v_avg'] * 1.5)

        bullish = curr['close'] > curr['open']

        # 5. MARKET CONTEXT — 24h ticker filter.
        # Derived from the 2026-05-10 P&L post-mortem:
        #   - winners avg 24h change +3.5 %, losers -2.8 %
        #     → reject coins already trending DOWN over 24h
        #   - winners avg 24h range 15.7 %, losers 8.5 %
        #     → reject coins too quiet to hit +0.5 % TP
        #   - extreme low-volume coins (ZRX $0.2M, STRAX $0.3M)
        #     showed up as losers → enforce a $2 M floor
        ticker_24h_ok = True
        change_24h = range_24h = vol_24h = 0.0
        if ticker_24h:
            change_24h = float(ticker_24h.get('percentage') or 0)
            high = float(ticker_24h.get('high') or 0)
            low = float(ticker_24h.get('low') or 0)
            range_24h = ((high - low) / low * 100) if low > 0 else 0
            vol_24h = float(ticker_24h.get('quoteVolume') or 0)
            ticker_24h_ok = (
                change_24h >= self.min_change_24h_pct and
                range_24h  >= self.min_range_24h_pct and
                vol_24h    >= self.min_vol_24h_usd
            )

        # Signal Matrix
        matrix = {
            "btc_stable": bool(btc_ok),
            "trend_stacked": bool(uptrend),
            "micro_breakout": bool(breakout),
            "rsi_momentum": bool(rsi_ok),
            "volume_surge": bool(vol_ok),
            "bullish_candle": bool(bullish),
            "ticker_24h": bool(ticker_24h_ok),
            "change_24h_pct": round(change_24h, 2),
            "range_24h_pct": round(range_24h, 2),
            "vol_24h_m_usd": round(vol_24h / 1_000_000, 2),
        }
        # Pass requires every original gate plus the 24h filter.
        passed = (btc_ok and uptrend and breakout and rsi_ok and vol_ok and bullish and ticker_24h_ok)
        return passed, matrix

    # 24h ticker cache (one fetch per symbol per ~30 s)
    _t24h_cache = None
    _t24h_cache_ttl_sec = 30

    async def _fetch_24h_ticker(self, symbol: str):
        """Cached async fetch of the 24h ticker. One Binance call per
        symbol per ~30 s — cheap enough that even with ~600 symbols
        we stay well under the 1200/min weight cap.
        Weight per call: 1. So 600 symbols × 1 / 30 s = 20 weight/min."""
        if self._t24h_cache is None:
            self._t24h_cache = {}
        now = time.time()
        cached = self._t24h_cache.get(symbol)
        if cached and (now - cached['_ts']) < self._t24h_cache_ttl_sec:
            return cached['data']
        try:
            data = await self.client.fetch_ticker(symbol)
        except Exception:
            return None
        if data:
            self._t24h_cache[symbol] = {'_ts': now, 'data': data}
        return data

    async def process_symbol(self, symbol, btc_df):
        """Analyze and trade a single symbol."""
        # Laddered Recovery: if THIS symbol has an active ladder, the
        # ladder state machine owns it — skip the legacy position-management
        # path. Other symbols are free to be evaluated for new entries.
        if (self.ladder_enabled and ladder is not None
                and ladder.load_state(self.redis, symbol) is not None):
            return

        # 1. Manage existing position
        if symbol in self.active_positions:
            pos = self.active_positions[symbol]

            # 1a. If we have an OCO order on Binance, check whether it
            # filled (TP or SL). The exchange handles the cancel-the-other
            # leg automatically, so once ALL_DONE we just clear our local
            # tracking. Done first so the polling fallback below doesn't
            # double-sell something the exchange already closed.
            if pos.get('oco_list_id'):
                resolved = await self._check_oco_status(symbol, pos)
                if resolved:
                    return

            # 1a-bis. TP-only LIMIT path (Option B "no stop" mode). Same
            # pattern: ask Binance whether the TP filled, finalize if yes.
            if pos.get('tp_order_id'):
                resolved = await self._check_tp_only_status(symbol, pos)
                if resolved:
                    return

            df = await self.get_indicators(symbol)
            if df is None: return

            curr = df.iloc[-1]
            pnl = (curr['close'] - pos['buy_price']) * pos['qty']

            # Track post-fill trajectory in METRICS:OUTCOME so the
            # dashboard can show how the price has moved since fill.
            # We update bottom_pct (lowest), max_pct (highest), and
            # the most recent vol-1m / pre-baseline ratio.
            self._update_trajectory_metrics(symbol, pos, curr)

            # 1b. Trend reversal exit — only when the operator has it
            # enabled. 2026-05-11 P&L showed most "panic exits" via this
            # path hit TP later if held, so default-live is False even
            # though code default is True (safety on Redis wipe).
            if self.trend_reversal_exit_enabled and curr['ema9'] < curr['ema21']:
                log.info(f"🔄 [{symbol}] Trend Reversal")
                await self.execute_sell(symbol, curr['close'])
                return

            # 1c. Polling-based TP/SL — backup safety net for positions
            # whose exchange-side resting order placement failed (no
            # oco_list_id and no tp_order_id). When either is active we
            # let Binance handle it for tighter timing.
            if not pos.get('oco_list_id') and not pos.get('tp_order_id'):
                buy_price = float(pos.get('buy_price') or 0)
                pnl_pct = ((float(curr['close']) - buy_price) / buy_price * 100.0) if buy_price > 0 else 0.0
                if pnl >= self.profit_target_usdt:
                    log.info(f"💰 [{symbol}] Profit Target Hit (poll): +${pnl:.2f}")
                    await self.execute_sell(symbol, curr['close'])
                elif self.stop_loss_usdt > 0 and pnl <= -self.stop_loss_usdt:
                    # Stop-loss only fires when explicitly enabled
                    # (stopLossUsdt > 0). Option B sets it to 0 so this
                    # branch is dead — patient hold until TP or trend exit.
                    log.info(f"🛡️ [{symbol}] Stop Loss Hit (poll): -${pnl:.2f}")
                    await self.execute_sell(symbol, curr['close'])
                elif (self.dip_buy_enabled and self.dip_hard_stop_pct > 0
                      and pnl_pct <= -self.dip_hard_stop_pct):
                    # iter180: dip-buy positions MUST have a real stop — the
                    # bottom thesis was wrong. Protects even when the USDT
                    # stop is disabled (Option B). Mean-reversion bleeds
                    # without this (the MEGAUSDT/ROSE failure mode).
                    log.info(f"🛡️ [{symbol}] Dip Hard-Stop (poll): {pnl_pct:.2f}% ≤ -{self.dip_hard_stop_pct:.1f}%")
                    await self.execute_sell(symbol, curr['close'])
            return

        # 2. Look for new entry
        df = await self.get_indicators(symbol)
        # Pre-fetch 24h ticker (cached 30 s) so the sync evaluate_entry
        # can use it for the post-mortem-derived market filter.
        ticker_24h = await self._fetch_24h_ticker(symbol)
        should_buy, matrix = self.evaluate_entry(symbol, df, btc_df, ticker_24h=ticker_24h)

        last_price = df.iloc[-1]['close'] if df is not None else 0
        # Broadcast detailed matrix
        self.broadcast_signal(symbol, "BUY" if should_buy else "NEUTRAL", last_price, matrix)

        if should_buy:
            # Layer the falling-knife filter on top of evaluate_entry. The
            # built-in 24h filter already catches downtrend/illiquid coins;
            # this catches buying a top after a recent pump.
            ok, features = await self.passes_falling_knife(symbol)
            if self.metrics is not None:
                self.metrics.signal_evaluated(
                    symbol, last_price,
                    features=features,
                    decision="pass" if ok else "skipped",
                )
            if not ok:
                return
            # Iter 38 (2026-05-15): near-top pump gate. Reuses the
            # `features` dict just produced by passes_falling_knife
            # (no extra Binance call). Catches the QNT/USDT pattern
            # where 24h change was only +7.46% (under maxChange24hPct=12)
            # but we entered 0.82% below the 24h peak.
            if not self._passes_near_top_pump_filter(symbol, features):
                return
            # Iter 44 (2026-05-15): macro-top exhaustion. Catches
            # PENDLE-style entries — coin rallied massively (>50% in
            # 30d), still near top of rally (within 10% of 30d high),
            # showing distribution (4+ red days in last 7). Fires
            # BEFORE post-pump-bleed which requires off-peak ≥10%.
            mt_ok, _ = await self._passes_macro_top_filter(symbol)
            if not mt_ok:
                return
            # Iter 45 (2026-05-15): volatility regime. Catches MLN-style
            # — coin recently crashed (-28% on 5/13), now in extreme
            # volatility regime (daily ranges 28%/28%/46% over 3 days).
            # Even iter43's deeper offset gets filled and then plunges.
            vr_ok, _ = await self._passes_vol_regime_filter(symbol)
            if not vr_ok:
                return
            # Daily-timeframe post-pump-bleed gate (2026-05-12). The 24h/1h
            # filters can't see a multi-day downtrend — e.g. JTO pumped a
            # week ago and bled back, every short-timeframe check looked
            # calm. This filter is the only one that opens the multi-day
            # window. Skip-events show up in METRICS:SKIP as `post_pump_bleed`.
            pp_ok, _ = await self._passes_post_pump_filter(symbol)
            if not pp_ok:
                return
            await self.execute_buy(symbol, last_price, features=features)

    # ── Laddered Recovery (multi-coin 3-tier averaging-down) ──────────────
    async def _maybe_route_ladder(self, symbol, price, features=None):
        """If ladder mode is on, route the buy through the ladder state
        machine. Returns True if the ladder handled the buy (caller skips
        the legacy single-buy path).

        Capacity rules (2026-05-11 iter 2):
          • Per-symbol uniqueness: no second ladder on the same symbol
          • Global cap: at most maxConcurrentLadders ladders in flight
        """
        if not (self.ladder_enabled and ladder is not None):
            return False
        # Already a ladder for this symbol (in EITHER scalper)?
        if ladder.is_active_anywhere(self.redis, symbol):
            log.info(f"⏸️  [{symbol}] already has an active ladder somewhere; ignoring signal")
            return True
        # Cooldown check
        if ladder.is_on_cooldown(self.redis, symbol):
            secs = ladder.cooldown_remaining_seconds(self.redis, symbol)
            log.info(f"⏸️  [{symbol}] on cooldown ({secs//60} min left); ignoring signal")
            return True
        # GLOBAL capacity check — combined across Fast + Virtual scalpers
        # (2026-05-11 iter 5: cap of 3 applies to the whole system).
        total_active = ladder.count_total_active(self.redis)
        if total_active >= self.max_concurrent_ladders:
            held = ladder.list_active_symbols_combined(self.redis)
            log.info(f"⏸️  [{symbol}] global ladder cap reached "
                     f"({total_active}/{self.max_concurrent_ladders} across all scalpers: {held})")
            return True
        await self._ladder_start(symbol, price, features=features)
        return True

    async def _has_sufficient_usdt(self, required: float) -> bool:
        """Pre-flight check: free USDT >= required. Returns True if we
        should proceed, False to skip (with a warning log)."""
        try:
            bal = await self.client.fetch_balance()
            free = float((bal.get('USDT') or {}).get('free') or 0)
        except Exception as exc:
            log.warning(f"⚠️ [ladder] balance fetch failed ({exc}); proceeding")
            return True
        # Safety margin to absorb fees + races with other pending orders.
        # 2026-05-12 iter 13: was 10% — too aggressive when free USDT is
        # close to the ladder size ($100 needs $110 free, but $103.88 free
        # was rejecting every ladder while the wallet was bigger than the
        # ladder itself). 3% is more than enough to cover 2 legs × $0.05
        # of BNB-discounted fees + small price drift between this balance
        # check and the actual order placement.
        margin = required * 0.03
        if free < required + margin:
            log.warning(f"⚠️ [ladder] insufficient USDT: free=${free:.4f} need=${required:.4f} (+margin)")
            return False
        return True

    async def _handle_external_cancel(self, state, who: str):
        """An order the bot was tracking was cancelled outside the bot
        (operator did it manually on the Binance app/web).

        iter 23 (2026-05-13): when the operator takes over, the bot
        STANDS DOWN COMPLETELY on this symbol. No more auto-cancels of
        any other legs (Buy 2 limit etc.), no market-sells, no further
        TP placements. The operator is now in control — anything else
        the bot does could fight against their manual order management.

        Why we used to cancel other legs:
          A pending Buy 2 LIMIT could still fill in the background,
          giving the operator extra qty they didn't expect.

        Why iter 23 stops doing that:
          The operator was reporting that the bot was "cancelling the
          sell order I just placed in Binance app". The bot wasn't
          actually cancelling the operator's NEW sell — it was cancelling
          its own Buy 2 LIMIT — but the cleanup confused the operator's
          mental model of "I'm taking over". Best behaviour is: respect
          the manual cancel as a 'hands off' signal and stop touching
          anything else on this symbol. If Buy 2 fills later, the
          operator manages it (or cancels it themselves on the Binance
          app — they already have it open).

        iter 17 retry chain stays: we still attempt to find the operator's
        sell trade and patch the OUTCOME hash with realised P&L.
        """
        log.info(f"🛑 [ladder] {state.symbol} {who} cancelled externally — bot STANDING DOWN "
                 f"(no auto-cancel of other legs; operator is in control)")

        # iter 23: list any remaining bot-owned orders for transparency,
        # but DO NOT cancel them. Operator can cancel via Binance app if
        # they don't want Buy 2 to fill.
        remaining = []
        for leg in (state.buy_1, state.buy_2, state.buy_3):
            if leg and leg.order_id and leg.status not in ("filled", "cancelled"):
                remaining.append(f"{leg.label}@{leg.order_id} ({leg.target_price})")
                # Mark as "operator-managed" in our state — we no longer
                # think of these orders as ours to manage. We still
                # remember the order_id for diagnostics.
                leg.status = "operator_managed"
        if state.tp_order_id and who != "TP":
            # If the cancel came from a leg, the TP is also still on the
            # book — operator now owns it.
            remaining.append(f"tp@{state.tp_order_id} ({state.tp_target_price})")
        if remaining:
            log.info(f"   [ladder] {state.symbol} bot-placed orders left ALONE for operator: {remaining}")
        # Clear our tracked tp_order_id either way — we no longer manage it.
        state.tp_order_id = None

        # Did Buy 1 actually fill? If so, qty + avg are real and the
        # operator may have realised a P&L by selling externally.
        filled_legs = [L for L in (state.buy_1, state.buy_2, state.buy_3)
                       if L and L.qty_filled and L.qty_filled > 0]
        total_qty = sum(L.qty_filled for L in filled_legs)
        avg_buy = state.weighted_avg() or 0
        had_fill = total_qty > 0 and avg_buy > 0

        # Initial exit record — best info we have right now.
        state.state = ladder.CLOSED
        state.closed_ts = int(time.time() * 1000)
        if had_fill:
            # Try to find a matching sell that's already on the books.
            buy_ts = int(state.buy_1.fill_ts) if state.buy_1 and state.buy_1.fill_ts else 0
            sell_info = await self._find_post_buy_sell(state.symbol, buy_ts, total_qty)
            if sell_info is not None:
                exit_price = sell_info["price"]
                fees = 2 * self.ladder_fee_rate_per_side * (avg_buy * total_qty)
                pnl = (exit_price - avg_buy) * total_qty - fees
                state.exit_reason = "manual_cancel_resolved"
                log.info(f"🛑 [ladder] {state.symbol} found operator sell @ ${exit_price:.6f} "
                         f"qty={sell_info['qty']:.6f} → realised P&L ${pnl:+.4f}")
                if self.metrics is not None:
                    try:
                        self.metrics.exit_recorded(state.symbol, avg_buy, exit_price,
                                                   reason="manual_cancel_resolved", pnl_usdt=pnl)
                    except Exception:
                        pass
            else:
                # No matching sell yet — operator may sell in the next
                # few minutes. Record as 'holding' and schedule retry.
                state.exit_reason = "manual_cancel_holding"
                log.info(f"🛑 [ladder] {state.symbol} no sell found yet — "
                         f"recording as holding ({total_qty:.6f} @ avg ${avg_buy:.6f}), "
                         f"will retry resolution in 5 min")
                if self.metrics is not None:
                    try:
                        self.metrics.exit_recorded(state.symbol, avg_buy, avg_buy,
                                                   reason="manual_cancel_holding", pnl_usdt=0)
                    except Exception:
                        pass
                # Fire-and-forget retry. Will re-query trade history and
                # patch the OUTCOME hash with the real P&L when the sell
                # appears. Multiple attempts at 60s / 5min / 15min so
                # operator delays are accommodated.
                asyncio.create_task(self._resolve_manual_cancel_later(
                    state.symbol, buy_ts, total_qty, avg_buy
                ))
        else:
            # Buy 1 never filled — the old behaviour was correct here.
            state.exit_reason = "manual_cancel"
            if self.metrics is not None:
                try:
                    self.metrics.exit_recorded(state.symbol, 0, 0,
                                               reason="manual_cancel", pnl_usdt=0)
                except Exception:
                    pass

        ladder.clear_state(self.redis, state.symbol)
        ladder.set_cooldown(self.redis, state.symbol, self.ladder_cooldown_seconds)

    async def _find_post_buy_sell(self, symbol: str, buy_ts_ms: int, expected_qty: float):
        """Search Binance trade history for a SELL trade matching this qty
        after buy_ts_ms. Returns {'price': avg_price, 'qty': total_qty}
        or None if no matching sell found.

        Uses ccxt.fetch_my_trades and aggregates any SELLs that happened
        after the buy timestamp. Matches on cumulative qty (within 5%)
        rather than a single fill, so split sells (e.g. partial fills
        across two limit orders) still resolve correctly.
        """
        if buy_ts_ms <= 0 or expected_qty <= 0:
            return None
        try:
            # ccxt: since= is start time in ms, limit caps result count
            trades = await self.client.fetch_my_trades(symbol, since=buy_ts_ms, limit=50)
        except Exception as exc:
            log.warning(f"⚠️ [ladder] {symbol} fetch_my_trades failed: {exc}")
            return None
        sells = [t for t in (trades or [])
                 if (t.get("side") or "").lower() == "sell"
                 and int(t.get("timestamp") or 0) >= buy_ts_ms]
        if not sells:
            return None
        total_qty = sum(float(t.get("amount") or 0) for t in sells)
        total_cost = sum(float(t.get("amount") or 0) * float(t.get("price") or 0) for t in sells)
        if total_qty <= 0:
            return None
        # Require cumulative sell qty within 5% of expected (catches both
        # partial sells and combined sells).
        if abs(total_qty - expected_qty) > expected_qty * 0.05:
            log.debug(f"[ladder] {symbol} sell qty mismatch: got {total_qty} expected {expected_qty}")
            return None
        return {"price": total_cost / total_qty, "qty": total_qty}

    async def _resolve_manual_cancel_later(self, symbol: str, buy_ts_ms: int,
                                            expected_qty: float, avg_buy: float):
        """Delayed retry: re-query trade history at 60s, 5 min, 15 min
        intervals to catch sells that happen after _handle_external_cancel
        fired. When a matching sell is found, patch the OUTCOME hash with
        the real exit price + realised P&L.

        Fire-and-forget — runs as an asyncio.Task. Exits silently after
        all retries exhaust without finding a match (operator is still
        holding the position)."""
        retry_delays = (60, 300, 900)   # 1 min, 5 min, 15 min
        for delay_sec in retry_delays:
            try:
                await asyncio.sleep(delay_sec)
            except asyncio.CancelledError:
                return
            try:
                sell_info = await self._find_post_buy_sell(symbol, buy_ts_ms, expected_qty)
            except Exception as exc:
                log.debug(f"[ladder] {symbol} delayed resolve query failed: {exc}")
                continue
            if sell_info is None:
                continue
            exit_price = sell_info["price"]
            fees = 2 * self.ladder_fee_rate_per_side * (avg_buy * expected_qty)
            pnl = (exit_price - avg_buy) * expected_qty - fees
            log.info(f"🛑 [ladder] {symbol} retroactively resolved manual_cancel: "
                     f"sell @ ${exit_price:.6f} → P&L ${pnl:+.4f} "
                     f"(after {delay_sec}s retry)")
            if self.metrics is not None:
                try:
                    self.metrics.patch_outcome(
                        symbol, exit_price=exit_price, pnl_usdt=pnl,
                        reason="manual_cancel_resolved",
                    )
                except Exception:
                    pass
            return
        log.info(f"🛑 [ladder] {symbol} manual_cancel still unresolved after all retries — "
                 f"operator is holding {expected_qty:.6f} @ avg ${avg_buy:.6f}")

    async def _compute_audit_snapshot(self, symbol, signal_price, buy1_limit_price):
        """Build the audit dict the dashboard needs:
            pre_signal_price (5m back), pre_signal_price_2 (10m back),
            past 15m low/high, buy_2 limit, three target-sell prices.
        Best-effort: any field that can't be computed stays None."""
        out = {
            "signal_price": signal_price,
            "buy_1_limit_price": buy1_limit_price,
            "pre_signal_price": None,        # 5 min before signal
            "pre_signal_price_2": None,      # 10 min before signal
            "past_15min_low": None,
            "past_15min_high": None,
            "buy_2_limit_price": None,
            "target_sell_005": None,
            "target_sell_010": None,
            "target_sell_015": None,
            "scalper_origin": "FAST",
        }
        try:
            candles = await self.client.fetch_ohlcv(symbol, "1m", limit=20)
            if candles and len(candles) >= 5:
                # Pre-signal-1 = close 5 min before now
                pre1 = candles[-6] if len(candles) >= 6 else candles[0]
                out["pre_signal_price"] = float(pre1[4] or 0)
                # Pre-signal-2 = close 10 min before now
                if len(candles) >= 11:
                    pre2 = candles[-11]
                    out["pre_signal_price_2"] = float(pre2[4] or 0)
                # Past-15m low/high
                last_15 = candles[-15:] if len(candles) >= 15 else candles
                highs = [float(c[2] or 0) for c in last_15 if float(c[2] or 0) > 0]
                lows  = [float(c[3] or 0) for c in last_15 if float(c[3] or 0) > 0]
                if highs: out["past_15min_high"] = max(highs)
                if lows:  out["past_15min_low"]  = min(lows)
        except Exception as exc:
            log.debug(f"audit snapshot 15m candles failed for {symbol}: {exc}")
        # Buy 2 limit = signal × (1 - buy2_offset)
        try:
            out["buy_2_limit_price"] = signal_price * (1 - self.ladder_buy2_offset_pct / 100.0)
        except Exception:
            pass
        # Target sell prices for $0.05/$0.10/$0.15 NET on this buy
        fee_2 = 2 * self.ladder_fee_rate_per_side
        try:
            buy_size = self.ladder_buy1_size or 1.0
            for net, key in ((0.05, "target_sell_005"), (0.10, "target_sell_010"), (0.15, "target_sell_015")):
                tp_pct = (net / buy_size) + fee_2   # decimal (not %)
                out[key] = buy1_limit_price * (1 + tp_pct)
        except Exception:
            pass
        return out

    def _compute_adaptive_entry_params(self, features):
        """iter 43 (2026-05-15) — Volatility-Adaptive Entry + TP.

        Reads `range_1h_pct` from the features dict and picks one of
        four entry strategies. Returns a dict:

            {
              'strategy':        'CALM' | 'NORMAL' | 'VOLATILE' | 'X_VOLATILE',
              'buy1_offset_pct': float,
              'buy2_offset_pct': float,
              'tp_target_usdt':  float,
              'range_1h_pct':    float,
            }

        When `ae_enabled` is False OR features are missing, returns the
        static config values (current behaviour) — fully backward-compat.
        """
        # Fallback to current static config
        defaults = {
            'strategy': 'STATIC',
            'buy1_offset_pct': self.ladder_buy1_offset_pct,
            'buy2_offset_pct': self.ladder_buy2_offset_pct,
            'tp_target_usdt':  self.ladder_target_net_usdt,
            'range_1h_pct':    -1.0,
        }
        if not self.ae_enabled:
            return defaults
        if not features:
            return defaults
        try:
            r1h = float(features.get('range_1h_pct') or -1)
        except (TypeError, ValueError):
            return defaults
        if r1h < 0:
            return defaults

        if r1h < self.ae_tier_calm_max:
            return {
                'strategy': 'CALM',
                'buy1_offset_pct': self.ae_buy1_calm,
                'buy2_offset_pct': self.ae_buy2_calm,
                'tp_target_usdt':  self.ae_tp_calm,
                'range_1h_pct':    r1h,
            }
        if r1h < self.ae_tier_normal_max:
            return {
                'strategy': 'NORMAL',
                'buy1_offset_pct': self.ae_buy1_normal,
                'buy2_offset_pct': self.ae_buy2_normal,
                'tp_target_usdt':  self.ae_tp_normal,
                'range_1h_pct':    r1h,
            }
        if r1h < self.ae_tier_volatile_max:
            return {
                'strategy': 'VOLATILE',
                'buy1_offset_pct': self.ae_buy1_volatile,
                'buy2_offset_pct': self.ae_buy2_volatile,
                'tp_target_usdt':  self.ae_tp_volatile,
                'range_1h_pct':    r1h,
            }
        return {
            'strategy': 'X_VOLATILE',
            'buy1_offset_pct': self.ae_buy1_xvolatile,
            'buy2_offset_pct': self.ae_buy2_xvolatile,
            'tp_target_usdt':  self.ae_tp_xvolatile,
            'range_1h_pct':    r1h,
        }

    # iter 87 — HARD KILL SWITCH for Fast Scalper.  Mirrors iter86 in
    # executor.try_buy.  Even if cfg.fastScalpMode is somehow flipped
    # to True via Redis, this hard-coded constant prevents any buy.
    # To re-enable, change this constant and redeploy.
    HARD_DISABLE_AUTOBUY: bool = True

    # iter 94 — HARD KILL SWITCH for Fast Scalper SELLs.
    # Blocks every exit path (ladder TP, OCO, force-exit, trailing-TP,
    # aggressive-limit, hard-stop) so the operator manages every sell
    # on Binance directly. Only the operator can flip this by editing
    # the constant and redeploying — Redis config cannot override.
    HARD_DISABLE_AUTOSELL: bool = True

    async def _publish_blocked_sell(self, symbol, price, reason, source="scalper"):
        """iter 94 — When the scalper WOULD have sold but is blocked by
        HARD_DISABLE_AUTOSELL, publish to BOT_SELL_SIGNALS:<date> so the
        operator sees on the dashboard which positions need a manual exit.
        """
        import json as _json
        from datetime import datetime as _dt, timezone as _tz
        try:
            date = _dt.now(_tz.utc).strftime("%Y-%m-%d")
            event = {
                "ts": int(time.time() * 1000),
                "symbol": symbol,
                "kind": "fast_scalper_sell",
                "source": source,
                "reason": reason,
                "would_sell_price": float(price) if price is not None else None,
                "blocked_by": "HARD_DISABLE_AUTOSELL",
            }
            payload = _json.dumps(event)
            await asyncio.to_thread(self.redis.rpush, f"BOT_SELL_SIGNALS:{date}", payload)
            await asyncio.to_thread(self.redis.expire, f"BOT_SELL_SIGNALS:{date}", 14 * 24 * 3600)
            await asyncio.to_thread(self.redis.hset, "BOT_SELL_SIGNALS:LATEST", symbol, payload)
            log.warning(f"⛔ [fast-scalper-sell-block] {symbol} {reason} @ {price} blocked by HARD_DISABLE_AUTOSELL")
        except Exception as e:
            log.debug(f"[blocked-sell] publish failed: {e}")

    async def _publish_scalper_signal(self, symbol, signal_price, features, source="ladder"):
        """iter 88 — When the scalper WOULD have bought but is blocked by
        HARD_DISABLE_AUTOBUY, publish a signal event so the operator can
        see it in the dashboard and decide manually.
        Redis key: FAST_SCALPER:DETECTIONS:<date> (RPUSH of JSON events)
        """
        import json as _json
        from datetime import datetime, timezone
        try:
            date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            event = {
                "ts": int(time.time() * 1000),
                "symbol": symbol,
                "kind": "fast_scalper",
                "source": source,
                "signal_price": float(signal_price),
                "features": features or {},
                "would_buy": True,
                "blocked_by": "HARD_DISABLE_AUTOBUY",
            }
            payload = _json.dumps(event)
            await asyncio.to_thread(self.redis.rpush, f"FAST_SCALPER:DETECTIONS:{date}", payload)
            await asyncio.to_thread(self.redis.ltrim, f"FAST_SCALPER:DETECTIONS:{date}", -2000, -1)  # cap day-key
            await asyncio.to_thread(self.redis.expire, f"FAST_SCALPER:DETECTIONS:{date}", 90 * 24 * 3600)  # iter157 90d
            await asyncio.to_thread(self.redis.hset, "FAST_SCALPER:LATEST", symbol, payload)
            log.info(f"📡 [scalper-signal] {symbol} would_buy @ {signal_price} (source={source})")
        except Exception as e:
            log.debug(f"[scalper-signal] publish failed: {e}")

    async def _ladder_start(self, symbol, signal_price, features=None):
        """Place buy 1 + buys 2/3 + TP. When buy 1 is a market order the
        whole ladder (legs 2/3 + TP) is on the book within microseconds
        of buy 1's fill — no waiting state."""
        # iter 87 — hard kill switch (manual-only mode)
        if self.HARD_DISABLE_AUTOBUY:
            # iter 88 — publish signal so operator sees what scalper would have bought
            await self._publish_scalper_signal(symbol, signal_price, features, source="ladder")
            log.debug(f"[ladder] {symbol} ignored — HARD_DISABLE_AUTOBUY=True (manual-only mode)")
            return
        try:
            if symbol not in self.filters: await self.fetch_filters(symbol)
            f = self.filters.get(symbol)
            if not f:
                log.warning(f"⚠️ [ladder] {symbol} missing market filters; skipping")
                return

            tick = f.get("tick_size") or 0.00000001

            # iter 43: compute volatility-adaptive params from features.
            # When ae_enabled=False or features missing, falls back to
            # static config — fully backward compatible.
            adapt = self._compute_adaptive_entry_params(features)
            dyn_buy1_off = adapt['buy1_offset_pct']
            dyn_buy2_off = adapt['buy2_offset_pct']
            dyn_tp_tgt   = adapt['tp_target_usdt']
            log.info(f"🎚️ [ladder] {symbol} adaptive strategy={adapt['strategy']} "
                     f"range_1h={adapt['range_1h_pct']:.2f}% → "
                     f"buy1_off={dyn_buy1_off}% buy2_off={dyn_buy2_off}% tp_target=${dyn_tp_tgt}")

            # ── iter 73 + iter 74 — All pre-buy gates ────────────────
            # Fast Scalper bypassed every safety filter until iter73
            # (NEIRO incident 2026-05-24).  iter74 routes all 3 gates
            # (USDT cooldown + check-coin + orderbook depth) through
            # the shared `pre_buy_gates` module so future filters land
            # in one place for ALL buy paths.
            try:
                cfg_dict = await self.config.get_dict() if hasattr(self, "config") else {}
            except Exception:
                cfg_dict = {}
            if not cfg_dict:
                try:
                    raw_cfg = await asyncio.to_thread(self.redis.get, "TRADING_CONFIG")
                    cfg_dict = json.loads(raw_cfg) if raw_cfg else {}
                except Exception:
                    cfg_dict = {}
            block_reason = await asyncio.to_thread(
                _run_pre_buy_gates,
                symbol, float(self.ladder_buy1_size), cfg_dict, self.redis,
            )
            if block_reason:
                log.info(f"⛔ [ladder] {symbol} pre-buy gate BLOCKED: {block_reason}")
                try:
                    await asyncio.to_thread(
                        self.redis.lpush,
                        f"METRICS:SKIP:{time.strftime('%Y-%m-%d')}",
                        json.dumps({
                            "ts": int(time.time() * 1000),
                            "symbol": symbol,
                            "rule": "fast_scalper_pre_buy_gate",
                            "reason": block_reason,
                            "rule_label": f"FAST_SCALPER:{adapt['strategy']}",
                        }),
                    )
                except Exception:
                    pass
                return

            # Routing priority:
            #   1. dyn_buy1_off > 0  → LIMIT at signal × (1 - X%)
            #   2. ladderBuy1UseMarketOrder → MARKET (current default)
            #   3. else                     → aggressive-limit-at-ask
            use_offset_limit = dyn_buy1_off > 0

            if use_offset_limit:
                # LIMIT BUY at a specific offset below signal price.
                buy1_price = self.round_step(
                    signal_price * (1 - dyn_buy1_off / 100.0), tick
                )
                buy1_qty = self.round_step(self.ladder_buy1_size / max(buy1_price, 1e-12), f["step_size"])
                if (buy1_qty * buy1_price) < f["min_notional"]:
                    log.info(f"⏸️  [ladder] {symbol} buy 1 notional too low")
                    return
                total_needed = (self.ladder_buy1_size
                                + self.ladder_buy2_size + self.ladder_buy3_size)
                if not await self._has_sufficient_usdt(total_needed):
                    log.info(f"⏸️  [ladder] {symbol} skipped — funds short")
                    return
                try:
                    placed = await self.client.create_limit_buy_order(symbol, buy1_qty, buy1_price)
                except Exception as exc:
                    log.error(f"❌ [ladder] {symbol} limit buy failed: {exc}")
                    return
                order_id = placed.get("id")
                if not order_id:
                    log.warning(f"⚠️ [ladder] {symbol} buy 1 returned no order id")
                    return
                now_ms = int(time.time() * 1000)
                state = ladder.LadderState(
                    symbol=symbol, signal_price=signal_price, signal_ts=now_ms,
                    state=ladder.PENDING_BUY_1,
                    buy_1=ladder.Leg(
                        label="buy_1", target_price=buy1_price,
                        size_usdt=self.ladder_buy1_size, order_id=str(order_id),
                    ),
                    pre_vol_baseline_usdt=float((features or {}).get("pre_vol_baseline_usdt") or 0),
                    dyn_buy1_offset_pct=dyn_buy1_off,
                    dyn_buy2_offset_pct=dyn_buy2_off,
                    dyn_tp_target_usdt=dyn_tp_tgt,
                    dyn_strategy=adapt['strategy'],
                )
                ladder.save_state(self.redis, state)
                log.info(f"🪜 [ladder] {symbol} buy 1 LIMIT @ {buy1_price} "
                         f"(-{dyn_buy1_off}% from signal {signal_price}) qty={buy1_qty} "
                         f"[strategy={adapt['strategy']}]")
                if self.metrics is not None:
                    audit = await self._compute_audit_snapshot(symbol, signal_price, buy1_price)
                    self.metrics.buy_placed(
                        symbol, buy1_price, self.ladder_buy1_size,
                        features=features, order_type="ladder_buy_1_offset_limit",
                        offset_pct=self.ladder_buy1_offset_pct,
                        **audit,
                    )
                return

            if self.ladder_buy1_market:
                # Fast-path: market order returns synchronously with fill
                # details, so we can place buys 2/3 + TP in the same call.
                buy1_qty = self.round_step(self.ladder_buy1_size / signal_price, f["step_size"])
                if (buy1_qty * signal_price) < f["min_notional"]:
                    log.info(f"⏸️  [ladder] {symbol} buy 1 notional too low")
                    return
                # Pre-flight: enough USDT for all 3 legs ($36 by default).
                total_needed = (self.ladder_buy1_size
                                + self.ladder_buy2_size + self.ladder_buy3_size)
                if not await self._has_sufficient_usdt(total_needed):
                    log.info(f"⏸️  [ladder] {symbol} skipped — funds short for full ladder")
                    return
                try:
                    placed = await self.client.create_market_buy_order(symbol, buy1_qty)
                except Exception as exc:
                    log.error(f"❌ [ladder] {symbol} market buy failed: {exc}")
                    return
                filled_qty = float(placed.get("filled") or buy1_qty)
                fill_price = float(placed.get("average") or placed.get("price") or signal_price)
                if filled_qty <= 0:
                    log.warning(f"⚠️ [ladder] {symbol} market buy returned filled=0")
                    return
                base_qty = self.round_step(filled_qty * 0.999, f["step_size"])
                now_ms = int(time.time() * 1000)
                state = ladder.LadderState(
                    symbol=symbol, signal_price=signal_price, signal_ts=now_ms,
                    state=ladder.ACTIVE_1,
                    buy_1=ladder.Leg(
                        label="buy_1", target_price=fill_price,
                        size_usdt=self.ladder_buy1_size, order_id=str(placed.get("id") or ""),
                        qty_filled=base_qty, fill_price=fill_price, fill_ts=now_ms,
                        status="filled",
                    ),
                    pre_vol_baseline_usdt=float((features or {}).get("pre_vol_baseline_usdt") or 0),
                )
                log.info(f"🪜 [ladder] {symbol} buy 1 MARKET filled qty={base_qty} @ {fill_price}")
                if self.metrics is not None:
                    audit = await self._compute_audit_snapshot(symbol, signal_price, fill_price)
                    self.metrics.buy_placed(
                        symbol, fill_price, self.ladder_buy1_size,
                        features=features, order_type="ladder_buy_1_market",
                        **audit,
                    )
                    self.metrics.fill_recorded(symbol, fill_price, base_qty)
                # Place legs 2, 3, and TP simultaneously
                await self._ladder_place_legs_after_buy1(state, f, tick)
                ladder.save_state(self.redis, state)
                return

            # Slow-path (aggressive limit at best ask) — kept for the case
            # where the operator wants spread protection.
            try:
                book = await self.client.fetch_order_book(symbol, limit=5)
                best_ask = float(book["asks"][0][0]) if book.get("asks") else signal_price
            except Exception:
                best_ask = signal_price
            buy1_price = self.round_step(best_ask, tick)
            buy1_qty = self.round_step(self.ladder_buy1_size / buy1_price, f["step_size"])
            if (buy1_qty * buy1_price) < f["min_notional"]:
                log.info(f"⏸️  [ladder] {symbol} buy 1 notional too low")
                return
            placed = await self.client.create_limit_buy_order(symbol, buy1_qty, buy1_price)
            order_id = placed.get("id")
            if not order_id:
                log.warning(f"⚠️ [ladder] {symbol} buy 1 returned no order id")
                return

            now_ms = int(time.time() * 1000)
            state = ladder.LadderState(
                symbol=symbol, signal_price=signal_price, signal_ts=now_ms,
                state=ladder.PENDING_BUY_1,
                buy_1=ladder.Leg(
                    label="buy_1", target_price=buy1_price,
                    size_usdt=self.ladder_buy1_size, order_id=str(order_id),
                ),
                pre_vol_baseline_usdt=float((features or {}).get("pre_vol_baseline_usdt") or 0),
            )
            ladder.save_state(self.redis, state)
            log.info(f"🪜 [ladder] {symbol} buy 1 LIMIT placed @ {buy1_price} qty={buy1_qty}")
            if self.metrics is not None:
                audit = await self._compute_audit_snapshot(symbol, signal_price, buy1_price)
                self.metrics.buy_placed(
                    symbol, buy1_price, self.ladder_buy1_size,
                    features=features, order_type="ladder_buy_1_limit",
                    **audit,
                )
        except Exception as exc:
            log.error(f"❌ [ladder] {symbol} buy 1 failed: {exc}")

    def _effective_tp_pct(self, state=None) -> float:
        """Returns the TP % to use right now. iter 43: if the ladder has
        a dynamic TP target stored (volatility-adaptive), use it. Else
        fall back to the static ladderTargetNetProfitUsdt → static %."""
        # iter 43: per-ladder dynamic TP target
        if state is not None and getattr(state, 'dyn_tp_target_usdt', 0) > 0:
            return ladder.required_tp_pct_for_net_profit(
                self.ladder_buy1_size,
                state.dyn_tp_target_usdt,
                self.ladder_fee_rate_per_side,
            )
        if self.ladder_target_net_usdt > 0:
            return ladder.required_tp_pct_for_net_profit(
                self.ladder_buy1_size,
                self.ladder_target_net_usdt,
                self.ladder_fee_rate_per_side,
            )
        return self.ladder_tp_from_avg_pct

    async def _ladder_place_legs_after_buy1(self, state, f, tick):
        """Helper called once Buy 1 has confirmed-filled. Places Buy 2,
        Buy 3 limits and the initial TP order. Used by both the market
        fast-path (in _ladder_start) and the polling slow-path (in
        _ladder_check_buy1_fill).

        2026-05-11 iter 6: reference price is now Buy 1's *actual fill*
        price, not the signal price. Buy 1 is a market order so it pays
        the spread — offsets calculated from the signal would be
        slightly off the actual entry. Using fill_price means the
        -0.5%/-1.0% offsets are exact relative to where we entered."""
        symbol = state.symbol
        # Reference = actual Buy 1 fill price (falls back to signal if
        # for some reason the fill price wasn't captured).
        ref_price = (state.buy_1.fill_price if state.buy_1 and state.buy_1.fill_price
                     else state.signal_price)
        # iter 43: use per-ladder dynamic Buy 2 offset when set
        eff_buy2_off = (state.dyn_buy2_offset_pct
                        if getattr(state, 'dyn_buy2_offset_pct', 0) > 0
                        else self.ladder_buy2_offset_pct)
        buy2_price = self.round_step(ref_price * (1 - eff_buy2_off / 100.0), tick)
        buy3_price = self.round_step(ref_price * (1 - self.ladder_buy3_offset_pct / 100.0), tick)
        buy2_qty = self.round_step(self.ladder_buy2_size / max(buy2_price, 1e-12), f["step_size"])
        buy3_qty = self.round_step(self.ladder_buy3_size / max(buy3_price, 1e-12), f["step_size"])

        buy2_oid = buy3_oid = None
        if (buy2_qty * buy2_price) >= f["min_notional"]:
            try:
                o2 = await self.client.create_limit_buy_order(symbol, buy2_qty, buy2_price)
                buy2_oid = str(o2.get("id"))
            except Exception as exc:
                log.warning(f"⚠️ [ladder] {symbol} buy 2 placement failed: {exc}")
        if (buy3_qty * buy3_price) >= f["min_notional"]:
            try:
                o3 = await self.client.create_limit_buy_order(symbol, buy3_qty, buy3_price)
                buy3_oid = str(o3.get("id"))
            except Exception as exc:
                log.warning(f"⚠️ [ladder] {symbol} buy 3 placement failed: {exc}")

        state.buy_2 = ladder.Leg(label="buy_2", target_price=buy2_price,
                                 size_usdt=self.ladder_buy2_size, order_id=buy2_oid)
        state.buy_3 = ladder.Leg(label="buy_3", target_price=buy3_price,
                                 size_usdt=self.ladder_buy3_size, order_id=buy3_oid)

        tp_pct = self._effective_tp_pct(state)  # iter 43: state-aware dyn TP
        avg_for_tp = state.weighted_avg()
        tp_p = ladder.tp_price(avg_for_tp, tp_pct, tick)
        await self._ladder_place_tp(state, state.total_qty(), tp_p, f)
        # iter 18 (2026-05-13): explicit logging of every input to the TP
        # math so we can verify the running container is using current
        # Redis values rather than stale defaults from a long-running
        # process. NEIRO/USDT today placed TP at +0.29% when target $0.40
        # net on $48 leg should have produced +0.98% — caused by stale
        # in-memory config that this restart forces a refresh of.
        log.info(f"🪜 [ladder] {symbol} TP-INPUTS avg={avg_for_tp:.8f} "
                 f"tp_pct={tp_pct:.4f}% buy1_size=${self.ladder_buy1_size} "
                 f"target_net=${self.ladder_target_net_usdt} "
                 f"fee_rate={self.ladder_fee_rate_per_side} → tp@{tp_p:.8f}")

    async def _drain_fill_events(self):
        """iter 33 (2026-05-14): consume Binance user-data-stream fill
        events that the frontend's binance-worker.js pushes to the Redis
        list BINANCE:FILL_EVENTS. These arrive via WebSocket push within
        a few ms of the actual fill on Binance, so reacting from this
        list gives us sub-second TP placement vs the previous polling
        which was ~1.2s best case (iter32) and up to 8min worst case
        (pre-iter32 — caused the ATOM 17:22→17:31 gap and the WLFI
        00:06→00:27 gap).

        For each event:
          - FILLED on a Buy 1 LIMIT belonging to a PENDING_BUY_1 ladder
            → mark state as ACTIVE_1 and place Buy 2 + TP immediately
          - FILLED on a TP order → handled later in this tick by the
            existing state-machine check (no extra work here)
          - CANCELED/EXPIRED on any tracked order → leave for the
            existing _handle_external_cancel detection
        """
        if not self.redis or ladder is None:
            return
        events_processed = 0
        # Drain up to 50 events per tick — usually 0 or 1.
        for _ in range(50):
            try:
                raw = self.redis.rpop("BINANCE:FILL_EVENTS")
            except Exception:
                break
            if not raw:
                break
            try:
                evt = json.loads(raw)
            except Exception:
                continue
            order_id = str(evt.get("orderId") or "")
            status = (evt.get("status") or "").upper()
            side = (evt.get("side") or "").upper()
            if status != "FILLED" or side != "BUY":
                # Only Buy fills need the immediate TP placement path.
                # Cancellations and sells are handled elsewhere.
                continue
            # Find the matching ladder by buy_1 order_id.
            states = ladder.load_all_states(self.redis)
            for state in states:
                if (state.state == ladder.PENDING_BUY_1
                        and state.buy_1
                        and state.buy_1.order_id == order_id):
                    try:
                        await self._on_buy1_fill_pushed(state, evt)
                    except Exception as exc:
                        log.warning(
                            f"⚠️ [ladder] WS fill handler failed for {state.symbol}: {exc} "
                            f"(falling back to polling)"
                        )
                    break
            events_processed += 1
        if events_processed:
            log.info(f"⚡ [ladder] drained {events_processed} WS fill event(s)")

    async def _on_buy1_fill_pushed(self, state, evt):
        """Mirror of _ladder_check_buy1_fill but driven by the WebSocket
        push event from the user data stream — no Binance REST call
        needed since the event already contains the fill price + qty.

        Transitions state to ACTIVE_1 and places Buy 2 + Buy 3 + TP."""
        symbol = state.symbol
        # Need market filters before placing follow-on legs.
        f = self.filters.get(symbol)
        if not f:
            await self.fetch_filters(symbol)
            f = self.filters.get(symbol)
        if not f:
            log.warning(f"⚠️ [ladder] {symbol} WS fill: no filters yet, skipping (will retry on poll)")
            return
        tick = f.get("tick_size") or 0.00000001

        # Extract fill data from the executionReport.
        try:
            fill_qty = float(evt.get("cumQty") or evt.get("fillQty") or 0)
            fill_price = float(evt.get("fillPrice") or state.buy_1.target_price or 0)
        except (TypeError, ValueError):
            fill_qty, fill_price = 0, 0
        if fill_qty <= 0 or fill_price <= 0:
            log.warning(f"⚠️ [ladder] {symbol} WS fill missing qty/price; falling back to polling")
            return

        # Mark the leg as filled (mirrors what _ladder_check_buy1_fill does)
        # and transition to ACTIVE_1. Then place Buy 2/3 + TP right away.
        base_qty = self.round_step(fill_qty * 0.999, f["step_size"])
        state.buy_1.qty_filled = base_qty
        state.buy_1.fill_price = fill_price
        state.buy_1.fill_ts = int(time.time() * 1000)
        state.buy_1.status = "filled"
        state.state = ladder.ACTIVE_1

        # iter 34: record fill BEFORE placing follow-on legs so the
        # OUTCOME hash reflects the real state even if leg placement
        # fails. The dashboard sees filled=1 immediately.
        if self.metrics is not None:
            try:
                self.metrics.fill_recorded(symbol, fill_price, base_qty)
            except Exception:
                pass

        await self._ladder_place_legs_after_buy1(state, f, tick)
        ladder.save_state(self.redis, state)
        log.info(
            f"⚡✅ [ladder] {symbol} buy 1 filled @ {fill_price} (qty={base_qty}) "
            f"— TP placed in same tick via WebSocket push"
        )

    async def _ladder_tick(self):
        """Called every loop iteration to drive ALL active ladders forward.
        Multi-ladder version: iterates every symbol with an in-flight
        state and runs the appropriate state-transition handler.

        iter 33: drains BINANCE:FILL_EVENTS first so push-based detection
        runs before polling. Polling stays as a safety net in case the
        frontend WebSocket drops or the Redis bridge has a hiccup."""
        if ladder is None: return
        # Push-based detection FIRST — handles fills within ms of receipt.
        await self._drain_fill_events()
        states = ladder.load_all_states(self.redis)
        if not states: return
        for state in states:
            if state.state == ladder.CLOSED: continue
            symbol = state.symbol
            try:
                f = self.filters.get(symbol)
                if not f:
                    await self.fetch_filters(symbol)
                    f = self.filters.get(symbol)
                if not f:
                    continue
                tick = f.get("tick_size") or 0.00000001

                # iter 14: time-based + break-even exit checks run BEFORE
                # the state-specific handlers so a stale ladder gets out
                # before another loop wastes a tick on TP polling.
                if await self._maybe_force_exit_ladder(state, f):
                    continue

                # iter 15: trailing-TP gate. Once price clears the static
                # TP we switch to trailing-mode; this method handles both
                # the switch and the trail-exit. If it returns True the
                # ladder is closed and we skip the rest of the tick.
                if await self._maybe_trailing_tp_exit(state, f):
                    continue

                # iter 16: pending-pump-dump cancel. If Buy 1 is a resting
                # LIMIT and price has pumped above limit then crashed back,
                # cancel before the limit fills into a falling knife.
                # (GIGGLE/USDT 2026-05-13 reference case.)
                if state.state == ladder.PENDING_BUY_1:
                    if await self._maybe_cancel_pending_pump_dump(state, f):
                        continue

                if state.state == ladder.PENDING_BUY_1:
                    await self._ladder_check_buy1_fill(state, f, tick)
                elif state.state == ladder.ACTIVE_1:
                    await self._ladder_check_buy2_or_buy3(state, f, tick)
                elif state.state == ladder.ACTIVE_2:
                    await self._ladder_check_buy3(state, f, tick)
                elif state.state == ladder.ACTIVE_3:
                    await self._ladder_check_hard_stop_or_tp(state, f, tick)
                # Update underwater accumulator (for TBE metric)
                await self._ladder_update_underwater(state, f)
            except Exception as exc:
                log.warning(f"⚠️ [ladder] tick failed for {symbol}: {exc}")

    def _check_market_stress_exit(self, state, current_price: float, avg: float,
                                   held_min: float, drop_pct: float):
        """iter 46 (2026-05-15) — Market Stress Exit.

        Fires BEFORE iter39 catastrophic (2.5%) when broader market
        signals confirm a downward drift. Built from the LINK/USDT
        2026-05-15 deep trace: at minute 130 LINK was -1.11%, BTC
        was -0.74%, and 1m volume spiked to 17× pre-signal baseline.
        iter39 didn't fire until minute 178 at -2.5% — we ignored 48
        minutes of clear warning.

        Three triggers (any one fires; held≥30min AND drop≥0.5% required):
          1. BTC weakness:    BTC down ≥ ms_btc_weakness_pct in last
                              ms_btc_lookback_min minutes
          2. Vol capitulation: 1m volume > ms_vol_spike_mult × baseline
                              AND that candle is red
          3. Red velocity:    ≥ ms_red_share_threshold of last 10
                              candles are red

        Returns (True, reason) when exit fires. The caller sells at
        market — actual loss will be drop_pct + slippage.
        """
        if not self.ms_enabled:
            return False, ""
        if held_min < self.ms_min_hold_min:
            return False, ""
        if drop_pct < self.ms_min_drop_pct:
            return False, ""

        # Trigger 1: BTC weakness over last ms_btc_lookback_min
        try:
            if self.klines_cache is not None and self.klines_cache.has("BTCUSDT", "1m"):
                # Slightly more than lookback so we have N candles to compare
                btc_df = self.klines_cache.get_klines("BTCUSDT", "1m",
                                                      self.ms_btc_lookback_min + 1)
                if btc_df is not None and not btc_df.empty and len(btc_df) >= 2:
                    btc_now = float(btc_df.iloc[-1]["close"])
                    btc_ago = float(btc_df.iloc[0]["close"])
                    if btc_ago > 0:
                        btc_chg = (btc_now / btc_ago - 1) * 100
                        if btc_chg <= -self.ms_btc_weakness_pct:
                            return True, (f"market_stress:btc_weakness "
                                          f"BTC={btc_chg:+.2f}% in {self.ms_btc_lookback_min}m "
                                          f"drop={drop_pct:.2f}% held={held_min:.0f}m")
        except Exception:
            pass

        # Triggers 2 + 3 share a klines fetch
        try:
            if self.klines_cache is not None and self.klines_cache.has(state.symbol, "1m"):
                df = self.klines_cache.get_klines(state.symbol, "1m", 10)
                if df is not None and not df.empty:
                    rows = df.to_dict("records")
                    baseline = state.pre_vol_baseline_usdt or 0.0

                    # Trigger 2: volume capitulation on a red candle
                    if rows and baseline > 0:
                        last = rows[-1]
                        last_vol_usdt = float(last["volume"]) * float(last["close"])
                        spike_ratio = last_vol_usdt / baseline
                        is_red = float(last["close"]) < float(last["open"])
                        if spike_ratio >= self.ms_vol_spike_mult and is_red:
                            return True, (f"market_stress:vol_capitulation "
                                          f"vol={spike_ratio:.1f}× baseline RED "
                                          f"drop={drop_pct:.2f}% held={held_min:.0f}m")

                    # Trigger 3: sustained red velocity
                    if len(rows) >= 10:
                        recent = rows[-10:]
                        red_count = sum(1 for r in recent
                                        if float(r["close"]) < float(r["open"]))
                        red_share = red_count / 10
                        if red_share >= self.ms_red_share_threshold:
                            return True, (f"market_stress:red_velocity "
                                          f"red_share={red_share*100:.0f}% "
                                          f"drop={drop_pct:.2f}% held={held_min:.0f}m")
        except Exception:
            pass

        return False, ""

    def _check_active2_monitor(self, state, current_price: float, avg: float,
                                drop_pct: float):
        """Iter 41 (2026-05-15) — Post-Buy-2 Careful Monitor.

        Only fires when state.state == ACTIVE_2 (Buy 2 filled, double
        exposure). Six priority-ordered checks; first match wins:

          1. GRACE PERIOD (first N min after Buy 2 fill):
             Skip all sub-checks below. Only the iter39 catastrophic
             floor (called later) can fire here. Gives the average-down
             time to work.

          2. QUICK PROFIT: current >= avg × (1 + a2_quick_profit_pct/100).
             Average-down succeeded — lock in any profit before it
             reverses. Default +0.2% above new avg.

          3. TIGHT BREAKEVEN: ever_underwater_since_buy2 AND current
             >= avg × (1 + a2_tight_breakeven_buffer_pct/100).
             Exit at flat the moment we touch the buffer (default 0.15%
             — JUST covers fees). Tighter than ACTIVE_1's 0.5% because
             after averaging down we should grab break-even when
             offered, not wait for more recovery that may not come.

          4. NO RECOVERY: held_since_buy2 >= a2_patience_minutes AND
             drop_pct >= a2_no_recovery_drop_pct AND no candle since
             Buy 2 fill reached avg × 0.999. Accept a minimal loss
             rather than letting it grow into a heavy one.

          5. HARD STOP: drop_pct >= a2_hard_stop_pct (default 1.5%).
             Cap the worst case. Tighter than iter37's fallback because
             ACTIVE_2 has 2× the exposure.

          6. Fall through — let iter39 / iter37 / iter14 run as normal
             safety nets.

        Returns (should_exit, reason). Caller is _maybe_force_exit_ladder.
        """
        if not self.a2_enabled:
            return False, ""
        if state.state != ladder.ACTIVE_2:
            return False, ""
        if not (state.buy_2 and state.buy_2.fill_ts and state.buy_2.fill_price):
            return False, ""

        now_ms = int(time.time() * 1000)
        held_since_b2_min = (now_ms - int(state.buy_2.fill_ts)) / 60_000.0

        # 1. GRACE PERIOD — patient first N min
        if held_since_b2_min < self.a2_grace_minutes:
            return False, ""

        # 2. QUICK PROFIT — exit on any positive return after grace
        qp_threshold = avg * (1 + self.a2_quick_profit_pct / 100.0)
        if current_price >= qp_threshold:
            return True, (f"active2_quick_profit "
                          f"current=${current_price:.6g} >= "
                          f"avg×(1+{self.a2_quick_profit_pct}%)=${qp_threshold:.6g} "
                          f"held={held_since_b2_min:.0f}m")

        # We need klines to evaluate #3 and #4. Pull last N+5 candles since
        # we want to look back to the Buy 2 fill timestamp.
        candles = None
        if self.klines_cache is not None and self.klines_cache.has(state.symbol, "1m"):
            try:
                df = self.klines_cache.get_klines(state.symbol, "1m", 60)
                if df is not None and not df.empty:
                    candles = df.to_dict("records")
            except Exception:
                pass

        # 3. TIGHT BREAKEVEN — ever underwater since Buy 2 + buffer hit
        tb_threshold = avg * (1 + self.a2_tight_breakeven_buffer_pct / 100.0)
        ever_underwater_since_b2 = False
        ever_recovered_since_b2 = False
        if candles:
            for c in candles:
                try:
                    ts = int(c.get("timestamp") or 0)
                    if ts < int(state.buy_2.fill_ts):
                        continue
                    if float(c.get("low") or 0) < avg:
                        ever_underwater_since_b2 = True
                    if float(c.get("high") or 0) >= avg * 0.999:
                        ever_recovered_since_b2 = True
                except Exception:
                    continue
        # Fall back to the ladder's general underwater flag if kline data
        # is missing — better than nothing.
        if not candles:
            ever_underwater_since_b2 = (state.total_underwater_ms or 0) > 0 or (state.below_avg_started_ts or 0) > 0
            ever_recovered_since_b2 = True   # assume yes when we can't tell

        if ever_underwater_since_b2 and current_price >= tb_threshold:
            return True, (f"active2_tight_breakeven "
                          f"current=${current_price:.6g} >= "
                          f"avg×(1+{self.a2_tight_breakeven_buffer_pct}%)=${tb_threshold:.6g}")

        # 4. NO RECOVERY — patient wait expired
        if (held_since_b2_min >= self.a2_patience_minutes
                and drop_pct >= self.a2_no_recovery_drop_pct
                and not ever_recovered_since_b2):
            return True, (f"active2_no_recovery "
                          f"held={held_since_b2_min:.0f}m drop={drop_pct:.2f}% "
                          f"never_touched_avg")

        # 5. HARD STOP — cap worst case (tighter than iter37)
        if (self.a2_hard_stop_pct > 0
                and drop_pct >= self.a2_hard_stop_pct):
            return True, (f"active2_hard_stop drop={drop_pct:.2f}% "
                          f">= {self.a2_hard_stop_pct}%")

        return False, ""

    def _check_liquidity_death(self, state, current_price: float, avg: float,
                               held_min: float, drop_pct: float):
        """Iter 39 (2026-05-15) — adaptive multi-factor hard-stop.

        Background: the QNT/HBAR/FLOKI forensic showed that the fixed
        1.5%-from-avg hard stop never fires on the real loss pattern:
        all three coins dropped only 0.36-0.63% but two were losers
        and one was a winner. The discriminating factor was VOLUME —
        winners had 2.6× pre-signal vol, losers had 0.28-0.63×.

        Three tiers:
          • Tier 1 (catastrophic): drop ≥ ld_catastrophic_drop_pct →
            instant exit, no further checks. Hard ceiling on loss.
          • Tier 2 (score-based): only when held ≥ ld_min_hold_min AND
            drop ≥ ld_min_drop_pct. Score points from vol-collapse,
            never-recovered, depth, lower-lows, red-share. Exit when
            score ≥ ld_exit_score_threshold.
          • Tier 3 (stagnation): held ≥ ld_stagnation_hold_min AND
            drop in [0, ld_stagnation_max_drop_pct] AND vol-collapsed
            AND not approaching TP. Frees stuck-but-not-dying capital
            (HBAR pattern: -0.28% for 5h).

        Returns (should_exit: bool, reason: str). On any internal
        failure returns (False, ""), letting the rest of the force-exit
        pipeline run as fallback.
        """
        if not self.ld_enabled:
            return False, ""
        if avg <= 0 or current_price <= 0:
            return False, ""

        # Tier 1: catastrophic — no diagnostics needed, just dump.
        if (self.ld_catastrophic_drop_pct > 0
                and drop_pct >= self.ld_catastrophic_drop_pct):
            return True, f"liquidity_death:catastrophic drop={drop_pct:.2f}%"

        # Below catastrophic floor: need klines to score liquidity.
        if self.klines_cache is None:
            return False, ""
        try:
            if not self.klines_cache.has(state.symbol, "1m"):
                return False, ""
            df = self.klines_cache.get_klines(state.symbol, "1m",
                                              self.ld_lookback_min)
        except Exception:
            return False, ""
        if df is None or df.empty or len(df) < max(3, self.ld_lookback_min // 2):
            return False, ""

        # Compute factors from the kline window.
        try:
            closes = df["close"].astype(float).tolist()
            opens = df["open"].astype(float).tolist()
            highs = df["high"].astype(float).tolist()
            lows = df["low"].astype(float).tolist()
            vols = df["volume"].astype(float).tolist()   # base-asset volume
        except Exception:
            return False, ""

        n = len(df)
        # Convert base-vol to USDT-equivalent using each candle's close.
        recent_vol_usdt = sum(v * c for v, c in zip(vols, closes))
        vol_per_min = recent_vol_usdt / n if n > 0 else 0.0

        baseline = state.pre_vol_baseline_usdt or 0.0
        vol_ratio = (vol_per_min / baseline) if baseline > 0 else 1.0

        lower_lows = sum(1 for i in range(1, n) if lows[i] < lows[i - 1])
        lower_lows_pct = lower_lows / max(1, n - 1)

        red = sum(1 for o, c in zip(opens, closes) if c < o)
        red_share = red / n

        max_high = max(highs)
        ever_recovered = max_high >= avg * 0.999    # touched breakeven since fill

        # Tier 2: score-based. Don't even score until we have a real
        # drop and the trade has had time to develop.
        tier2_eligible = (
            held_min >= self.ld_min_hold_min and
            drop_pct >= self.ld_min_drop_pct
        )
        if tier2_eligible:
            score = 0
            factors = []
            if vol_ratio < self.ld_vol_collapse_threshold:
                score += 3
                factors.append(f"vol={vol_ratio:.2f}x")
                # Extreme bonus when vol is HALF the already-low threshold.
                if vol_ratio < self.ld_vol_collapse_threshold * 0.5:
                    score += 2
                    factors.append("vol_extreme")
            if not ever_recovered:
                score += 2
                factors.append("never_recovered")
            if drop_pct >= 1.0:
                score += 2
                factors.append(f"drop={drop_pct:.2f}%")
            if lower_lows_pct >= self.ld_lower_lows_threshold:
                score += 2
                factors.append(f"ll={lower_lows_pct:.0%}")
            if red_share >= self.ld_red_share_threshold:
                score += 1
                factors.append(f"red={red_share:.0%}")

            if score >= self.ld_exit_score_threshold:
                reason = (f"liquidity_death:score={score} "
                          + " ".join(factors)
                          + f" drop={drop_pct:.2f}% held={held_min:.0f}m")
                return True, reason

        # Tier 3: stagnation — no drop big enough to score, but the
        # position has been sitting forever with no buyers.
        tier3_eligible = (
            held_min >= self.ld_stagnation_hold_min and
            0 <= drop_pct <= self.ld_stagnation_max_drop_pct and
            vol_ratio < self.ld_vol_collapse_threshold and
            # Price never came close to TP (peak / TP target ratio < 0.7).
            (state.tp_target_price <= 0
             or max_high / state.tp_target_price < 0.7)
        )
        if tier3_eligible:
            reason = (f"liquidity_death:stagnation "
                      f"held={held_min:.0f}m drop={drop_pct:.2f}% "
                      f"vol={vol_ratio:.2f}x no_tp_approach")
            return True, reason

        return False, ""

    async def _maybe_force_exit_ladder(self, state, f):
        # iter 94 — hard kill switch (manual-only sell mode).
        if self.HARD_DISABLE_AUTOSELL:
            try:
                price = getattr(state, "last_price", None) or getattr(state, "signal_price", None)
                await self._publish_blocked_sell(state.symbol, price, "force_exit_ladder", source="_maybe_force_exit_ladder")
            except Exception:
                pass
            log.warning(f"[ForceExit] {state.symbol} blocked — HARD_DISABLE_AUTOSELL=True (iter94)")
            return
        """Iter 14 time-based + break-even exit logic.

        Two conditions trigger a force exit at market on whatever has
        already been bought (buy_1 ± buy_2 ± buy_3 filled qty):

          1. BREAK-EVEN RECOVERY (ladderBreakevenExitEnabled):
             If the ladder went underwater at any point AND price has
             returned to weighted_avg × (1 + breakeven_buffer_pct/100)
             — exit now. Locks in ~$0 net rather than waiting hours
             for full TP which may never come.

          2. TIME EXIT (ladderTimeExitEnabled):
             If the ladder is older than ladderMaxHoldSeconds (default
             4h), force-sell at market regardless of P&L. Prevents
             positions sitting indefinitely while operator panic-sells.

        Returns True iff a force exit was executed (caller should
        ``continue`` to the next state). Returns False when the ladder
        is still in a normal state and the regular handlers should run.
        Best-effort: any unexpected error is swallowed.
        """
        if state is None or state.state in (ladder.CLOSED, ladder.PENDING_BUY_1):
            return False
        # iter 37: hard_stop_from_avg is also a valid reason to enter
        # this function. If all three exits are off, skip.
        if not (self.ladder_time_exit_enabled
                or self.ladder_breakeven_exit_enabled
                or self.ladder_hard_stop_from_avg_enabled):
            return False

        now_ms = int(time.time() * 1000)
        signal_ts = int(state.signal_ts or 0)
        age_sec = (now_ms - signal_ts) / 1000.0 if signal_ts else 0

        # Compute weighted avg from filled legs only.
        filled_legs = [L for L in (state.buy_1, state.buy_2, state.buy_3)
                       if L and L.qty_filled and L.fill_price]
        if not filled_legs:
            return False
        total_qty = sum(L.qty_filled for L in filled_legs)
        total_cost = sum(L.qty_filled * L.fill_price for L in filled_legs)
        avg = total_cost / total_qty if total_qty > 0 else 0
        if avg <= 0:
            return False

        # Current price — cheap ticker fetch.
        try:
            tk = await self.client.fetch_ticker(state.symbol)
            current_price = float(tk.get("last") or tk.get("close") or 0)
        except Exception:
            return False
        if current_price <= 0:
            return False

        # Break-even threshold: avg × (1 + buffer%). Covers spread + slippage.
        be_threshold = avg * (1 + self.ladder_breakeven_buffer_pct / 100.0)
        # iter 37: hard-stop-from-avg threshold. Floor below which we
        # market-sell to cap worst-case loss.
        hs_threshold = avg * (1 - self.ladder_hard_stop_from_avg_pct / 100.0)

        force_reason = None

        # Path -1 (iter 41, 2026-05-15): POST-BUY-2 CAREFUL MONITOR.
        # When the ladder has entered ACTIVE_2 (Buy 2 filled — double
        # exposure), use a specialised decision tree:
        #   1. grace period (no exits except catastrophic)
        #   2. quick profit (lock any +0.2% above new avg)
        #   3. tight breakeven (0.15% buffer instead of 0.5%)
        #   4. no-recovery exit (small loss after patience window)
        #   5. tighter hard stop (1.5% from new avg)
        # Falls through to iter39/iter37/iter14 if no tier fires.
        drop_pct_calc = (avg - current_price) / avg * 100
        a2_exit, a2_reason = self._check_active2_monitor(
            state, current_price, avg, drop_pct_calc,
        )
        if a2_exit:
            force_reason = a2_reason

        # Path -0.5 (iter 46, 2026-05-15): MARKET STRESS EXIT — earlier
        # than iter39 catastrophic. Fires when held≥30min AND drop≥0.5%
        # AND one of: BTC weakness, volume capitulation, or sustained
        # red velocity. Targets the LINK pattern where iter39 sat too
        # long while broader market gave clear sell signals.
        if force_reason is None:
            held_min_calc = ((now_ms - (state.buy_1.fill_ts if state.buy_1 else 0)) / 60_000.0
                              if state.buy_1 and state.buy_1.fill_ts else 0.0)
            ms_exit, ms_reason = self._check_market_stress_exit(
                state, current_price, avg, held_min_calc, drop_pct_calc,
            )
            if ms_exit:
                force_reason = ms_reason

        # Path 0-A (iter 39, 2026-05-15): LIQUIDITY-DEATH adaptive exit.
        # Smarter than iter37's fixed-pct hard stop. Combines drop %
        # with volume collapse, lower-lows, never-recovered flags, and
        # a stagnation tier for "dead-but-not-dropping" positions like
        # HBAR (sat -0.28% for 5h, vol ratio 0.28x). Always evaluated
        # first; falls through to the iter37 fixed stop if disabled or
        # if no tier fires.
        drop_pct = (avg - current_price) / avg * 100
        fill_ts = state.buy_1.fill_ts if state.buy_1 else 0
        held_min = ((now_ms - fill_ts) / 60_000.0) if fill_ts else 0.0
        ld_exit, ld_reason = self._check_liquidity_death(
            state, current_price, avg, held_min, drop_pct,
        )
        if ld_exit:
            force_reason = ld_reason

        # Path 0-B (iter 37): fixed-pct HARD STOP FROM AVG — fallback.
        # Only engages if the smarter liquidity-death exit didn't fire.
        # Retained as a belt-and-suspenders cap in case the kline
        # cache is unavailable or the operator disables iter39.
        if (force_reason is None
                and self.ladder_hard_stop_from_avg_enabled
                and self.ladder_hard_stop_from_avg_pct > 0
                and current_price <= hs_threshold):
            force_reason = "hard_stop_from_avg"

        # Path 1: break-even recovery. Only fires when we've gone
        # underwater AT ANY POINT in this ladder's life. The state flag
        # below_avg_started_ts is "currently underwater"; the more
        # interesting flag is "ever went underwater" — covered by
        # total_underwater_ms > 0 OR currently underwater.
        ever_underwater = (state.total_underwater_ms or 0) > 0 or (state.below_avg_started_ts or 0) > 0
        # iter 41: skip the generic iter14 breakeven exit when in ACTIVE_2 —
        # the post-Buy-2 monitor has a tighter version (0.15% vs 0.5%).
        if (force_reason is None
                and state.state != ladder.ACTIVE_2
                and self.ladder_breakeven_exit_enabled
                and ever_underwater):
            if current_price >= be_threshold:
                force_reason = "breakeven_recovery"

        # Path 2: hard time exit. Always wins over break-even path
        # because at this point we've waited long enough that whatever
        # P&L exists is what we get.
        if force_reason is None and self.ladder_time_exit_enabled:
            if age_sec >= self.ladder_max_hold_seconds:
                force_reason = "time_exit"

        if force_reason is None:
            return False

        # Cancel TP order if any
        if state.tp_order_id:
            try:
                await self.client.cancel_order(state.tp_order_id, state.symbol)
            except Exception:
                pass
            state.tp_order_id = None

        # Cancel any unfilled buy legs (buy_2/buy_3 limit orders)
        for leg in (state.buy_2, state.buy_3):
            if leg and leg.order_id and leg.status not in ("filled", "cancelled"):
                try:
                    await self.client.cancel_order(leg.order_id, state.symbol)
                except Exception:
                    pass
                leg.status = "cancelled"
                leg.order_id = None

        # Market-sell the entire filled qty (rounded down)
        sell_qty = self.round_step(total_qty * 0.999, f["step_size"])
        if sell_qty <= 0:
            log.warning(f"⚠️ [ladder] {state.symbol} force-exit qty rounded to 0; abandoning state")
            state.state = ladder.CLOSED
            state.exit_reason = force_reason
            state.closed_ts = now_ms
            ladder.clear_state(self.redis, state.symbol)
            ladder.set_cooldown(self.redis, state.symbol, self.ladder_cooldown_seconds)
            return True

        exit_price = current_price
        try:
            sold = await self.client.create_market_sell_order(state.symbol, sell_qty)
            exit_price = float(sold.get("average") or sold.get("price") or current_price)
            log.info(f"🚪 [ladder] {state.symbol} {force_reason} — sold {sell_qty} @ ~${exit_price:.6f}")
        except Exception as exc:
            log.error(f"❌ [ladder] {state.symbol} force-exit market sell failed: {exc}; clearing state anyway")

        # Compute P&L (gross — fees are deducted elsewhere by Binance)
        pnl = (exit_price - avg) * total_qty
        # Apply rough fee adjustment (2 × 0.075% BNB-discounted, both sides)
        pnl -= 2 * self.ladder_fee_rate_per_side * total_cost

        state.state = ladder.CLOSED
        state.exit_reason = force_reason
        state.closed_ts = now_ms
        if self.metrics is not None:
            try:
                # iter 35: fix arg-order bug. exit_recorded signature is
                # (symbol, fill_price, exit_price, reason, pnl). The bug
                # was passing (symbol, exit_price, sell_qty) which stored
                # the QTY in the exit_price OUTCOME field. DOGE today
                # showed exit_price=415 (the qty) in the dashboard.
                self.metrics.exit_recorded(state.symbol, avg, exit_price,
                                           reason=force_reason, pnl_usdt=pnl)
            except Exception:
                pass

        log.info(f"🪜 [ladder] {state.symbol} CLOSED reason={force_reason} "
                 f"net=${pnl:+.4f} age={age_sec/60:.1f}m avg=${avg:.6f} exit=${exit_price:.6f}")
        ladder.clear_state(self.redis, state.symbol)
        ladder.set_cooldown(self.redis, state.symbol, self.ladder_cooldown_seconds)
        return True

    async def _maybe_trailing_tp_exit(self, state, f):
        # iter 94 — hard kill switch (manual-only sell mode).
        if self.HARD_DISABLE_AUTOSELL:
            try:
                price = getattr(state, "last_price", None) or getattr(state, "signal_price", None)
                await self._publish_blocked_sell(state.symbol, price, "trailing_tp_exit", source="_maybe_trailing_tp_exit")
            except Exception:
                pass
            log.warning(f"[TrailingTP] {state.symbol} blocked — HARD_DISABLE_AUTOSELL=True (iter94)")
            return
        """Iter 15 trailing-TP gate.

        Activation: when current price first reaches state.tp_target_price
        (the static net-profit TP) we (a) cancel the limit TP order, (b)
        mark trailing_active=True, (c) seed peak_price_since_tp.

        Trailing: each tick, refresh peak_price_since_tp = max(peak, price).
        Exit at market when current_price <= peak × (1 - trailing_pct/100).

        Why this matters: the static $0.15-$0.40 TP captured ~10% of the
        available move on winners like ETHFI (+$0.18 net while +$1.84 was
        on the table). Trailing TP lets winners run — exits when momentum
        actually fades, not at an arbitrary fixed level.

        Returns True iff the ladder was closed via trail. Returns False
        when we either (a) haven't reached TP yet, (b) are trailing but
        the peak hasn't retraced, or (c) the feature is disabled.
        """
        if not self.ladder_trailing_tp_enabled:
            return False
        if state is None or state.state in (ladder.CLOSED, ladder.PENDING_BUY_1):
            return False
        if not state.tp_target_price or state.tp_target_price <= 0:
            return False
        filled_legs = [L for L in (state.buy_1, state.buy_2, state.buy_3)
                       if L and L.qty_filled and L.fill_price]
        if not filled_legs:
            return False
        total_qty = sum(L.qty_filled for L in filled_legs)
        if total_qty <= 0:
            return False

        try:
            tk = await self.client.fetch_ticker(state.symbol)
            current_price = float(tk.get("last") or tk.get("close") or 0)
        except Exception:
            return False
        if current_price <= 0:
            return False

        # Phase 1: not yet at TP — nothing to do; static limit TP handles it.
        if not state.trailing_active:
            if current_price < state.tp_target_price:
                return False
            # First tick at-or-above TP: cancel limit TP and switch to trailing.
            log.info(f"📈 [ladder] {state.symbol} TP {state.tp_target_price:.6f} reached "
                     f"@ {current_price:.6f} — switching to trailing-TP")
            if state.tp_order_id:
                try:
                    await self.client.cancel_order(state.tp_order_id, state.symbol)
                except Exception as exc:
                    log.warning(f"⚠️ [ladder] {state.symbol} failed to cancel limit TP "
                                f"({exc}); proceeding to trail anyway")
                state.tp_order_id = None
            state.trailing_active = True
            state.peak_price_since_tp = current_price
            ladder.save_state(self.redis, state)
            return False  # keep ladder alive in trailing mode

        # Phase 2: trailing — update peak and check trail-stop.
        if current_price > state.peak_price_since_tp:
            state.peak_price_since_tp = current_price
            ladder.save_state(self.redis, state)
            return False

        trail_threshold = state.peak_price_since_tp * (1 - self.ladder_trailing_tp_pct / 100.0)
        if current_price > trail_threshold:
            return False  # still inside the trail buffer

        # Trail-stop triggered → market sell.
        total_cost = sum(L.qty_filled * L.fill_price for L in filled_legs)
        avg = total_cost / total_qty
        sell_qty = self.round_step(total_qty * 0.999, f["step_size"])
        exit_price = current_price
        try:
            sold = await self.client.create_market_sell_order(state.symbol, sell_qty)
            exit_price = float(sold.get("average") or sold.get("price") or current_price)
        except Exception as exc:
            log.error(f"❌ [ladder] {state.symbol} trailing-TP market sell failed: {exc}")

        pnl = (exit_price - avg) * total_qty
        pnl -= 2 * self.ladder_fee_rate_per_side * total_cost
        now_ms = int(time.time() * 1000)
        state.state = ladder.CLOSED
        state.exit_reason = "trailing_tp"
        state.closed_ts = now_ms
        if self.metrics is not None:
            try:
                # iter 35: fix arg-order bug — see force-exit comment above.
                self.metrics.exit_recorded(state.symbol, avg, exit_price,
                                           reason="trailing_tp", pnl_usdt=pnl)
            except Exception:
                pass

        log.info(f"🎯 [ladder] {state.symbol} TRAILING-TP exit "
                 f"net=${pnl:+.4f} peak=${state.peak_price_since_tp:.6f} "
                 f"exit=${exit_price:.6f} avg=${avg:.6f}")
        ladder.clear_state(self.redis, state.symbol)
        ladder.set_cooldown(self.redis, state.symbol, self.ladder_cooldown_seconds)
        return True

    async def _maybe_cancel_pending_pump_dump(self, state, f):
        """Iter 16 (2026-05-13): cancel a resting Buy 1 LIMIT when price
        is showing a pump-then-dump pattern that would fill our limit
        into a falling knife.

        Trigger conditions (ALL must hold):
          - state is PENDING_BUY_1 (Buy 1 limit still resting)
          - age >= pendingMinAgeSeconds (don't trigger on noise)
          - peak_since_signal >= limit × (1 + pendingPumpThresholdPct/100)
            (price actually pumped meaningfully ABOVE our limit)
          - current_price <= peak × (1 - pendingDumpFromPeakPct/100)
            (price has now dropped from that peak)

        Reference case: 2026-05-13 GIGGLE/USDT 1m chart. Signal fired,
        Buy 1 LIMIT placed ~$35.75. Price pumped to $36.09 (+0.95% above
        limit) then crashed to $35.74 (-0.97% from peak). Without this
        filter, the limit would fill at $35.75 into a continuing
        downtrend → instant unrealized loss before TP can ever fire.

        Returns True iff the order was cancelled (caller skips fill check).
        Best-effort: any error returns False so the regular flow continues.
        """
        if not self.pending_pump_dump_enabled:
            return False
        if state is None or state.state != ladder.PENDING_BUY_1:
            return False
        if not state.buy_1 or not state.buy_1.order_id:
            return False
        limit_price = state.buy_1.target_price
        if limit_price <= 0:
            return False

        now_ms = int(time.time() * 1000)
        signal_ts = int(state.signal_ts or 0)
        age_sec = (now_ms - signal_ts) / 1000.0 if signal_ts else 0
        # Update peak first even if we don't trigger — useful for audit.
        try:
            tk = await self.client.fetch_ticker(state.symbol)
            current_price = float(tk.get("last") or tk.get("close") or 0)
        except Exception:
            return False
        if current_price <= 0:
            return False
        if current_price > state.peak_since_signal:
            state.peak_since_signal = current_price
            ladder.save_state(self.redis, state)

        # Gate 1: enough time elapsed so we're not reacting to a single
        # tick spike right after order placement.
        if age_sec < self.pending_min_age_seconds:
            return False

        # Gate 2: peak actually pumped meaningfully above the limit.
        pump_above_limit_pct = ((state.peak_since_signal - limit_price)
                                / limit_price * 100.0) if limit_price > 0 else 0
        if pump_above_limit_pct < self.pending_pump_threshold_pct:
            return False

        # Gate 3: price has dropped from the peak.
        drop_from_peak_pct = ((state.peak_since_signal - current_price)
                              / state.peak_since_signal * 100.0) if state.peak_since_signal > 0 else 0
        if drop_from_peak_pct < self.pending_dump_from_peak_pct:
            return False

        # All three gates fired — cancel the limit order.
        log.info(f"💥 [ladder] {state.symbol} pump-then-dump during PENDING_BUY_1: "
                 f"peak {state.peak_since_signal:.6f} (+{pump_above_limit_pct:.2f}% above "
                 f"limit {limit_price:.6f}), now {current_price:.6f} "
                 f"(-{drop_from_peak_pct:.2f}% from peak), age={age_sec:.0f}s — cancelling")
        try:
            await self.client.cancel_order(state.buy_1.order_id, state.symbol)
        except Exception as exc:
            log.warning(f"⚠️ [ladder] {state.symbol} cancel failed ({exc}); "
                        f"will rely on next tick or natural fill")

        state.buy_1.status = "cancelled"
        state.buy_1.order_id = None
        state.state = ladder.CLOSED
        state.exit_reason = "pending_pump_dump_cancel"
        state.closed_ts = now_ms

        if self.metrics is not None:
            try:
                # Record as a skip-style event so the dashboard sees why
                # the ladder ended without a fill.
                self.metrics.signal_skipped(
                    state.symbol, "pending_pump_dump",
                    f"limit {limit_price:.6f} pumped to {state.peak_since_signal:.6f} "
                    f"(+{pump_above_limit_pct:.2f}%) then dropped to {current_price:.6f} "
                    f"(-{drop_from_peak_pct:.2f}% from peak), age {age_sec:.0f}s",
                    {"limit_price": limit_price,
                     "peak": state.peak_since_signal,
                     "current_price": current_price,
                     "pump_pct": pump_above_limit_pct,
                     "drop_pct": drop_from_peak_pct,
                     "age_sec": int(age_sec)},
                )
            except Exception:
                pass
            # iter 34 (2026-05-14): ALSO write exited=1 to METRICS:OUTCOME
            # so the /metrics dashboard shows CANCELLED instead of PENDING
            # forever. Previous iters only called signal_skipped which
            # didn't touch the per-coin OUTCOME hash created by buy_placed.
            try:
                self.metrics.exit_recorded(
                    state.symbol, 0, 0,
                    reason="pending_pump_dump_cancel", pnl_usdt=0,
                )
            except Exception:
                pass

        ladder.clear_state(self.redis, state.symbol)
        ladder.set_cooldown(self.redis, state.symbol, self.ladder_cooldown_seconds)
        return True

    async def _ladder_check_buy1_fill(self, state, f, tick):
        """Slow-path: when Buy 1 was placed as a limit, poll for its fill.
        Once filled, delegate to the shared helper that places Buy 2/3 + TP."""
        if not state.buy_1 or not state.buy_1.order_id: return
        try:
            o = await self.client.fetch_order(state.buy_1.order_id, state.symbol)
        except Exception as exc:
            log.debug(f"[ladder] fetch buy 1 {state.symbol} failed: {exc}")
            return
        status = (o.get("status") or "").lower()
        # Manual cancel detection (operator cancelled on Binance UI)
        if status in ("canceled", "cancelled", "expired"):
            await self._handle_external_cancel(state, "buy 1")
            return
        if status != "closed":
            return
        filled_qty = float(o.get("filled") or 0)
        fill_price = float(o.get("average") or o.get("price") or state.buy_1.target_price)
        if filled_qty <= 0:
            log.warning(f"⚠️ [ladder] {state.symbol} buy 1 closed with zero qty")
            ladder.clear_state(self.redis, state.symbol)
            return

        base_qty = self.round_step(filled_qty * 0.999, f["step_size"])
        state.buy_1.qty_filled = base_qty
        state.buy_1.fill_price = fill_price
        state.buy_1.fill_ts = int(time.time() * 1000)
        state.buy_1.status = "filled"
        state.state = ladder.ACTIVE_1

        # iter 34 (2026-05-14): record fill BEFORE attempting to place
        # follow-on legs. If _ladder_place_legs_after_buy1 ever throws
        # (timeout, weird response, etc.) the fill itself is still
        # recorded — so the dashboard never shows PENDING for a coin
        # that was actually bought. The OUTCOME hash now matches reality.
        if self.metrics is not None:
            try:
                self.metrics.fill_recorded(state.symbol, fill_price, base_qty)
            except Exception:
                pass

        await self._ladder_place_legs_after_buy1(state, f, tick)
        ladder.save_state(self.redis, state)
        log.info(f"✅ [ladder] {state.symbol} buy 1 (limit) filled qty={base_qty} @ {fill_price}")

    async def _ladder_place_tp(self, state, qty_total, tp_price, filters):
        """Place a LIMIT SELL for entire filled qty at tp_price."""
        # iter 94 — hard kill switch (manual-only sell mode).
        if self.HARD_DISABLE_AUTOSELL:
            await self._publish_blocked_sell(state.symbol, tp_price, "ladder_place_tp", source="_ladder_place_tp")
            log.warning(
                f"[LadderTP] {state.symbol} qty={qty_total} tp={tp_price} blocked "
                f"— HARD_DISABLE_AUTOSELL=True (iter94)"
            )
            return None
        try:
            placed = await self.client.create_limit_sell_order(state.symbol, qty_total, tp_price)
            tp_oid = placed.get("id")
            state.tp_order_id = str(tp_oid) if tp_oid else None
            state.tp_target_price = tp_price
        except Exception as exc:
            log.warning(f"⚠️ [ladder] {state.symbol} TP placement failed: {exc}")
            state.tp_order_id = None
            state.tp_target_price = tp_price

    async def _ladder_refresh_tp(self, state, filters, tick):
        """Cancel old TP + place new TP at updated avg × (1 + tp_pct)."""
        if state.tp_order_id:
            try:
                await self.client.cancel_order(state.tp_order_id, state.symbol)
            except Exception:
                pass
        qty_total = state.total_qty()
        new_tp = ladder.tp_price(state.weighted_avg(), self._effective_tp_pct(state), tick)  # iter 43
        await self._ladder_place_tp(state, qty_total, new_tp, filters)

    async def _ladder_check_buy2_or_buy3(self, state, f, tick):
        """In ACTIVE_1 we watch both buy 2 and buy 3 limits and the TP order.
        - If TP fires → close out
        - If buy 2 fills → cancel buy 3, refresh TP, transition to ACTIVE_2
        - If buy 3 fills before buy 2 (gap) → transition to ACTIVE_3
        - If TP is manually cancelled → free slot, set cooldown"""
        # Check TP first
        if state.tp_order_id:
            try:
                tp_o = await self.client.fetch_order(state.tp_order_id, state.symbol)
                tp_status = (tp_o.get("status") or "").lower()
                if tp_status == "closed":
                    exit_price = float(tp_o.get("average") or tp_o.get("price") or state.tp_target_price)
                    await self._ladder_close(state, exit_price, ladder.EXIT_TP)
                    return
                if tp_status in ("canceled", "cancelled", "expired"):
                    await self._handle_external_cancel(state, "TP")
                    return
            except Exception:
                pass

        # Check buy 2
        if state.buy_2 and state.buy_2.order_id:
            try:
                o2 = await self.client.fetch_order(state.buy_2.order_id, state.symbol)
            except Exception:
                o2 = None
            o2_status = (o2.get("status") or "").lower() if o2 else ""
            if o2_status in ("canceled", "cancelled", "expired"):
                await self._handle_external_cancel(state, "buy 2")
                return

            # iter 37 (2026-05-15): Buy 2 staleness cancel.
            # If Buy 2 LIMIT is still open (not closed, not cancelled)
            # and more than N minutes have passed since Buy 1 filled,
            # cancel Buy 2. The retrace we were averaging-down for never
            # materialised. Pattern from forensic: FLOKI Buy 2 filled
            # in 2 min → winner; HBAR Buy 2 took 21 min → averaged-down
            # into a sustained downtrend and lost. Cancelling here keeps
            # Buy 1 + the existing TP order alive; when the TP refreshes
            # it'll target avg=Buy1 × (1+tp_pct) which is fully reachable.
            if (self.ladder_buy2_staleness_enabled
                    and o2 and o2_status not in ("closed", "filled")
                    and state.buy_1 and state.buy_1.fill_ts):
                staleness_ms = self.ladder_buy2_staleness_minutes * 60 * 1000
                age_since_buy1_fill = int(time.time() * 1000) - int(state.buy_1.fill_ts)
                if age_since_buy1_fill >= staleness_ms:
                    try:
                        await self.client.cancel_order(state.buy_2.order_id, state.symbol)
                    except Exception as exc:
                        log.warning(f"⚠️ [ladder] {state.symbol} stale buy 2 cancel failed: {exc}")
                    state.buy_2.status = "cancelled"
                    state.buy_2.order_id = None
                    # Also cancel Buy 3 (if any) — same logic, the
                    # retrace never came.
                    if state.buy_3 and state.buy_3.order_id:
                        try:
                            await self.client.cancel_order(state.buy_3.order_id, state.symbol)
                        except Exception:
                            pass
                        state.buy_3.status = "cancelled"
                        state.buy_3.order_id = None
                    # Refresh TP at the new (Buy 1 only) weighted avg so
                    # it actually fires at a price the market can reach.
                    try:
                        await self._ladder_refresh_tp(state, f, tick)
                    except Exception as exc:
                        log.warning(f"⚠️ [ladder] {state.symbol} TP refresh after stale buy 2 failed: {exc}")
                    ladder.save_state(self.redis, state)
                    log.info(f"⏱️  [ladder] {state.symbol} buy 2 cancelled — stale "
                             f"({age_since_buy1_fill/60000:.1f} min ≥ "
                             f"{self.ladder_buy2_staleness_minutes} min since Buy 1 fill); "
                             f"new TP={state.tp_target_price:.6g}")
                    return

            if o2 and o2_status == "closed":
                qty = float(o2.get("filled") or 0)
                price = float(o2.get("average") or o2.get("price") or state.buy_2.target_price)
                if qty > 0:
                    state.buy_2.qty_filled = self.round_step(qty * 0.999, f["step_size"])
                    state.buy_2.fill_price = price
                    state.buy_2.fill_ts = int(time.time() * 1000)
                    state.buy_2.status = "filled"
                    # CANCEL buy 3 per operator rule
                    if state.buy_3 and state.buy_3.order_id:
                        try:
                            await self.client.cancel_order(state.buy_3.order_id, state.symbol)
                        except Exception:
                            pass
                        state.buy_3.status = "cancelled"
                        state.buy_3.order_id = None
                    # Refresh TP at new avg
                    await self._ladder_refresh_tp(state, f, tick)
                    state.state = ladder.ACTIVE_2
                    ladder.save_state(self.redis, state)
                    log.info(f"📥 [ladder] {state.symbol} buy 2 filled @ {price} avg={state.weighted_avg():.6g} "
                             f"new TP={state.tp_target_price:.6g}; buy 3 cancelled")
                    if self.metrics is not None:
                        self.metrics.fill_recorded(state.symbol, price, state.buy_2.qty_filled)
                    return

        # Check buy 3 (gap scenario)
        if state.buy_3 and state.buy_3.order_id:
            try:
                o3 = await self.client.fetch_order(state.buy_3.order_id, state.symbol)
            except Exception:
                o3 = None
            o3_status = (o3.get("status") or "").lower() if o3 else ""
            if o3_status in ("canceled", "cancelled", "expired"):
                # Buy 3 only cancelled — that's fine, we can keep going on
                # the buy 1 (+ maybe buy 2) position. Mark it dead so we
                # don't poll it again.
                state.buy_3.status = "cancelled"
                state.buy_3.order_id = None
                ladder.save_state(self.redis, state)
                log.info(f"⚠️  [ladder] {state.symbol} buy 3 cancelled externally; continuing")
                return
            if o3 and o3_status == "closed":
                qty = float(o3.get("filled") or 0)
                price = float(o3.get("average") or o3.get("price") or state.buy_3.target_price)
                if qty > 0:
                    state.buy_3.qty_filled = self.round_step(qty * 0.999, f["step_size"])
                    state.buy_3.fill_price = price
                    state.buy_3.fill_ts = int(time.time() * 1000)
                    state.buy_3.status = "filled"
                    await self._ladder_refresh_tp(state, f, tick)
                    state.hard_stop_price = ladder.hard_stop_price(
                        price, self.ladder_hard_stop_pct, tick
                    )
                    state.state = ladder.ACTIVE_3
                    ladder.save_state(self.redis, state)
                    log.info(f"📥 [ladder] {state.symbol} buy 3 filled @ {price} (gap) avg={state.weighted_avg():.6g} "
                             f"new TP={state.tp_target_price:.6g} hard_stop={state.hard_stop_price:.6g}")
                    if self.metrics is not None:
                        self.metrics.fill_recorded(state.symbol, price, state.buy_3.qty_filled)
                    return

        ladder.save_state(self.redis, state)

    async def _ladder_check_buy3(self, state, f, tick):
        """In ACTIVE_2 we still watch the TP and buy 3 (in case it fills
        before we manage to cancel — race condition safety)."""
        # TP check
        if state.tp_order_id:
            try:
                tp_o = await self.client.fetch_order(state.tp_order_id, state.symbol)
                tp_status = (tp_o.get("status") or "").lower()
                if tp_status == "closed":
                    exit_price = float(tp_o.get("average") or tp_o.get("price") or state.tp_target_price)
                    await self._ladder_close(state, exit_price, ladder.EXIT_TP)
                    return
                if tp_status in ("canceled", "cancelled", "expired"):
                    await self._handle_external_cancel(state, "TP (ACTIVE_2)")
                    return
            except Exception:
                pass
        # Defensive: buy 3 lingering open
        if state.buy_3 and state.buy_3.order_id:
            try:
                o3 = await self.client.fetch_order(state.buy_3.order_id, state.symbol)
                o3_status = (o3.get("status") or "").lower()
                if o3_status in ("canceled", "cancelled", "expired"):
                    state.buy_3.status = "cancelled"
                    state.buy_3.order_id = None
                    ladder.save_state(self.redis, state)
                    log.info(f"⚠️  [ladder] {state.symbol} buy 3 (ACTIVE_2) cancelled externally")
                    return
                if o3_status == "closed":
                    qty = float(o3.get("filled") or 0)
                    price = float(o3.get("average") or o3.get("price") or state.buy_3.target_price)
                    if qty > 0:
                        state.buy_3.qty_filled = self.round_step(qty * 0.999, f["step_size"])
                        state.buy_3.fill_price = price
                        state.buy_3.fill_ts = int(time.time() * 1000)
                        state.buy_3.status = "filled"
                        await self._ladder_refresh_tp(state, f, tick)
                        state.hard_stop_price = ladder.hard_stop_price(
                            price, self.ladder_hard_stop_pct, tick
                        )
                        state.state = ladder.ACTIVE_3
                        ladder.save_state(self.redis, state)
                        log.info(f"📥 [ladder] {state.symbol} buy 3 race-filled @ {price} avg={state.weighted_avg():.6g}")
                        return
            except Exception:
                pass
        ladder.save_state(self.redis, state)

    async def _ladder_check_hard_stop_or_tp(self, state, f, tick):
        """In ACTIVE_3 we watch both TP order and hard-stop threshold."""
        if state.tp_order_id:
            try:
                tp_o = await self.client.fetch_order(state.tp_order_id, state.symbol)
                tp_status = (tp_o.get("status") or "").lower()
                if tp_status == "closed":
                    exit_price = float(tp_o.get("average") or tp_o.get("price") or state.tp_target_price)
                    await self._ladder_close(state, exit_price, ladder.EXIT_TP)
                    return
                if tp_status in ("canceled", "cancelled", "expired"):
                    await self._handle_external_cancel(state, "TP (ACTIVE_3)")
                    return
            except Exception:
                pass
        # Hard stop check (live ticker vs threshold)
        try:
            tk = await self.client.fetch_ticker(state.symbol)
            last = float(tk.get("last") or 0)
        except Exception:
            last = 0
        if last > 0 and state.hard_stop_price > 0 and last <= state.hard_stop_price:
            log.info(f"🛡️ [ladder] {state.symbol} HARD STOP @ {last:.6g} "
                     f"(threshold={state.hard_stop_price:.6g}, avg={state.weighted_avg():.6g})")
            # Cancel TP, market-sell everything
            if state.tp_order_id:
                try: await self.client.cancel_order(state.tp_order_id, state.symbol)
                except Exception: pass
            qty = state.total_qty()
            try:
                # Aggressive limit-at-bid sell for known price
                book = await self.client.fetch_order_book(state.symbol, limit=5)
                bid = float(book["bids"][0][0]) if book.get("bids") else last
                bid = self.round_step(bid, tick)
                sell = await self.client.create_limit_sell_order(state.symbol, qty, bid)
                exit_price = float(sell.get("average") or sell.get("price") or bid)
            except Exception as exc:
                log.warning(f"⚠️ [ladder] hard-stop sell failed for {state.symbol}: {exc}")
                exit_price = last
            await self._ladder_close(state, exit_price, ladder.EXIT_STOP)

    async def _ladder_update_underwater(self, state, filters):
        """Track time spent below weighted-avg for the TBE metric."""
        if state.state not in (ladder.ACTIVE_1, ladder.ACTIVE_2, ladder.ACTIVE_3):
            return
        if not state.filled_legs():
            return
        try:
            tk = await self.client.fetch_ticker(state.symbol)
            last = float(tk.get("last") or 0)
        except Exception:
            return
        if last <= 0: return
        avg = state.weighted_avg()
        now = int(time.time() * 1000)
        if last < avg:
            if state.below_avg_started_ts == 0:
                state.below_avg_started_ts = now
        else:
            # Recovered to break-even
            if state.below_avg_started_ts > 0:
                state.total_underwater_ms += (now - state.below_avg_started_ts)
                state.below_avg_started_ts = 0
                state.recovered_to_break_even = True
        ladder.save_state(self.redis, state)

    async def _ladder_close(self, state, exit_price, reason):
        """Finalise the ladder: record metrics, cancel pending limit-buys,
        clear state, free a slot."""
        # Cancel any still-pending Buy 2 / Buy 3 limit orders so they
        # don't fill after we've already exited (would leave us holding
        # naked qty with no TP/stop).
        for leg in (state.buy_2, state.buy_3):
            if leg and leg.order_id and leg.status not in ("filled", "cancelled"):
                try:
                    await self.client.cancel_order(leg.order_id, state.symbol)
                    leg.status = "cancelled"
                    leg.order_id = None
                    log.info(f"🚫 [ladder] {state.symbol} cancelled pending {leg.label} on close")
                except Exception:
                    pass

        # Flush any remaining underwater time
        if state.below_avg_started_ts > 0:
            state.total_underwater_ms += int(time.time() * 1000) - state.below_avg_started_ts
            state.below_avg_started_ts = 0
        state.state = ladder.CLOSED
        state.exit_reason = reason
        state.closed_ts = int(time.time() * 1000)
        summary = ladder.summarise_closed_trade(state, exit_price)

        # Standard metrics events
        if self.metrics is not None:
            avg = summary["weighted_avg"]
            net = summary["net_pnl_usdt"]
            if reason == ladder.EXIT_TP:
                self.metrics.tp_hit(state.symbol, avg, exit_price)
            self.metrics.exit_recorded(state.symbol, avg, exit_price,
                                       reason=reason, pnl_usdt=net)

        # Ladder-specific persistent record
        try:
            from datetime import datetime as _dt
            date = _dt.utcnow().strftime("%Y-%m-%d")
            key = f"METRICS:LADDER:{date}"
            self.redis.lpush(key, json.dumps(summary))
            self.redis.ltrim(key, 0, 999)
            self.redis.expire(key, 30 * 24 * 3600)
        except Exception:
            pass

        # Long-term archive
        try:
            archive_closed_trade(state.symbol, "FAST_LADDER", {
                "entry_price": summary["weighted_avg"],
                "exit_price": exit_price,
                "qty": summary["qty"],
                "investment": summary["invested_usdt"],
                "pnl_usdt": summary["net_pnl_usdt"],
                "pnl_pct": (
                    (exit_price - summary["weighted_avg"]) / summary["weighted_avg"] * 100.0
                ) if summary["weighted_avg"] else 0.0,
                "fees_paid": summary["fees_usdt"],
                "reason": f"LADDER_{reason.upper()}",
                "exit_time": datetime.now().strftime("%H:%M:%S"),
            })
        except Exception:
            pass

        log.info(f"🪜✅ [ladder] {state.symbol} CLOSED reason={reason} "
                 f"net=${summary['net_pnl_usdt']:+.4f} buys_filled={summary['buys_filled']} "
                 f"rer_recovered={summary['rer_recovered']} tbe_min={summary['tbe_minutes']:.1f}")
        ladder.clear_state(self.redis, state.symbol)
        # Per-coin cooldown — block re-entry on the same symbol
        ladder.set_cooldown(self.redis, state.symbol, self.ladder_cooldown_seconds)

    async def execute_buy(self, symbol, price, features=None):
        # iter 87 — hard kill switch (manual-only mode)
        if self.HARD_DISABLE_AUTOBUY:
            # iter 88 — publish signal so operator sees what scalper would have bought
            await self._publish_scalper_signal(symbol, price, features, source="execute_buy")
            log.debug(f"[fast-scalper] execute_buy {symbol} ignored — HARD_DISABLE_AUTOBUY=True")
            return
        if not self.auto_enabled:
            return

        # Route through the laddered recovery state machine when enabled.
        if await self._maybe_route_ladder(symbol, price, features=features):
            return

        if len(self.active_positions) >= 5: # Safety limit: Max 5 concurrent scalps
            return

        try:
            if symbol not in self.filters: await self.fetch_filters(symbol)
            f = self.filters[symbol]

            # ── LIMIT BUY entry (replaces market buy) ─────────────────
            # Per operator request 2026-05-10: enter via LIMIT at
            # limit_buy_offset_pct below signal price, wait up to
            # limit_buy_timeout_sec for fill, otherwise cancel and skip.
            # Set offset to 0 or negative to fall back to MARKET buy.
            order = None
            if self.limit_buy_offset_pct > 0:
                tick = f.get('tick_size') or 0.00000001
                limit_price = self.round_step(price * (1 - self.limit_buy_offset_pct / 100.0), tick)
                qty = self.round_step(self.buy_amount_usdt / limit_price, f['step_size'])
                if (qty * limit_price) < f['min_notional']:
                    return

                log.info(f"🛒 [SCALPER] LIMIT BUY {symbol} qty={qty} @ {limit_price} "
                         f"(-{self.limit_buy_offset_pct}% from {price}, timeout={self.limit_buy_timeout_sec}s)")
                placed = await self.client.create_limit_buy_order(symbol, qty, limit_price)
                order_id = placed.get('id')
                if not order_id:
                    log.warning(f"⚠️ LIMIT BUY {symbol} returned no order id")
                    return

                # Pre-signal volume baseline used by the fast-drop pattern
                # check (one number, captured at signal time inside the
                # MarketFeatures snapshot).
                signal_price = price
                signal_ts = time.time()
                vol_baseline = 0.0
                if features:
                    vol_baseline = float(features.get("pre_vol_baseline_usdt") or 0)

                # Poll for fill — also runs the fast-drop trajectory check
                # while we wait. If the bad pattern fires we cancel the
                # order before it can fill into a falling knife.
                deadline = signal_ts + self.limit_buy_timeout_sec
                fd_deadline = signal_ts + self.fd_detect_minutes * 60
                fd_cancelled = False
                while time.time() < deadline:
                    await asyncio.sleep(1)
                    o = await self.client.fetch_order(order_id, symbol)
                    if (o.get('status') or '').lower() == 'closed':
                        order = o
                        break

                    # Fast-drop check, only inside the detection window.
                    if (self.fd_enabled and vol_baseline > 0
                            and time.time() <= fd_deadline):
                        try:
                            tk = await self.client.fetch_ticker(symbol)
                            last = float(tk.get("last") or 0)
                        except Exception:
                            last = 0
                        if last > 0:
                            drop_pct = (last - signal_price) / signal_price * 100
                            if drop_pct <= -self.fd_threshold_pct:
                                # Price has bled past the threshold. Now
                                # measure live vol-1m vs the pre-signal
                                # baseline. If volume is NOT surging, we
                                # treat this as Pattern C (slow bleed).
                                try:
                                    rec = await self.client.fetch_ohlcv(symbol, "1m", limit=1)
                                except Exception:
                                    rec = []
                                vol_1m = 0.0
                                if rec:
                                    close = float(rec[-1][4] or 0)
                                    base_vol = float(rec[-1][5] or 0)
                                    if close and base_vol:
                                        vol_1m = close * base_vol
                                vol_ratio = (vol_1m / vol_baseline) if vol_baseline > 0 else 0
                                if vol_ratio < self.fd_vol_surge_mult:
                                    log.info(f"🔪🩸 [{symbol}] fast-drop {drop_pct:+.2f}% "
                                             f"vol={vol_ratio:.2f}x baseline → cancel")
                                    try:
                                        await self.client.cancel_order(order_id, symbol)
                                    except Exception:
                                        pass
                                    fd_cancelled = True
                                    if self.metrics is not None:
                                        reason = (f"fast-drop {drop_pct:+.2f}% within "
                                                  f"{self.fd_detect_minutes}m, vol "
                                                  f"{vol_ratio:.2f}x < {self.fd_vol_surge_mult}x")
                                        self.metrics.signal_skipped(
                                            symbol, "fast_drop_no_volume", reason,
                                            features or {},
                                        )
                                    break

                if fd_cancelled:
                    return

                if order is None:
                    # Cancel the unfilled (or partially-filled) order so we
                    # don't leak a resting bid sitting on the book.
                    try:
                        await self.client.cancel_order(order_id, symbol)
                    except Exception:
                        pass
                    log.info(f"⏰ LIMIT BUY {symbol} expired without fill — cancelled")
                    return
            else:
                # Fallback to market buy when offset is 0 or negative.
                qty = self.round_step(self.buy_amount_usdt / price, f['step_size'])
                if (qty * price) < f['min_notional']: return
                log.info(f"🛒 [SCALPER] MARKET BUY {symbol} qty={qty} @ {price} (offset disabled)")
                order = await self.client.create_market_buy_order(symbol, qty)

            # Per-fill audit. Binance buy orders often split across
            # several price levels — log each so the operator can
            # verify the bot's avg-price math against the actual fills.
            for i, fl in enumerate(order.get('fills') or []):
                log.info(
                    f"   fill[{i}] qty={fl.get('qty')} @ {fl.get('price')} "
                    f"fee={fl.get('commission')} {fl.get('commissionAsset')}"
                )

            # Read the exchange's response for the real fill numbers.
            #   'filled'   = total base asset filled, GROSS (before fees)
            #   'fills[]'  = per-trade fee breakdown — we subtract any
            #                fee taken in the base asset itself, since
            #                that's what Binance keeps and it's what
            #                makes the eventual sell qty match the wallet
            #
            # Earlier we trusted `filled` as net-of-fees, which is wrong:
            # Binance Spot deducts its 0.1 % fee from the base asset
            # received, so on a 68.35 TIA fill the wallet ends up holding
            # 68.28 TIA. Selling 68.35 then 422s with "insufficient
            # balance". This was the OCO-rejection root cause.
            exec_price = float(order.get('average') or order.get('price') or price)
            filled_qty_gross = float(order.get('filled') or 0.0)

            if filled_qty_gross <= 0:
                # Order accepted but didn't actually fill (rare on market
                # orders — usually means insufficient balance or the
                # exchange immediately cancelled). Don't record a phantom
                # position.
                log.warning(f"⚠️ Buy {symbol} returned filled=0; ignoring")
                return

            # Subtract base-asset fees. CCXT normalises Binance's `fills`
            # array; commissionAsset is in upper case for the symbol the
            # fee is taken from. For BTC/USDT the base is BTC.
            base_asset = symbol.split('/')[0] if '/' in symbol else symbol[:-4]
            base_asset = base_asset.upper()
            fee_in_base = 0.0
            for fill in (order.get('fills') or []):
                fee = fill.get('fee') or {}
                fee_ccy = (fee.get('currency') or fill.get('commissionAsset') or '').upper()
                fee_amt = float(fee.get('cost') or fill.get('commission') or 0.0)
                if fee_ccy == base_asset and fee_amt > 0:
                    fee_in_base += fee_amt

            filled_qty_net = filled_qty_gross - fee_in_base

            # Defence in depth: even after subtracting reported fees,
            # round down by step_size so we never oversell. If `fills`
            # was empty (some endpoints omit it) we still get a 0.1 %
            # safety buffer from the explicit floor below.
            sellable_qty = self.round_step(filled_qty_net, f['step_size'])
            if fee_in_base == 0 and sellable_qty == filled_qty_gross:
                # No fees reported — apply a 0.1 % safety buffer so the
                # OCO/sell quantity stays inside what we actually own.
                sellable_qty = self.round_step(filled_qty_gross * 0.999, f['step_size'])

            # Choose the resting-sell pattern based on whether stop-loss is
            # active. Option B (stopLossUsdt <= 0) means "patient hold, no
            # stop" — place a single TP-only LIMIT instead of OCO so the
            # exchange never auto-exits us at a loss.
            oco_list_id = None
            tp_order_id = None
            if self.stop_loss_usdt > 0:
                # Classic OCO (TP + SL legs) — exchange handles either.
                oco_list_id = await self._place_oco_sell(
                    symbol, sellable_qty, exec_price, f
                )
            else:
                # TP-only: a plain LIMIT SELL at the profit target.
                tp_order_id = await self._place_tp_only_limit(
                    symbol, sellable_qty, exec_price, f
                )

            self.active_positions[symbol] = {
                'buy_price':    exec_price,
                'qty':          sellable_qty,
                'oco_list_id':  oco_list_id,    # str or None
                'tp_order_id':  tp_order_id,    # str or None — Option B path
                'opened_ts':    time.time(),
            }
            self._persist_position(symbol)  # mirror to Redis for restart safety
            if oco_list_id:
                mode_tag = f"oco={oco_list_id}"
            elif tp_order_id:
                mode_tag = f"tp-only={tp_order_id}"
            else:
                mode_tag = "exit-pattern=FAILED→polling"
            log.info(f"✅ [SCALPER] Bought {symbol} qty={sellable_qty} @ {exec_price} {mode_tag}")
            if self.metrics is not None:
                size_usdt = exec_price * sellable_qty
                order_type = "limit" if self.limit_buy_offset_pct > 0 else "market"
                self.metrics.buy_placed(symbol, exec_price, size_usdt,
                                        features=features, order_type=order_type,
                                        offset_pct=self.limit_buy_offset_pct)
                self.metrics.fill_recorded(symbol, exec_price, sellable_qty)
                # Stamp the pre-signal volume baseline directly on the
                # OUTCOME hash so trajectory ticks can compute vol_ratio
                # without re-reading features.
                try:
                    pre_baseline = float((features or {}).get("pre_vol_baseline_usdt") or 0)
                    if pre_baseline > 0:
                        from datetime import datetime as _dt
                        date = _dt.utcnow().strftime("%Y-%m-%d")
                        outcome_key = f"METRICS:OUTCOME:{date}:{symbol.replace('/', '')}"
                        self.redis.hset(outcome_key, "pre_vol_baseline_usdt", pre_baseline)
                except Exception:
                    pass
        except Exception as e:
            log.error(f"❌ Buy {symbol} failed: {e}")

    async def _place_oco_sell(self, symbol: str, qty: float, entry_price: float, filters: dict):
        """Place a one-cancels-other sell on Binance (LIMIT TP + STOP_LOSS_LIMIT SL).

        Returns the orderListId on success, None on failure (caller
        falls back to the polling-based exit pattern).
        """
        # iter 94 — hard kill switch (manual-only sell mode).
        if self.HARD_DISABLE_AUTOSELL:
            await self._publish_blocked_sell(symbol, entry_price, "oco_sell", source="_place_oco_sell")
            log.warning(
                f"[OCO-SELL] {symbol} qty={qty} entry={entry_price} blocked "
                f"— HARD_DISABLE_AUTOSELL=True (iter94)"
            )
            return None
        # Prices derived from current config (when not hard-disabled):
        #     TP = entry × (1 + profit_target_usdt / buy_amount_usdt)
        #     SL trigger = entry × (1 - stop_loss_usdt / buy_amount_usdt)
        #     SL limit   = trigger × 0.998   (small buffer below trigger so
        #                                     the stop-limit can fill in
        #                                     fast-moving markets)
        if self.buy_amount_usdt <= 0:
            return None
        try:
            tick = filters.get('tick_size', 0.00000001) or 0.00000001
            tp_pct = self.profit_target_usdt / self.buy_amount_usdt
            sl_pct = self.stop_loss_usdt / self.buy_amount_usdt
            tp_price = self.round_step(entry_price * (1 + tp_pct), tick)
            sl_trigger = self.round_step(entry_price * (1 - sl_pct), tick)
            sl_limit = self.round_step(sl_trigger * 0.998, tick)

            # Sanity: TP must be strictly above entry, SL strictly below.
            if not (tp_price > entry_price > sl_trigger > sl_limit > 0):
                log.warning(
                    f"⚠️ [OCO] {symbol} bad price triplet: "
                    f"entry={entry_price} tp={tp_price} sl_trig={sl_trigger} sl_lim={sl_limit} — skipping OCO"
                )
                return None

            params = {
                'symbol':                 symbol.replace('/', ''),  # Binance native (no slash)
                'side':                   'SELL',
                'quantity':               str(qty),
                'price':                  str(tp_price),     # limit sell (TP leg)
                'stopPrice':              str(sl_trigger),    # SL trigger
                'stopLimitPrice':         str(sl_limit),      # SL limit price after trigger
                'stopLimitTimeInForce':   'GTC',
            }
            # ccxt exposes Binance's POST /api/v3/order/oco as private_post_order_oco.
            result = await self.client.private_post_order_oco(params)
            list_id = result.get('orderListId')
            if list_id is None:
                log.warning(f"⚠️ [OCO] {symbol} response missing orderListId: {result}")
                return None
            log.info(f"📋 [OCO] {symbol} placed list={list_id} TP={tp_price} SL={sl_trigger}/{sl_limit}")
            return str(list_id)
        except Exception as e:
            log.warning(f"⚠️ [OCO] placement failed for {symbol}: {e}")
            return None

    async def _check_oco_status(self, symbol: str, pos: dict) -> bool:
        """Poll OCO list status. Returns True if the position has been
        closed by the exchange (one leg filled, the other auto-cancelled)."""
        list_id = pos.get('oco_list_id')
        if not list_id:
            return False
        try:
            result = await self.client.private_get_orderlist({'orderListId': list_id})
        except Exception as e:
            log.debug(f"[OCO] {symbol} status check transient error: {e}")
            return False

        # Binance OCO list status:
        #   EXECUTING  – one or both orders still open
        #   ALL_DONE   – list resolved (filled or cancelled on both legs)
        #   REJECT     – never placed successfully
        list_status = (result.get('listOrderStatus') or '').upper()
        if list_status != 'ALL_DONE':
            return False

        # Identify which leg actually filled by inspecting the children.
        leg_summary = []
        for leg in result.get('orders', []) or []:
            try:
                child = await self.client.fetch_order(leg.get('orderId'), self._to_ccxt_symbol(symbol))
                leg_summary.append(f"{leg.get('orderId')}:{child.get('status')}:{child.get('filled')}")
            except Exception:
                continue

        log.info(f"✅ [OCO] {symbol} resolved by exchange — list={list_id} legs=[{', '.join(leg_summary)}]")
        self._drop_persisted_position(symbol)
        del self.active_positions[symbol]
        return True

    async def _cancel_oco(self, symbol: str, list_id: str) -> None:
        """Best-effort OCO cancel; absorbs failures so the caller can
        always proceed to a market sell."""
        if not list_id:
            return
        try:
            await self.client.private_delete_orderlist({
                'symbol':       symbol.replace('/', ''),
                'orderListId':  list_id,
            })
            log.info(f"🚫 [OCO] cancelled list={list_id} for {symbol}")
        except Exception as e:
            log.warning(f"⚠️ [OCO] cancel failed for {symbol} list={list_id}: {e}")

    # ── TP-only LIMIT (no SL leg) — Option B "patient hold" path ─────────
    async def _place_tp_only_limit(self, symbol: str, qty: float, entry_price: float, filters: dict):
        """Place a single LIMIT SELL at the profit-target price.

        Used when the operator has disabled stop-loss (stopLossUsdt <= 0).
        The exchange will fire whenever price reaches TP; we never get
        auto-exited at a loss. Cancellation on trend-reversal still works
        via the order_id we return.
        """
        if self.buy_amount_usdt <= 0 or self.profit_target_usdt <= 0:
            return None
        try:
            tick = filters.get('tick_size', 0.00000001) or 0.00000001
            tp_pct = self.profit_target_usdt / self.buy_amount_usdt
            tp_price = self.round_step(entry_price * (1 + tp_pct), tick)
            if not (tp_price > entry_price > 0):
                log.warning(f"⚠️ [TP-only] {symbol} bad TP price entry={entry_price} tp={tp_price}")
                return None
            order = await self.client.create_limit_sell_order(symbol, qty, tp_price)
            order_id = order.get('id')
            if order_id is None:
                log.warning(f"⚠️ [TP-only] {symbol} response missing order id: {order}")
                return None
            log.info(f"📋 [TP-only] {symbol} LIMIT SELL @ {tp_price} (no SL — patient hold)")
            return str(order_id)
        except Exception as e:
            log.warning(f"⚠️ [TP-only] placement failed for {symbol}: {e}")
            return None

    async def _check_tp_only_status(self, symbol: str, pos: dict) -> bool:
        """Poll a TP-only LIMIT SELL. Returns True if it filled (position
        closed and metrics/archive recorded)."""
        order_id = pos.get('tp_order_id')
        if not order_id:
            return False
        try:
            order = await self.client.fetch_order(order_id, symbol)
        except Exception as e:
            log.debug(f"[TP-only] {symbol} status check transient error: {e}")
            return False
        if (order.get('status') or '').lower() != 'closed':
            return False

        exit_price = float(order.get('average') or order.get('price') or 0.0)
        qty = float(pos.get('qty') or 0.0)
        buy_price = float(pos.get('buy_price') or 0.0)
        buy_value = buy_price * qty
        sell_value = exit_price * qty
        fees = (buy_value + sell_value) * 0.001
        net_pnl = (sell_value - buy_value) - fees

        if self.metrics is not None:
            self.metrics.tp_hit(symbol, buy_price, exit_price)
            self.metrics.exit_recorded(symbol, buy_price, exit_price,
                                       reason="tp", pnl_usdt=net_pnl)

        archive_closed_trade(symbol, "FAST", {
            "entry_price":  buy_price,
            "exit_price":   exit_price,
            "qty":          qty,
            "investment":   buy_value,
            "pnl_usdt":     net_pnl,
            "pnl_pct":      ((exit_price - buy_price) / buy_price * 100.0) if buy_price else 0.0,
            "fees_paid":    fees,
            "reason":       "TP_ONLY_FILLED",
            "exit_time":    datetime.now().strftime("%H:%M:%S"),
        })

        self._drop_persisted_position(symbol)
        del self.active_positions[symbol]
        log.info(f"💰 [{symbol}] TP-only filled @ {exit_price} → +${net_pnl:.4f}")
        return True

    async def _cancel_tp_only(self, symbol: str, order_id: str) -> None:
        """Best-effort cancel of a TP-only LIMIT SELL (used by trend-reversal
        exits and execute_sell)."""
        if not order_id:
            return
        try:
            await self.client.cancel_order(order_id, symbol)
            log.info(f"🚫 [TP-only] cancelled order={order_id} for {symbol}")
        except Exception as e:
            log.warning(f"⚠️ [TP-only] cancel failed for {symbol} order={order_id}: {e}")

    @staticmethod
    def _to_ccxt_symbol(symbol: str) -> str:
        return symbol if '/' in symbol else f"{symbol[:-4]}/USDT"

    async def execute_sell(self, symbol, price):
        # iter 94 — hard kill switch (manual-only sell mode).
        if self.HARD_DISABLE_AUTOSELL:
            await self._publish_blocked_sell(symbol, price, "execute_sell", source="execute_sell")
            log.warning(f"[execute_sell] {symbol} @ {price} blocked — HARD_DISABLE_AUTOSELL=True (iter94)")
            return
        if symbol not in self.active_positions: return
        try:
            pos = self.active_positions[symbol]
            qty = pos['qty']
            buy_price = pos.get('buy_price', 0.0) or 0.0
            list_id = pos.get('oco_list_id')
            tp_order_id = pos.get('tp_order_id')

            # If a resting sell is sitting on the exchange we must cancel
            # it before any new sell — otherwise the TP leg could fill in
            # parallel and we'd oversell what we own. Both OCO list IDs
            # and TP-only order IDs need clearing.
            if list_id:
                await self._cancel_oco(symbol, list_id)
            if tp_order_id:
                await self._cancel_tp_only(symbol, tp_order_id)

            # Replaces the previous market-sell. We never want unbounded
            # slippage — even on trend-reversal exits. The aggressive
            # limit places at the current best bid, which crosses the
            # spread and fills near-instantly at a *known* price. Up to
            # 3 retries (5 s each) before we give up and surface a
            # warning so the operator can intervene.
            sold = await self._place_aggressive_limit_sell(symbol, qty, max_wait_sec=5, retries=3)
            if sold is None:
                log.error(f"❌ [SCALPER] {symbol} aggressive-limit sell failed after retries — position retained for retry")
                return

            exit_price = float(sold.get('average') or sold.get('price') or price)

            # Long-term archive on the analyse Redis (best-effort).
            buy_value  = buy_price * qty
            sell_value = exit_price * qty
            buy_fee    = buy_value * 0.001
            sell_fee   = sell_value * 0.001
            net_pnl    = (sell_value - buy_value) - (buy_fee + sell_fee)
            archive_closed_trade(symbol, "FAST", {
                "entry_price": buy_price,
                "exit_price": exit_price,
                "qty": qty,
                "investment": buy_value,
                "pnl_usdt": net_pnl,
                "pnl_pct": ((exit_price - buy_price) / buy_price * 100.0) if buy_price else 0.0,
                "fees_paid": buy_fee + sell_fee,
                "reason": "SCALPER_EXIT",
                "exit_time": datetime.now().strftime("%H:%M:%S"),
            })

            if self.metrics is not None:
                # Tag wins/losses by sign of net PnL. We don't know whether
                # the exit was triggered by OCO TP, OCO SL, trend reversal,
                # or polling — mark as 'tp' only when net_pnl >= 0 so the
                # dashboard's win/loss counters stay honest.
                reason = "tp" if net_pnl >= 0 else "exit"
                if reason == "tp":
                    self.metrics.tp_hit(symbol, buy_price, exit_price)
                self.metrics.exit_recorded(symbol, buy_price, exit_price,
                                           reason=reason, pnl_usdt=net_pnl)

            self._drop_persisted_position(symbol)
            del self.active_positions[symbol]
        except Exception as e:
            log.error(f"❌ Sell {symbol} failed: {e}")

    async def _place_aggressive_limit_sell(self, symbol: str, qty: float, max_wait_sec: int = 5, retries: int = 3):
        # iter 94 — hard kill switch (manual-only sell mode).
        if self.HARD_DISABLE_AUTOSELL:
            await self._publish_blocked_sell(symbol, None, "aggressive_limit_sell", source="_place_aggressive_limit_sell")
            log.warning(f"[AggressiveSell] {symbol} qty={qty} blocked — HARD_DISABLE_AUTOSELL=True (iter94)")
            return None
        """Place a LIMIT SELL at the current best bid and wait up to
        max_wait_sec for it to fill. If it doesn't, cancel and try
        again with a freshly-fetched bid. Total attempts = retries.

        Returns the filled order dict on success, None on total failure.

        Why aggressive-limit instead of market:
          - Market orders take whatever liquidity is available, slipping
            through multiple price levels in fast-moving markets.
          - LIMIT-at-current-best-bid is a "marketable limit" — it crosses
            the spread and matches the bid book immediately, but the
            worst-case fill is bounded by the limit price itself.
          - The 5 s wait gives Binance time to match; the retry handles
            the case where the bid moved between fetch and place.

        Partial-fill safety (the AAVE/PENGU bug from this morning):
        if attempt N partially fills and we cancel, the wallet now
        owns LESS than the original `qty`. The next attempt MUST cap
        its qty at the actual free balance, otherwise Binance rejects
        with "insufficient balance" and the remainder ends up
        orphaned in the wallet. Each iteration re-fetches the free
        balance and rounds down to step_size before placing.
        """
        base_asset = symbol.split('/')[0] if '/' in symbol else symbol[:-4]
        base_asset = base_asset.upper()

        # Pull symbol filters once so we can step-round per attempt.
        if symbol not in self.filters:
            await self.fetch_filters(symbol)
        f = self.filters.get(symbol) or {}
        step_size = f.get('step_size') or 0.00000001

        accumulated_filled = 0.0
        last_fill_order = None
        remaining = qty

        for attempt in range(1, retries + 1):
            try:
                # 1) Re-anchor qty to whatever we actually own. After a
                #    partial fill on a previous attempt the wallet has
                #    shrunk, so we must read the current balance.
                bal = await self.client.fetch_balance()
                free = float((bal.get(base_asset) or {}).get('free') or 0)
                usable = self.round_step(min(remaining, free), step_size)
                if usable <= 0:
                    log.info(f"✅ [LIMIT-SELL] {symbol} nothing left to sell (free={free:.6f}) — done")
                    break

                # 2) Get the bid book and place the limit at best bid.
                book = await self.client.fetch_order_book(symbol, limit=5)
                bids = book.get('bids') or []
                if not bids:
                    log.warning(f"⚠️ [LIMIT-SELL] {symbol} attempt {attempt}: empty bid book — retrying")
                    await asyncio.sleep(1)
                    continue
                bid_price = float(bids[0][0])
                log.info(f"⚡ [SCALPER] {symbol} attempt {attempt}/{retries}: LIMIT SELL {usable} @ {bid_price} (free={free:.6f})")
                order = await self.client.create_limit_sell_order(symbol, usable, bid_price)
                order_id = order.get('id')
                if not order_id:
                    log.warning(f"⚠️ [LIMIT-SELL] {symbol} placement returned no order id")
                    continue

                # 3) Poll for fill.
                deadline = time.time() + max_wait_sec
                final = None
                while time.time() < deadline:
                    await asyncio.sleep(1)
                    o = await self.client.fetch_order(order_id, symbol)
                    if (o.get('status') or '').lower() == 'closed':
                        final = o
                        break

                if final is not None:
                    # Fully filled this attempt. Combine with any prior
                    # partial fills and return the latest order shape.
                    just_filled = float(final.get('filled') or usable)
                    accumulated_filled += just_filled
                    last_fill_order = final
                    log.info(f"✅ [LIMIT-SELL] {symbol} filled @ {final.get('average') or bid_price} "
                             f"(this attempt: {just_filled}, total filled across attempts: {accumulated_filled})")
                    return final

                # 4) Timeout. Read current state of the order to capture
                #    any partial fill before cancelling.
                try:
                    last = await self.client.fetch_order(order_id, symbol)
                    partial = float(last.get('filled') or 0)
                    if partial > 0:
                        accumulated_filled += partial
                        last_fill_order = last
                        remaining = max(0.0, remaining - partial)
                        log.warning(f"⏱  [LIMIT-SELL] {symbol} attempt {attempt} partial-filled {partial}, "
                                    f"remaining={remaining}")
                except Exception:
                    pass
                # Cancel whatever didn't fill.
                try:
                    await self.client.cancel_order(order_id, symbol)
                except Exception:
                    pass
                log.warning(f"⏱  [LIMIT-SELL] {symbol} attempt {attempt} timed out — cancelling and retrying")
            except Exception as e:
                log.warning(f"⚠️ [LIMIT-SELL] {symbol} attempt {attempt} error: {e}")
                await asyncio.sleep(1)

        # Loop finished without a clean full fill.
        if accumulated_filled > 0:
            log.warning(f"⚠️ [LIMIT-SELL] {symbol} exhausted retries with partial fills "
                        f"({accumulated_filled} of {qty}). Returning last partial order — "
                        f"caller should check actual balance.")
            return last_fill_order
        return None

    def broadcast_signal(self, symbol, status, price, matrix=None):
        if not self.redis: return
        payload = {
            "symbol": symbol, 
            "signal": status, 
            "price": price, 
            "matrix": matrix, 
            "ts": datetime.now().isoformat()
        }
        clean_symbol = symbol.replace("/", "")
        self.redis.set(f"{SIGNAL_PREFIX}{clean_symbol}", json.dumps(payload), ex=30)

    def round_step(self, qty, step):
        if not step: return float(int(qty))
        precision = str(Decimal(str(step)).normalize())
        p = int(precision.split('E-')[1]) if 'E-' in precision else len(precision.split('.')[1]) if '.' in precision else 0
        return float(Decimal(str(qty)).quantize(Decimal(str(10**-p)), rounding=ROUND_DOWN))

    async def start(self):
        # iter 31 (2026-05-13): diagnostic heartbeat written to Redis on
        # every iteration so we can confirm from outside whether the
        # main loop is alive and which stage it's reaching. Useful when
        # SCALPER:SIGNAL:* keys (TTL=30s) aren't appearing — tells us
        # if sync_config is the blocker vs the loop never starting.
        def _heartbeat(stage):
            try:
                if self.redis:
                    import time as _t
                    self.redis.set("SCALPER:HEARTBEAT", json.dumps({
                        "stage": stage,
                        "ts": int(_t.time() * 1000),
                        "config_loaded": bool(getattr(self, "config_loaded", False)),
                    }), ex=120)
            except Exception:
                pass

        _heartbeat("pre_initialize")
        await self.initialize()
        _heartbeat("post_initialize")

        last_symbol_refresh = 0
        while self.is_running:
            try:
                _heartbeat("loop_iter_start")
                # iter 22: STRICT Redis read. If any required key is
                # missing or Redis is unhealthy, sync_config raises and
                # we skip this cycle entirely — no trading actions run
                # with stale in-memory state.
                try:
                    await self.sync_config()
                    _heartbeat("sync_config_ok")
                except Exception as cfg_err:
                    log.error(f"❌ sync_config failed — bot will NOT trade this cycle: {cfg_err}")
                    try:
                        if self.redis:
                            self.redis.set("SCALPER:LAST_CONFIG_ERROR",
                                str(cfg_err)[:500], ex=300)
                    except Exception:
                        pass
                    self.config_loaded = False
                    await asyncio.sleep(5)
                    continue
                if not self.config_loaded:
                    log.warning("sync_config did not set config_loaded — skipping cycle")
                    await asyncio.sleep(5)
                    continue

                # Refresh symbols every 5 minutes
                if time.time() - last_symbol_refresh > 300:
                    await self.refresh_symbols()
                    last_symbol_refresh = time.time()

                btc_df = await self.get_indicators("BTC/USDT")
                if btc_df is None:
                    await asyncio.sleep(5)
                    continue

                # iter 32 (2026-05-13): ladder_tick used to run ONCE per
                # outer loop. With ~600 symbols × 5/batch × 1s sleep =
                # 2-8 min per full cycle, fill detection lagged by minutes.
                # ATOM/USDT today: Buy 1 LIMIT filled at 17:22:48 but the
                # TP wasn't placed until 17:31:00 — an 8m12s gap during
                # which the bot was holding qty with no exit on the book.
                #
                # Fix: run _ladder_tick BETWEEN every batch. With batch=5
                # and 1s sleep, that's a tick every ~1.2s. Now Buy 1 fill
                # → Buy 2/3 + TP placement happens within ~2 seconds.
                # Cost: ~3 extra Binance API calls per active ladder per
                # second (fetch_order on tp / buy_2 / buy_3). For 1 active
                # ladder that's 180 calls/min, well under the 1200 weight
                # budget.
                batch_size = 5
                for i in range(0, len(self.symbols), batch_size):
                    batch = self.symbols[i : i + batch_size]
                    tasks = [self.process_symbol(s, btc_df) for s in batch]
                    await asyncio.gather(*tasks)
                    # Drive the laddered-recovery state machine after each
                    # batch — keeps fill detection latency at ~1-2 seconds
                    # instead of 1-8 minutes.
                    if self.ladder_enabled and ladder is not None:
                        try:
                            await self._ladder_tick()
                        except Exception as exc:
                            log.warning(f"⚠️ ladder_tick raised: {exc}")
                    await asyncio.sleep(1.0) # Gentle spacing

            except Exception as e:
                log.error(f"🔥 Engine Error: {e}")
                await asyncio.sleep(5)

if __name__ == "__main__":
    import time
    scalper = MultiSymbolScalper()
    try:
        asyncio.run(scalper.start())
    except KeyboardInterrupt:
        pass
