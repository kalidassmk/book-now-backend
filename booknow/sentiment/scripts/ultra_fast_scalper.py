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
        self.symbols = []
        self.active_positions = {} # symbol -> {buy_price, qty}

        # Dynamic Settings (Aligned with User Micro-Trend Strategy)
        # auto_enabled defaults to True — auto buy/sell is the intended
        # operational mode. Explicitly set "autoBuyEnabled": false in
        # booknow:config to pause without restarting the process.
        # 2026-05-10: switched to Option B sizing — $6 buys aiming at
        # $0.05 NET per win (gross $0.06 = 1 % of $6, after $0.012 in
        # round-trip Binance fees). If Redis TRADING_CONFIG is ever wiped,
        # these defaults take over — we never want a fallback to the old
        # $100 / $0.20 settings that blindsided the operator on 2026-05-09,
        # nor the intermediate $30 sizing the operator iterated past.
        self.auto_enabled = True
        self.buy_amount_usdt = 6.0
        self.profit_target_usdt = 0.06
        self.stop_loss_usdt = 0.06

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

    async def sync_config(self):
        """Fetch global dashboard settings from TRADING_CONFIG.

        Field semantics (mirrors what the dashboard writes):
          autoBuyEnabled    : bool  — gates new buys
          buyAmountUsdt     : float — investment per trade
          profitPct         : float — profit target as % of entry; takes
                                      priority when > 0 (matches the
                                      dashboard's UX)
          profitAmountUsdt  : float — flat USDT profit target; used only
                                      when profitPct == 0
          stopLossUsdt      : float — flat USDT loss before exit; falls
                                      back to 1 % of buyAmountUsdt if
                                      omitted, so older configs without
                                      this field still get a sane stop
        """
        if not self.redis: return
        try:
            raw = self.redis.get(CONFIG_KEY)
            if not raw:
                return
            cfg = json.loads(raw)
            self.auto_enabled    = bool(cfg.get("autoBuyEnabled", True))
            self.buy_amount_usdt = float(cfg.get("buyAmountUsdt", 6.0))

            # Profit: prefer percentage if set, fall back to flat USDT.
            # Defaults: profitPct = 1.0 % (Option B), legacy USDT = $0.05.
            profit_pct = float(cfg.get("profitPct", 1.0) or 1.0)
            if profit_pct > 0:
                self.profit_target_usdt = self.buy_amount_usdt * profit_pct / 100.0
            else:
                self.profit_target_usdt = float(cfg.get("profitAmountUsdt", 0.05))

            # Stop loss: explicit USDT; if absent, default to 1 % of trade size.
            self.stop_loss_usdt = float(cfg.get("stopLossUsdt", self.buy_amount_usdt * 0.01))

            # Market-context filters (added 2026-05-10 after PENGU/AAVE
            # post-mortem revealed the bot was buying coins already in
            # 24h downtrends). Set any to None / 0 to disable a filter.
            self.min_change_24h_pct = float(cfg.get("minChange24hPct", -1.0))
            self.min_range_24h_pct  = float(cfg.get("minRange24hPct", 5.0))
            self.min_vol_24h_usd    = float(cfg.get("minVol24hUsd", 2_000_000))

            # Limit-buy entry params (set offset to 0 or negative to fall
            # back to market buys). Same Redis key the dashboard already
            # writes for Virtual Scalper, so both engines use the same
            # offset and the operator only has one knob to tune.
            # Default 0.65 % matches Option B (2026-05-10 backtest).
            self.limit_buy_offset_pct = float(cfg.get("limitBuyOffsetPct", 0.65))
            self.limit_buy_timeout_sec = int(cfg.get("limitBuyTimeoutSec", 60))

            # Falling-knife filter (added 2026-05-10 after Option-B backtest
            # showed XEC/LUNC/LUMIA were all bought near a peak/pump).
            # Layered on top of the existing 24h market-context filter.
            self.fk_enabled = bool(cfg.get("fallingKnifeFilterEnabled", True))
            self.fk_max_change_24h_pct = float(cfg.get("maxChange24hPct", 8.0))
            self.fk_max_range_1h_pct = float(cfg.get("maxRange1hPct", 6.0))
            self.fk_overbought_skip = bool(cfg.get("overboughtSkipEnabled", True))
            self.fk_overbought_60m_pct = float(cfg.get("overbought60mPct", 1.5))
            if self.metrics is not None:
                self.metrics.enabled = bool(cfg.get("metricsEnabled", True))
        except Exception as e:
            log.warning("sync_config failed (continuing with last values): %s", e)

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

    def evaluate_entry(self, symbol, df, btc_df, ticker_24h=None):
        """Micro-Trend Momentum Acceleration Strategy + 24h market filter.

        ticker_24h is an optional dict with Binance's 24h ticker fields
        (percentage, high, low, quoteVolume). Pre-fetched by the caller
        in async context. None disables the 24h filter for this call.
        """
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

            df = await self.get_indicators(symbol)
            if df is None: return

            curr = df.iloc[-1]
            pnl = (curr['close'] - pos['buy_price']) * pos['qty']

            # 1b. Trend reversal exit always wins — even when OCO is
            # active. We cancel the resting orders so a later TP/SL fill
            # doesn't double-sell, then market-sell.
            if curr['ema9'] < curr['ema21']:
                log.info(f"🔄 [{symbol}] Trend Reversal")
                await self.execute_sell(symbol, curr['close'])
                return

            # 1c. Polling-based TP/SL — backup safety net for positions
            # whose OCO placement failed (oco_list_id is None). When OCO
            # is active we let Binance handle it for tighter timing.
            if not pos.get('oco_list_id'):
                if pnl >= self.profit_target_usdt:
                    log.info(f"💰 [{symbol}] Profit Target Hit (poll): +${pnl:.2f}")
                    await self.execute_sell(symbol, curr['close'])
                elif pnl <= -self.stop_loss_usdt:
                    log.info(f"🛡️ [{symbol}] Stop Loss Hit (poll): -${pnl:.2f}")
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
            await self.execute_buy(symbol, last_price, features=features)

    async def execute_buy(self, symbol, price, features=None):
        if not self.auto_enabled:
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

                # Poll for fill
                deadline = time.time() + self.limit_buy_timeout_sec
                while time.time() < deadline:
                    await asyncio.sleep(1)
                    o = await self.client.fetch_order(order_id, symbol)
                    if (o.get('status') or '').lower() == 'closed':
                        order = o
                        break

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

            # Place an OCO sell on the exchange so TP/SL fire the moment
            # price touches them — no polling delay, works even if the
            # bot is offline. If OCO placement fails the position falls
            # back to the polling-based exits in process_symbol.
            oco_list_id = await self._place_oco_sell(
                symbol, sellable_qty, exec_price, f
            )

            self.active_positions[symbol] = {
                'buy_price':    exec_price,
                'qty':          sellable_qty,
                'oco_list_id':  oco_list_id,    # str or None
                'opened_ts':    time.time(),
            }
            self._persist_position(symbol)  # mirror to Redis for restart safety
            mode_tag = f"oco={oco_list_id}" if oco_list_id else "oco=FAILED→polling"
            log.info(f"✅ [SCALPER] Bought {symbol} qty={sellable_qty} @ {exec_price} {mode_tag}")
            if self.metrics is not None:
                size_usdt = exec_price * sellable_qty
                order_type = "limit" if self.limit_buy_offset_pct > 0 else "market"
                self.metrics.buy_placed(symbol, exec_price, size_usdt,
                                        features=features, order_type=order_type,
                                        offset_pct=self.limit_buy_offset_pct)
                self.metrics.fill_recorded(symbol, exec_price, sellable_qty)
        except Exception as e:
            log.error(f"❌ Buy {symbol} failed: {e}")

    async def _place_oco_sell(self, symbol: str, qty: float, entry_price: float, filters: dict):
        """Place a one-cancels-other sell on Binance (LIMIT TP + STOP_LOSS_LIMIT SL).

        Returns the orderListId on success, None on failure (caller
        falls back to the polling-based exit pattern).

        Prices derived from current config:
            TP = entry × (1 + profit_target_usdt / buy_amount_usdt)
            SL trigger = entry × (1 - stop_loss_usdt / buy_amount_usdt)
            SL limit   = trigger × 0.998   (small buffer below trigger so
                                            the stop-limit can fill in
                                            fast-moving markets)
        """
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

    @staticmethod
    def _to_ccxt_symbol(symbol: str) -> str:
        return symbol if '/' in symbol else f"{symbol[:-4]}/USDT"

    async def execute_sell(self, symbol, price):
        if symbol not in self.active_positions: return
        try:
            pos = self.active_positions[symbol]
            qty = pos['qty']
            buy_price = pos.get('buy_price', 0.0) or 0.0
            list_id = pos.get('oco_list_id')

            # If an OCO is sitting on the exchange we must cancel it
            # before any sell — otherwise a TP/SL leg could fill in
            # parallel and we'd oversell what we own.
            if list_id:
                await self._cancel_oco(symbol, list_id)

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
        await self.initialize()
        
        last_symbol_refresh = 0
        while self.is_running:
            try:
                await self.sync_config()
                
                # Refresh symbols every 5 minutes
                if time.time() - last_symbol_refresh > 300:
                    await self.refresh_symbols()
                    last_symbol_refresh = time.time()

                btc_df = await self.get_indicators("BTC/USDT")
                if btc_df is None: 
                    await asyncio.sleep(5)
                    continue

                # Process in small batches to avoid Binance Rate Limits
                batch_size = 5
                for i in range(0, len(self.symbols), batch_size):
                    batch = self.symbols[i : i + batch_size]
                    tasks = [self.process_symbol(s, btc_df) for s in batch]
                    await asyncio.gather(*tasks)
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
