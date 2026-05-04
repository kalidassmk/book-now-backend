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

# WebSocket-backed kline cache replaces fetch_ohlcv polling.
# When unavailable (older deployments), fall back to CCXT REST.
try:
    from klines_ws_cache import KlinesCache  # type: ignore
except Exception:
    KlinesCache = None  # type: ignore

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
CONFIG_KEY     = "booknow:config"
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
        self.auto_enabled = False
        self.buy_amount_usdt = 100.0
        self.profit_target_usdt = 0.20
        self.stop_loss_usdt = 0.50

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
            self.redis = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
            self.redis.ping()
            log.info("🔗 Connected to Redis.")
        except Exception as e:
            log.error(f"❌ Redis Connection Failed: {e}")

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
        """Fetch global dashboard settings."""
        if not self.redis: return
        try:
            raw = self.redis.get(CONFIG_KEY)
            if raw:
                cfg = json.loads(raw)
                self.auto_enabled = cfg.get("autoBuyEnabled", False)
                self.buy_amount_usdt = cfg.get("buyAmountUsdt", 100.0)
                self.profit_target_usdt = cfg.get("profitAmountUsdt", 0.20)
                self.stop_loss_usdt = 0.50
        except Exception:
            pass

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

    def evaluate_entry(self, symbol, df, btc_df):
        """Micro-Trend Momentum Acceleration Strategy."""
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

        # Signal Matrix
        matrix = {
            "btc_stable": bool(btc_ok),
            "trend_stacked": bool(uptrend),
            "micro_breakout": bool(breakout),
            "rsi_momentum": bool(rsi_ok),
            "volume_surge": bool(vol_ok),
            "bullish_candle": bool(bullish)
        }
        return all(matrix.values()), matrix

    async def process_symbol(self, symbol, btc_df):
        """Analyze and trade a single symbol."""
        # 1. Manage existing position
        if symbol in self.active_positions:
            df = await self.get_indicators(symbol)
            if df is None: return
            
            curr = df.iloc[-1]
            pos = self.active_positions[symbol]
            pnl = (curr['close'] - pos['buy_price']) * pos['qty']
            
            # Exit Conditions
            should_exit = False
            if pnl >= self.profit_target_usdt: 
                log.info(f"💰 [{symbol}] Profit Target Hit: +${pnl:.2f}")
                should_exit = True
            elif pnl <= -self.stop_loss_usdt:
                log.info(f"🛡️ [{symbol}] Stop Loss Hit: -${pnl:.2f}")
                should_exit = True
            elif curr['ema9'] < curr['ema21']:
                log.info(f"🔄 [{symbol}] Trend Reversal")
                should_exit = True
                
            if should_exit:
                await self.execute_sell(symbol, curr['close'])
            return

        # 2. Look for new entry
        df = await self.get_indicators(symbol)
        should_buy, matrix = self.evaluate_entry(symbol, df, btc_df)
        
        # Broadcast detailed matrix
        self.broadcast_signal(symbol, "BUY" if should_buy else "NEUTRAL", df.iloc[-1]['close'] if df is not None else 0, matrix)

        if should_buy:
            await self.execute_buy(symbol, df.iloc[-1]['close'])

    async def execute_buy(self, symbol, price):
        if not self.auto_enabled:
            return

        if len(self.active_positions) >= 5: # Safety limit: Max 5 concurrent scalps
            return

        try:
            if symbol not in self.filters: await self.fetch_filters(symbol)
            f = self.filters[symbol]
            
            qty = self.round_step(self.buy_amount_usdt / price, f['step_size'])
            if (qty * price) < f['min_notional']: return

            log.info(f"🛒 [SCALPER] Buying {symbol} @ {price}")
            order = await self.client.create_market_buy_order(symbol, qty)
            
            exec_price = float(order.get('average', order.get('price', price)))
            self.active_positions[symbol] = {'buy_price': exec_price, 'qty': qty}
        except Exception as e:
            log.error(f"❌ Buy {symbol} failed: {e}")

    async def execute_sell(self, symbol, price):
        if symbol not in self.active_positions: return
        try:
            qty = self.active_positions[symbol]['qty']
            log.info(f"⚡ [SCALPER] Selling {symbol} @ {price}")
            await self.client.create_market_sell_order(symbol, qty)
            del self.active_positions[symbol]
        except Exception as e:
            log.error(f"❌ Sell {symbol} failed: {e}")

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
