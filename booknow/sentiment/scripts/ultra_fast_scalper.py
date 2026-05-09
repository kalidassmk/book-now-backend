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
        self.auto_enabled = True
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
            self.redis = redis.Redis(
                host=os.getenv("REDIS_HOST", "127.0.0.1"),
                port=int(os.getenv("REDIS_PORT", "6379")),
                decode_responses=True,
            )
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
            self.buy_amount_usdt = float(cfg.get("buyAmountUsdt", 100.0))

            # Profit: prefer percentage if set, fall back to flat USDT.
            profit_pct = float(cfg.get("profitPct", 0.0) or 0.0)
            if profit_pct > 0:
                self.profit_target_usdt = self.buy_amount_usdt * profit_pct / 100.0
            else:
                self.profit_target_usdt = float(cfg.get("profitAmountUsdt", 0.20))

            # Stop loss: explicit USDT; if absent, default to 1 % of trade size.
            self.stop_loss_usdt = float(cfg.get("stopLossUsdt", self.buy_amount_usdt * 0.01))
        except Exception as e:
            log.warning("sync_config failed (continuing with last values): %s", e)

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
            # before market-selling — otherwise a TP/SL leg could fill
            # in parallel and we'd try to oversell what we own.
            if list_id:
                await self._cancel_oco(symbol, list_id)

            log.info(f"⚡ [SCALPER] Selling {symbol} @ {price}")
            await self.client.create_market_sell_order(symbol, qty)

            # Long-term archive on the analyse Redis (best-effort).
            buy_value  = buy_price * qty
            sell_value = price * qty
            buy_fee    = buy_value * 0.001
            sell_fee   = sell_value * 0.001
            net_pnl    = (sell_value - buy_value) - (buy_fee + sell_fee)
            archive_closed_trade(symbol, "FAST", {
                "entry_price": buy_price,
                "exit_price": price,
                "qty": qty,
                "investment": buy_value,
                "pnl_usdt": net_pnl,
                "pnl_pct": ((price - buy_price) / buy_price * 100.0) if buy_price else 0.0,
                "fees_paid": buy_fee + sell_fee,
                "reason": "SCALPER_EXIT",
                "exit_time": datetime.now().strftime("%H:%M:%S"),
            })

            self._drop_persisted_position(symbol)
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
