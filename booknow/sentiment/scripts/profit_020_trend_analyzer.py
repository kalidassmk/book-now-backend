import os
import redis
import json
import time
import logging
from datetime import datetime, timedelta
import ccxt

# Shared WebSocket cache for 24h tickers (replaces ccxt.fetch_tickers() polling).
# Falls back to REST CCXT below if the module is missing on this machine.
try:
    from tickers_ws_cache import get_default_cache
except Exception:
    get_default_cache = None  # type: ignore

# Multi-source kline fetcher (Binance → Bybit → OKX → KuCoin → CryptoCompare).
from klines_router import get_default_router as _get_klines_router

# ==========================================
# 1. LOGGING & CONFIG
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-7s | %(message)s')
log = logging.getLogger("Profit020Trend")

# Redis Configuration
LOCAL_REDIS = {
    'host': os.getenv("REDIS_HOST", "127.0.0.1"),
    'port': int(os.getenv("REDIS_PORT", "6379")),
    'db': 0,
    'decode_responses': True,
}

# Source & Destination Keys
PROFIT_HIT_KEY = "PROFIT_REACHED_020"
TREND_ANALYSIS_KEY = "ANALYSIS_020_TIMELINE"
BTC_REGIME_KEY = "BTC_REGIME"   # consumed by virtual_scalp_executor
MAX_HISTORY_POINTS = 100  # Keep last 100 snapshots per coin
MAX_TRACKING_HOURS = 4    # Stop tracking coins after 4 hours of inactivity

# BTC regime filter — when BTC drops more than this over the rolling
# 5-minute window, we treat the broader market as bearish and stop
# emitting SCALP_BUY_SIGNAL for everything else. Empirical: in the
# 2026-05-07 paper run, 13 of 16 losses fell inside an 18-minute window
# of correlated dump — exactly what this filter is meant to dodge.
BTC_DUMP_PCT_5M = -0.30
BTC_HISTORY_SECONDS = 360   # keep ~6 min so the 5-min lookup always has tail

# Falling-knife filters — a layered defence against the "buy a coin
# bouncing off a recent low while it's still in a multi-hour pullback"
# failure mode. Calibrated against three live losing trades on 2026-05-07
# (JTOUSDT, ONTUSDT, MANTAUSDT) — each filter is conservative enough to
# stay quiet on healthy pumps but at least one always catches the
# falling-knife pattern.
#
#  A1: 24h % change must be above this. Hard cutoff for coins in an
#      obvious daily downtrend.
#  A2: % below 24h high must be under this. A coin trading 8%+ below
#      its 24h high is in a meaningful pullback that needs a reversal
#      signal we don't yet have.
#  A3: Vol-change is bounded — below the upper bound to filter cold-start
#      cumulative-volume artifacts (e.g. 144,167% nonsense), above the
#      lower bound to require a real burst.
#  A4: Trend confirmation needs net upward drift across ~30s of history,
#      not just two ticks of bid noise.
#  A5: Distance above 24h low — the single most reliable feature across
#      the three losing trades. Coin pinned within a couple percent of
#      the daily low is the classic falling-knife setup.
FALLING_KNIFE_DAILY_CHANGE_PCT_MIN  = -3.0   # A1
FALLING_KNIFE_FROM_HIGH_PCT_MAX     =  8.0   # A2
FALLING_KNIFE_VOL_CHANGE_PCT_MIN    =   15.0 # A3 lower bound (real burst)
FALLING_KNIFE_VOL_CHANGE_PCT_MAX    =  500.0 # A3 upper bound (artifact cap)
FALLING_KNIFE_TREND_LOOKBACK        = 6      # A4 ~30s at 5s ticks
FALLING_KNIFE_TREND_MIN_DRIFT_PCT   = 0.05   # A4 net up-drift required
FALLING_KNIFE_ABOVE_LOW_PCT_MIN     = 2.0    # A5 must sit at least this far above 24h low

class Profit020TrendAnalyzer:
    def __init__(self):
        self.r = redis.Redis(**LOCAL_REDIS)
        # CCXT is kept around for the one-shot historical kline seed
        # (fetch_historical_context). The hot path — bulk ticker reads —
        # now goes through the WebSocket cache.
        self.ccxt = ccxt.binance({
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })
        self.tickers_cache = get_default_cache() if get_default_cache else None
        if self.tickers_cache:
            log.info("📡 Using TickersCache (!miniTicker@arr WS) — REST fetch_tickers disabled.")
        else:
            log.warning("⚠️ tickers_ws_cache not available — falling back to REST polling.")
        # Klines: multi-exchange router instead of direct ccxt.binance to spread rate-limit load.
        self.klines = _get_klines_router()
        # Rolling BTCUSDT samples for the 5-minute regime filter — list of
        # (ts, price). Trimmed every loop in update_btc_regime.
        self.btc_history = []

    def fetch_historical_context(self, ccxt_symbol):
        """Fetches last 10 minutes of 1m candles as initial context."""
        try:
            log.info(f"📜 Fetching 10m history for {ccxt_symbol}")
            ohlcv = self.klines.fetch_ohlcv(ccxt_symbol, timeframe='1m', limit=10)
            context = []
            for candle in ohlcv:
                ts, open_p, high, low, close, vol = candle
                context.append({
                    "time": datetime.fromtimestamp(ts/1000).strftime("%Y-%m-%d %H:%M:%S"),
                    "timestamp": ts / 1000,
                    "price": close,
                    "volume": vol,
                    "status": "historical",
                    "price_increase": close - open_p,
                    "volume_gradual_inc": False,
                    "sequence_report": {"type": "neutral", "vol_change": 0, "price_impact": 0},
                    "micro_signal": "NEUTRAL",
                    "prediction_confidence": 0,
                    "is_candle": True # Mark as 1m candle data
                })
            return context
        except Exception as e:
            log.error(f"Failed to fetch history for {ccxt_symbol}: {e}")
            return []

    def get_volume_trend(self, history):
        if len(history) < 5:
            return False
        volumes = [h['volume'] for h in history[-5:]]
        avg_prev = sum(volumes[:-1]) / (len(volumes) - 1)
        return volumes[-1] > avg_prev * 1.02

    def update_btc_regime(self):
        """Maintain rolling BTCUSDT samples and write the regime to Redis.

        Returns the current 5m % change (or None if not enough history yet).
        Both the analyzer (entry signal) and the executor (defensive secondary
        check) read this — keep the single producer here so the two stay
        in sync.
        """
        if not self.tickers_cache:
            return None
        t = self.tickers_cache.get_ticker('BTCUSDT')
        if not t or t.get('last') is None:
            return None
        now = time.time()
        price = float(t['last'])

        # Trim, then append. Trimming first means we never compare against
        # a sample we've already discarded.
        self.btc_history = [(ts, p) for ts, p in self.btc_history if now - ts <= BTC_HISTORY_SECONDS]
        self.btc_history.append((now, price))

        # Need at least one sample that is ~5 min old for a meaningful read.
        old_samples = [(ts, p) for ts, p in self.btc_history if now - ts >= 290]
        if not old_samples:
            return None
        # Use the oldest sample inside the window as the reference price.
        ref_price = old_samples[0][1]
        pct_5m = (price - ref_price) / ref_price * 100 if ref_price else 0.0
        blocking = pct_5m <= BTC_DUMP_PCT_5M

        try:
            self.r.set(BTC_REGIME_KEY, json.dumps({
                "pct_5m": round(pct_5m, 4),
                "ref_price": ref_price,
                "last_price": price,
                "ts": now,
                "blocking": blocking,
                "threshold": BTC_DUMP_PCT_5M,
            }))
        except Exception as e:
            log.error(f"Failed to write BTC_REGIME: {e}")
        return pct_5m

    def run(self):
        log.info("🚀 Optimized Profit 0.20 Trend Analyzer Started.")

        while True:
            try:
                # Refresh BTC regime first — every per-symbol path below
                # consumes the result. Failure to compute (cold start) is
                # treated as "not blocking" by downstream readers.
                btc_pct_5m = self.update_btc_regime()
                btc_blocking = btc_pct_5m is not None and btc_pct_5m <= BTC_DUMP_PCT_5M

                hits = self.r.hgetall(PROFIT_HIT_KEY)
                if not hits:
                    log.info("⏳ No profit hits found. Monitoring...")
                    time.sleep(10)
                    continue

                symbols = list(hits.keys())
                now_ts = time.time()

                # Resolve tickers without hitting Binance REST. The cache
                # is fed by a single !miniTicker@arr WebSocket subscription
                # that updates ~once per second for every spot symbol, so a
                # per-symbol lookup is O(1) and free.
                if self.tickers_cache is None:
                    try:
                        tickers_rest = self.ccxt.fetch_tickers()
                    except Exception as e:
                        log.error(f"Failed to fetch bulk tickers: {e}")
                        time.sleep(10)
                        continue
                else:
                    tickers_rest = None

                for symbol in symbols:
                    try:
                        ccxt_symbol = symbol if "/" in symbol else f"{symbol[:-4]}/{symbol[-4:]}"

                        if self.tickers_cache is not None:
                            t = self.tickers_cache.get_ticker(symbol)
                            if not t:
                                # Not seen yet (cold start) — skip this round; will arrive within ~1s.
                                continue
                            curr_price = t['last']
                            curr_vol   = t['quoteVolume']
                            daily_high = t['high']
                            daily_low  = t['low']
                            daily_open = t.get('open')
                        else:
                            if ccxt_symbol not in tickers_rest:
                                continue
                            ticker = tickers_rest[ccxt_symbol]
                            curr_price = ticker['last']
                            curr_vol = ticker['quoteVolume']
                            daily_high = ticker['high']
                            daily_low = ticker['low']
                            # ccxt normalises this as 'open' on most venues; fall back to None.
                            daily_open = ticker.get('open')

                        # --- FOMO FILTER: Avoid buying at the daily peak ---
                        # Calculate price position in 24h range (0 to 1.0)
                        range_size = daily_high - daily_low
                        price_pos = (curr_price - daily_low) / range_size if range_size > 0 else 0.5

                        # % change from daily low
                        from_low_pct = ((curr_price - daily_low) / daily_low) * 100 if daily_low > 0 else 0

                        # BLOCK BUY if price is in top 10% of daily range OR up > 15% from low
                        is_overheated = (price_pos > 0.9) or (from_low_pct > 15)

                        # --- Falling-knife metrics (A1, A2, A5) ---
                        # daily_change_pct uses the 24h-ago open from miniTicker —
                        # we cache it but the previous version of this code
                        # didn't read it.
                        daily_change_pct = (
                            ((curr_price - daily_open) / daily_open) * 100
                            if daily_open else None
                        )
                        from_high_pct = (
                            ((daily_high - curr_price) / daily_high) * 100
                            if daily_high else None
                        )
                        above_low_pct = from_low_pct  # alias — same number, expresses the A5 intent
                        
                        timeline_raw = self.r.hget(TREND_ANALYSIS_KEY, symbol)
                        timeline = json.loads(timeline_raw) if timeline_raw else []
                        
                        # --- INITIALIZATION: Fetch 10m History ---
                        if not timeline:
                            timeline = self.fetch_historical_context(ccxt_symbol)

                        if timeline:
                            last_ts = timeline[-1].get('timestamp', 0)
                            if now_ts - last_ts > (MAX_TRACKING_HOURS * 3600):
                                log.info(f"🧹 Removing inactive coin {symbol}")
                                self.r.hdel(PROFIT_HIT_KEY, symbol)
                                self.r.hdel(TREND_ANALYSIS_KEY, symbol)
                                continue

                        # --- LIVE SNAPSHOT ---
                        status = "stable"
                        price_change = 0.0
                        if timeline:
                            last_point = timeline[-1]
                            price_diff = curr_price - last_point['price']
                            price_change = round(price_diff, 8)
                            status = "increase" if price_diff > 0 else "decrease" if price_diff < 0 else "stable"
                        
                        volume_gradual_inc = self.get_volume_trend(timeline)
                        seq_report = {"type": "neutral", "vol_change": 0, "price_impact": 0}
                        micro_signal, confidence = "NEUTRAL", 0

                        # NOTE on the volume math: curr_vol is Binance's
                        # 24h rolling quoteVolume from miniTicker, which is
                        # monotonically non-decreasing for any actively
                        # traded coin — so v3>v2>v1>v0 is trivially true
                        # most of the time. The real filter is the magnitude
                        # check (vol_change > 15) which detects a sudden
                        # 24h-volume jump (i.e. a recent burst that's
                        # large relative to the prior 24h trail).
                        # A4: sustained-trend check across ~30s of timeline.
                        # The previous 2-tick check (`p3 > p2 ≥ p1`) was satisfied
                        # by routine bid noise, which is how the falling-knife
                        # losers slipped through. Use a real 6-sample window
                        # and require the cumulative drift to clear a 0.05%
                        # bar so we're not catching wiggle.
                        sustained_up = False
                        sustained_drift_pct = 0.0
                        if len(timeline) >= FALLING_KNIFE_TREND_LOOKBACK:
                            window = timeline[-FALLING_KNIFE_TREND_LOOKBACK:]
                            wp_first, wp_last = window[0]['price'], window[-1]['price']
                            if wp_first:
                                sustained_drift_pct = (wp_last - wp_first) / wp_first * 100
                                sustained_up = sustained_drift_pct >= FALLING_KNIFE_TREND_MIN_DRIFT_PCT

                        price_trend_up = False
                        if len(timeline) >= 3:
                            v0, v1, v2, v3 = timeline[-3]['volume'], timeline[-2]['volume'], timeline[-1]['volume'], curr_vol
                            p0, p1, p2, p3 = timeline[-3]['price'], timeline[-2]['price'], timeline[-1]['price'], curr_price

                            # Kept as a soft directional flag for context/logging,
                            # but no longer a primary buy gate — superseded by
                            # the sustained-trend window above.
                            price_trend_up = (p3 > p2) and (p3 >= p1)

                            if v3 > v2 > v1 > v0:
                                seq_report = {
                                    "type": "bullish_seq",
                                    "vol_change": ((v3-v0)/v0)*100 if v0 > 0 else 0,
                                    "price_impact": ((p3-p0)/p0)*100 if p0 > 0 else 0,
                                }
                                vol_change = seq_report['vol_change']
                                price_impact = seq_report['price_impact']

                                # Layered defence — ordered cheapest-first so
                                # the symbol gets the most specific reason
                                # code in its snapshot. Order doesn't change
                                # the buy/no-buy outcome.
                                if is_overheated:
                                    micro_signal, confidence = "OVERHEATED_NO_BUY", 10
                                elif btc_blocking:
                                    # Broad-market dump in progress — refuse
                                    # to take new longs. Existing positions
                                    # still exit via their own rules.
                                    micro_signal, confidence = "BTC_REGIME_NO_BUY", 5
                                elif daily_change_pct is not None and daily_change_pct < FALLING_KNIFE_DAILY_CHANGE_PCT_MIN:
                                    # A1: coin is in a 24h downtrend
                                    micro_signal, confidence = "DOWNTREND_NO_BUY", 8
                                elif from_high_pct is not None and from_high_pct > FALLING_KNIFE_FROM_HIGH_PCT_MAX:
                                    # A2: deep pullback from 24h high
                                    micro_signal, confidence = "DEEP_PULLBACK_NO_BUY", 8
                                elif above_low_pct < FALLING_KNIFE_ABOVE_LOW_PCT_MIN:
                                    # A5: pinned to the 24h low — keystone filter
                                    micro_signal, confidence = "NEAR_LOW_NO_BUY", 8
                                elif vol_change > FALLING_KNIFE_VOL_CHANGE_PCT_MAX:
                                    # A3: cold-start cumulative-volume artifact
                                    # (e.g. v0 captured at startup gave a
                                    # 144,167% change on JTOUSDT). Don't
                                    # trust the magnitude.
                                    micro_signal, confidence = "VOL_ARTIFACT_NO_BUY", 5
                                elif not sustained_up:
                                    # A4: only had 2-tick noise, not a real trend
                                    micro_signal, confidence = "WEAK_TREND_NO_BUY", 10
                                else:
                                    # All gates clear — this is a real signal.
                                    if (vol_change > FALLING_KNIFE_VOL_CHANGE_PCT_MIN
                                            and 0 < price_impact < 1.0):
                                        micro_signal, confidence = "SCALP_BUY_SIGNAL", 85
                                    elif vol_change > 5:
                                        micro_signal, confidence = "EARLY_ACCUMULATION", 60

                            elif v3 < v2 < v1 < v0:
                                seq_report = {
                                    "type": "bearish_seq",
                                    "vol_change": ((v3-v0)/v0)*100 if v0 > 0 else 0,
                                    "price_impact": ((p3-p0)/p0)*100 if p0 > 0 else 0,
                                }
                                if seq_report['vol_change'] < -10:
                                    micro_signal, confidence = "EXHAUSTION_EXIT", 90

                        snapshot = {
                            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "timestamp": now_ts,
                            "price": curr_price,
                            "volume": curr_vol,
                            "status": status,
                            "price_increase": price_change,
                            "volume_gradual_inc": volume_gradual_inc,
                            "sequence_report": seq_report,
                            "micro_signal": micro_signal,
                            "prediction_confidence": confidence,
                            "is_candle": False,
                            "daily_position": round(price_pos * 100, 2),  # % of 24h range
                            "is_overheated": is_overheated,
                            # Context fields used by the executor to log
                            # entry conditions on each opened position.
                            "price_trend_up": price_trend_up,
                            "btc_pct_5m": round(btc_pct_5m, 4) if btc_pct_5m is not None else None,
                            "btc_blocking": btc_blocking,
                            # Falling-knife filter telemetry — these are the
                            # numbers the A1/A2/A4/A5 gates above checked.
                            "daily_change_pct": round(daily_change_pct, 3) if daily_change_pct is not None else None,
                            "from_high_pct":    round(from_high_pct, 3)    if from_high_pct    is not None else None,
                            "above_low_pct":    round(above_low_pct, 3),
                            "sustained_drift_pct": round(sustained_drift_pct, 3),
                            "sustained_up":     sustained_up,
                        }
                        
                        timeline.append(snapshot)
                        if len(timeline) > MAX_HISTORY_POINTS:
                            timeline = timeline[-MAX_HISTORY_POINTS:]
                        
                        self.r.hset(TREND_ANALYSIS_KEY, symbol, json.dumps(timeline))
                        
                    except Exception as e:
                        log.error(f"Error processing {symbol}: {e}")
                        continue

                # Cache reads are free, but each loop iteration writes to
                # Redis per tracked symbol. 5s gives ~3× responsiveness vs
                # the old 15s REST-bounded cadence without measurable cost.
                time.sleep(5 if self.tickers_cache is not None else 15)
            except Exception as e:
                log.error(f"Main Loop Error: {e}")
                time.sleep(15)

if __name__ == "__main__":
    analyzer = Profit020TrendAnalyzer()
    analyzer.run()
