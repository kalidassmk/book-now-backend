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

# ==========================================
# 1. LOGGING & CONFIG
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-7s | %(message)s')
log = logging.getLogger("Profit020Trend")

# Redis Configuration
LOCAL_REDIS = {'host': 'localhost', 'port': 6379, 'db': 0, 'decode_responses': True}

# Source & Destination Keys
PROFIT_HIT_KEY = "PROFIT_REACHED_020"
TREND_ANALYSIS_KEY = "ANALYSIS_020_TIMELINE"
MAX_HISTORY_POINTS = 100  # Keep last 100 snapshots per coin
MAX_TRACKING_HOURS = 4    # Stop tracking coins after 4 hours of inactivity

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

    def fetch_historical_context(self, ccxt_symbol):
        """Fetches last 10 minutes of 1m candles as initial context."""
        try:
            log.info(f"📜 Fetching 10m history for {ccxt_symbol}")
            ohlcv = self.ccxt.fetch_ohlcv(ccxt_symbol, timeframe='1m', limit=10)
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

    def run(self):
        log.info("🚀 Optimized Profit 0.20 Trend Analyzer Started.")
        
        while True:
            try:
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
                        else:
                            if ccxt_symbol not in tickers_rest:
                                continue
                            ticker = tickers_rest[ccxt_symbol]
                            curr_price = ticker['last']
                            curr_vol = ticker['quoteVolume']
                            daily_high = ticker['high']
                            daily_low = ticker['low']
                        
                        # --- FOMO FILTER: Avoid buying at the daily peak ---
                        # Calculate price position in 24h range (0 to 1.0)
                        range_size = daily_high - daily_low
                        price_pos = (curr_price - daily_low) / range_size if range_size > 0 else 0.5
                        
                        # % change from daily low
                        from_low_pct = ((curr_price - daily_low) / daily_low) * 100 if daily_low > 0 else 0
                        
                        # BLOCK BUY if price is in top 10% of daily range OR up > 15% from low
                        is_overheated = (price_pos > 0.9) or (from_low_pct > 15)
                        
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

                        if len(timeline) >= 3:
                            v0, v1, v2, v3 = timeline[-3]['volume'], timeline[-2]['volume'], timeline[-1]['volume'], curr_vol
                            p0, p1, p2, p3 = timeline[-3]['price'], timeline[-2]['price'], timeline[-1]['price'], curr_price
                            
                            if v3 > v2 > v1 > v0:
                                seq_report = {"type": "bullish_seq", "vol_change": ((v3-v0)/v0)*100 if v0>0 else 0, "price_impact": ((p3-p0)/p0)*100 if p0>0 else 0}
                                
                                # Apply OVERHEATED FILTER to Buy Signals
                                if not is_overheated:
                                    if seq_report['vol_change'] > 15 and seq_report['price_impact'] < 0.3:
                                        micro_signal, confidence = "SCALP_BUY_SIGNAL", 85
                                    elif seq_report['vol_change'] > 5:
                                        micro_signal, confidence = "EARLY_ACCUMULATION", 60
                                else:
                                    micro_signal, confidence = "OVERHEATED_NO_BUY", 10
                                    
                            elif v3 < v2 < v1 < v0:
                                seq_report = {"type": "bearish_seq", "vol_change": ((v3-v0)/v0)*100 if v0>0 else 0, "price_impact": ((p3-p0)/p0)*100 if p0>0 else 0}
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
                            "daily_position": round(price_pos * 100, 2), # % of 24h range
                            "is_overheated": is_overheated
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
