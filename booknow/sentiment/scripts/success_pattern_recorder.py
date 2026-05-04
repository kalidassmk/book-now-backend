import redis
import json
import time
import logging
import ccxt
from datetime import datetime

# ==========================================
# 1. LOGGING & CONFIG
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-7s | %(message)s')
log = logging.getLogger("PatternRecorder")

# Redis Configuration
LOCAL_REDIS = {'host': 'localhost', 'port': 6379, 'db': 0, 'decode_responses': True}
REMOTE_REDIS = {
    'host': 'redis-18144.c89.us-east-1-3.ec2.cloud.redislabs.com',
    'port': 18144,
    'password': 'Gn9jKtL0SBkMLYynSjXbblmkjkIGrdPS',
    'decode_responses': True
}

# Intervals to capture
INTERVALS_SEC = [1, 2, 5, 10, 20, 30, 45, 50]
INTERVALS_MIN = [1, 2, 3, 5, 10, 15, 20, 30, 45]
INTERVALS_HOUR = [1, 2, 3, 5]

# Source Keys
PROFIT_HIT_KEY = "PROFIT_REACHED_020"
CONSENSUS_KEY = "FINAL_CONSENSUS_STATE"
REGIME_KEY = "REGIME_STATE"
VOLUME_KEY = "VOLUME_SCORE"
BTC_KEY = "BTC_CORRELATION_FILTERS"

# Destination Keys (AnalyseDB)
ANALYSE_DB_KEY = "ANALYSE_DB"
PENDING_FOLLOWUP_KEY = "PENDING_FOLLOWUP_1H"

class SuccessPatternRecorder:
    """
    Analyzes and stores the market DNA of coins that reached the $0.20 profit milestone.
    Now includes a 1-hour follow-up analysis to track post-success performance.
    """
    def __init__(self):
        # Connection for live market data (Local DB 0)
        self.r_source = redis.Redis(**LOCAL_REDIS)
        # Dedicated connection for Pattern Storage (Remote Redis Cloud)
        self.r_analyse = redis.Redis(**REMOTE_REDIS)
        
        # CCXT Client
        self.ccxt = ccxt.binance({
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })
        
        self.processed_hits = set()

    def run(self):
        log.info("🧠 Success Pattern Recorder Online. Monitoring 1h Post-Success Performance...")
        
        while True:
            try:
                # ─── PART 1: Capture New Success Hits ──────────────────────
                hits = self.r_source.hgetall(PROFIT_HIT_KEY)
                for symbol, hit_json in hits.items():
                    hit_data = json.loads(hit_json)
                    hit_id = f"{symbol}_{hit_data.get('reachedAt')}"

                    if hit_id not in self.processed_hits:
                        pattern = self.capture_pattern(symbol, hit_data)
                        if pattern:
                            self.r_analyse.hset(ANALYSE_DB_KEY, hit_id, json.dumps(pattern))
                            # Add to pending queue for 1h follow-up
                            self.r_analyse.hset(PENDING_FOLLOWUP_KEY, hit_id, json.dumps({
                                "symbol": symbol,
                                "timestamp": time.time(),
                                "price_at_hit": hit_data.get('currentPrice')
                            }))
                            self.processed_hits.add(hit_id)
                            log.info(f"💾 [SUCCESS] {symbol} recorded. Scheduled 1h follow-up.")

                # ─── PART 2: Process 1h Follow-ups ─────────────────────────
                self.process_followups()

                time.sleep(10)
            except Exception as e:
                log.error(f"Loop Error: {e}")
                time.sleep(5)

    def process_followups(self):
        """Checks for coins that hit the target >1 hour ago and records their current state."""
        pending = self.r_analyse.hgetall(PENDING_FOLLOWUP_KEY)
        now = time.time()

        for hit_id, data_json in pending.items():
            data = json.loads(data_json)
            # Check if 1 hour (3600s) has passed
            if now - data['timestamp'] >= 3600:
                symbol = data['symbol']
                log.info(f"🕒 [FOLLOW-UP] 1 hour reached for {symbol}. Capturing final state...")
                
                try:
                    # Fetch current price/volume
                    ticker = self.ccxt.fetch_ticker(symbol.replace("USDT", "/USDT"))
                    curr_price = ticker['last']
                    curr_vol = ticker['quoteVolume']
                    
                    # Update the record in AnalyseDB
                    pattern_raw = self.r_analyse.hget(ANALYSE_DB_KEY, hit_id)
                    if pattern_raw:
                        pattern = json.loads(pattern_raw)
                        pattern["performance_after_1h"] = {
                            "price": curr_price,
                            "volume_24h": curr_vol,
                            "gain_since_hit_pct": round(((curr_price / data['price_at_hit']) - 1) * 100, 2),
                            "captured_at": datetime.now().isoformat()
                        }
                        self.r_analyse.hset(ANALYSE_DB_KEY, hit_id, json.dumps(pattern))
                        
                    # Remove from pending queue
                    self.r_analyse.hdel(PENDING_FOLLOWUP_KEY, hit_id)
                    log.info(f"✅ [FOLLOW-UP COMPLETE] {symbol} updated with 1h performance data.")

                except Exception as e:
                    log.error(f"Follow-up failed for {symbol}: {e}")

    def capture_pattern(self, symbol, hit_metadata):
        """
        Gathers a comprehensive snapshot of the market state for the winning coin.
        """
        try:
            # 1. Get Consensus Signals (The most important part of the pattern)
            consensus_raw = self.r_source.hget(CONSENSUS_KEY, symbol)
            consensus = json.loads(consensus_raw) if consensus_raw else {}

            # 2. Get Regime Context
            regime_raw = self.r_source.hget(REGIME_KEY, symbol)
            regime = json.loads(regime_raw) if regime_raw else {}

            # 3. Get Volume Strength (VOLUME_SCORE stores a JSON summary)
            volume_raw = self.r_source.hget(VOLUME_KEY, symbol)
            volume_score = 0.0
            if volume_raw:
                try:
                    vol_data = json.loads(volume_raw)
                    volume_score = float(vol_data.get('score', 0))
                except:
                    # Fallback for raw float values if any
                    try: volume_score = float(volume_raw)
                    except: pass

            # 4. Get BTC Market Context
            btc_raw = self.r_source.hget(BTC_KEY, "BTCUSDT")
            btc_context = json.loads(btc_raw) if btc_raw else {}

            # 5. Capture High-Resolution Time Series (New Request)
            time_series = self.capture_time_series(symbol)

            # Build the "Winning DNA" object
            pattern_dna = {
                "identity": {
                    "symbol": symbol,
                    "recorded_at": datetime.now().isoformat(),
                    "profit_achieved": hit_metadata.get('profit'),
                    "price_performance": {
                        "base": hit_metadata.get('basePrice'),
                        "current": hit_metadata.get('currentPrice'),
                        "gain_pct": round(((hit_metadata.get('currentPrice') / hit_metadata.get('basePrice')) - 1) * 100, 2)
                    }
                },
                "market_dna": {
                    "consensus_score": consensus.get('score', 0),
                    "layers": consensus.get('signals', {}),
                    "regime": regime.get('regime', 'UNKNOWN'),
                    "regime_confidence": regime.get('confidence', 0),
                    "volume_strength": volume_score,
                    "btc_condition": btc_context.get('condition', 'STABLE'),
                    "is_btc_bullish": btc_context.get('trade_allowed', True)
                },
                "performance_before_1h": self.extract_before_state(time_series),
                "time_series_dna": time_series,
                "meta_criteria": {
                    "decision_at_hit": consensus.get('decision', 'HOLD'),
                    "trend_direction": regime.get('trend', 'NEUTRAL')
                }
            }

            return pattern_dna

        except Exception as e:
            log.error(f"Failed to capture pattern for {symbol}: {e}")
            return None

    def extract_before_state(self, time_series):
        """Helper to pull the 1h-ago state from the time_series data."""
        try:
            h1 = time_series.get("hours", {}).get("1h", {})
            return {
                "price": h1.get("price"),
                "volume_24h": h1.get("volume"),
                "state": "CAPTURED" if h1 else "NOT_AVAILABLE"
            }
        except:
            return {"state": "ERROR"}

    def capture_time_series(self, symbol):
        """Fetches price and volume across multiple timeframes."""
        data = {"seconds": {}, "minutes": {}, "hours": {}}
        ccxt_symbol = symbol if "/" in symbol else f"{symbol[:-4]}/{symbol[-4:]}"
        
        try:
            # 1. Seconds (1s to 50s)
            log.info(f"   ⏱️  Capturing seconds-series for {symbol}...")
            sec_klines = self.ccxt.fetch_ohlcv(ccxt_symbol, timeframe='1s', limit=60)
            if sec_klines:
                for sec in INTERVALS_SEC:
                    if len(sec_klines) >= sec:
                        k = sec_klines[-sec]
                        data["seconds"][f"{sec}s"] = {"price": float(k[4]), "volume": float(k[5])}

            # 2. Minutes (1m to 45m)
            log.info(f"   ⏱️  Capturing minutes-series for {symbol}...")
            min_klines = self.ccxt.fetch_ohlcv(ccxt_symbol, timeframe='1m', limit=60)
            if min_klines:
                for m in INTERVALS_MIN:
                    if len(min_klines) >= m:
                        k = min_klines[-m]
                        data["minutes"][f"{m}m"] = {"price": float(k[4]), "volume": float(k[5])}

            # 3. Hours (1h to 5h)
            log.info(f"   ⏱️  Capturing hours-series for {symbol}...")
            hr_klines = self.ccxt.fetch_ohlcv(ccxt_symbol, timeframe='1h', limit=10)
            if hr_klines:
                for h in INTERVALS_HOUR:
                    if len(hr_klines) >= h:
                        k = hr_klines[-h]
                        data["hours"][f"{h}h"] = {"price": float(k[4]), "volume": float(k[5])}

        except Exception as e:
            log.warning(f"⚠️  Time-series capture partial failure for {symbol}: {e}")
            
        return data

if __name__ == "__main__":
    recorder = SuccessPatternRecorder()
    recorder.run()
