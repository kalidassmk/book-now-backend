import redis
import json
import time
import logging

# ==========================================
# 1. LOGGING & CONFIG
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-7s | %(message)s')
log = logging.getLogger("PatternMatcher")

# Redis Configuration
LOCAL_REDIS = {'host': 'localhost', 'port': 6379, 'db': 0, 'decode_responses': True}
REMOTE_REDIS = {
    'host': 'redis-18144.c89.us-east-1-3.ec2.cloud.redislabs.com',
    'port': 18144,
    'password': 'Gn9jKtL0SBkMLYynSjXbblmkjkIGrdPS',
    'decode_responses': True
}

# Source Keys
ANALYSE_DB_KEY = "ANALYSE_DB"
CONSENSUS_KEY = "FINAL_CONSENSUS_STATE"
REGIME_KEY = "REGIME_STATE"
VOLUME_KEY = "VOLUME_SCORE"

# Output Key
PATTERN_SIGNAL_KEY = "PATTERN_MATCH_SIGNALS"

class PatternMatchingEngine:
    """
    Compares real-time market data against historical "Success Patterns" from AnalyseDB.
    Triggers high-confidence signals when current coin behavior mimics past $0.20+ profit runs.
    """
    def __init__(self):
        # Connection for live market data (Local DB 0)
        self.r_source = redis.Redis(**LOCAL_REDIS)
        # Dedicated connection for Pattern Storage (Remote Redis Cloud)
        self.r_analyse = redis.Redis(**REMOTE_REDIS)
        
        self.success_patterns = []
        self.last_sync = 0

    def sync_patterns(self):
        """Refreshes the local cache of successful patterns from AnalyseDB (DB 1)."""
        try:
            raw_patterns = self.r_analyse.hgetall(ANALYSE_DB_KEY)
            self.success_patterns = [json.loads(p) for p in raw_patterns.values()]
            self.last_sync = time.time()
            log.info(f"🔄 [SYNC] Synchronized {len(self.success_patterns)} success patterns from AnalyseDB (DB 1)")
        except Exception as e:
            log.error(f"Sync error: {e}")

    def calculate_similarity(self, current, historical):
        """
        Calculates how closely the current state matches a historical success pattern.
        Weights:
        - Layer Scores: 50%
        - Regime Type: 30%
        - Volume Strength: 20%
        """
        score = 0
        
        # 1. Compare Consensus Layers (ML, Trend, Book, Sentiment)
        curr_layers = current.get('layers', {})
        hist_layers = historical.get('market_dna', {}).get('layers', {})
        
        layer_match = 0
        for key in ['ml_layer', 'trend_layer', 'book_layer', 'sentiment_layer']:
            curr_val = curr_layers.get(key, 0)
            hist_val = hist_layers.get(key, 0)
            # If values are within 10 points, consider it a match
            if abs(curr_val - hist_val) <= 15:
                layer_match += 25
        
        score += (layer_match * 0.50)

        # 2. Compare Regime
        curr_regime = current.get('regime')
        hist_regime = historical.get('market_dna', {}).get('regime')
        if curr_regime == hist_regime:
            score += 30

        # 3. Compare Volume Strength
        curr_vol = current.get('volume_strength', 0)
        hist_vol = historical.get('market_dna', {}).get('volume_strength', 0)
        if abs(curr_vol - hist_vol) <= 2:
            score += 20

        return score

    def run(self):
        log.info("🎯 Pattern Matching Engine Online. Hunting for historical repeats...")
        
        while True:
            # Sync patterns every 5 minutes
            if time.time() - self.last_sync > 300:
                self.sync_patterns()

            if not self.success_patterns:
                time.sleep(10)
                continue

            try:
                # Get all current consensus states from DB 0
                all_consensus = self.r_source.hgetall(CONSENSUS_KEY)
                
                for symbol, consensus_json in all_consensus.items():
                    consensus = json.loads(consensus_json)
                    
                    # Gather current context for comparison from DB 0
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
                            try: volume_score = float(volume_raw)
                            except: pass

                    current_state = {
                        "layers": consensus.get('signals', {}),
                        "regime": regime.get('regime', 'UNKNOWN'),
                        "volume_strength": volume_score
                    }

                    # Compare against all success patterns
                    best_match_score = 0
                    for pattern in self.success_patterns:
                        match_score = self.calculate_similarity(current_state, pattern)
                        if match_score > best_match_score:
                            best_match_score = match_score

                    # If similarity is very high (> 85%), issue a pattern-match signal (Write to DB 0 for live bot)
                    if best_match_score >= 85:
                        signal = {
                            "symbol": symbol,
                            "similarity": round(best_match_score, 2),
                            "action": "BUY_BY_PATTERN",
                            "timestamp": time.time(),
                            "status": "ACTIVE"
                        }
                        self.r_source.hset(PATTERN_SIGNAL_KEY, symbol, json.dumps(signal))
                        log.info(f"✨ [PATTERN MATCH] {symbol} matches historical success DNA with {best_match_score}% similarity!")

                time.sleep(5) # Analysis cycle every 5s

            except Exception as e:
                log.error(f"Matcher Loop Error: {e}")
                time.sleep(5)

if __name__ == "__main__":
    matcher = PatternMatchingEngine()
    matcher.run()
