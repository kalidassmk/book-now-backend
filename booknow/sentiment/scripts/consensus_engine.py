import redis
import json
import time
import logging
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
from symbols_config import ACTIVE_SYMBOLS

# Logging Setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
log = logging.getLogger("ConsensusEngine")

class ConsensusEngine:
    """
    The "Supreme Court" of the trading system.
    Unifies the 7 Original Spring Boot signals with the 9 New Python engines.
    """
    def __init__(self, interval_sec=2):
        self.redis_client = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
        self.interval_sec = interval_sec
        self.symbols = ACTIVE_SYMBOLS  # Monitor all active coins

    def run(self):
        log.info("💎 Consensus Engine Online. Unifying 16 algorithms...")
        while True:
            for symbol in self.symbols:
                self.evaluate_consensus(symbol)
            time.sleep(self.interval_sec)

    def evaluate_consensus(self, symbol):
        try:
            # 1. Gather ALL Inputs
            inputs = self.gather_all_signals(symbol)
            
            # 2. Calculate Weighted Score (0 to 100)
            score, details = self.calculate_weighted_score(inputs)
            
            # 3. Apply Hard Vetoes (BTC Filter & Risk Engine)
            is_blocked, veto_reason = self.check_vetoes(inputs)
            
            # 4. Final Decision
            final_decision = {
                "symbol": symbol,
                "score": round(score, 2),
                "decision": "BUY" if score >= 65 and not is_blocked else "HOLD",
                "is_blocked": is_blocked,
                "block_reason": veto_reason,
                "signals": details,
                "timestamp": time.time()
            }
            
            # 5. Publish to Redis for Spring Boot and Dashboard
            self.redis_client.hset("FINAL_CONSENSUS_STATE", symbol, json.dumps(final_decision))
            
            if final_decision["decision"] == "BUY":
                log.info(f"🚀 [BUY SIGNAL] {symbol} | Score: {score} | Consensus Reached!")
            else:
                log.info(f"💎 [{symbol}] Score: {score:.1f} | State: {final_decision['decision']}")
            
        except Exception as e:
            log.error(f"Error evaluating consensus for {symbol}: {e}")

    def gather_all_signals(self, symbol):
        """Fetches data from all 16 algorithms across Redis."""
        data = {}
        # New Python Engines
        data['meta_prob'] = self._get_json_val("META_MODEL_PREDICTIONS", symbol, "probability", 0.0)
        data['btc_filter'] = self._get_json_val("BTC_CORRELATION_FILTERS", symbol, "trade_allowed", True)
        data['risk_allowed'] = self._get_json_val("RISK_STATE", "GLOBAL", "trading_enabled", True)
        data['obi_imbalance'] = self._get_json_val("OBI_STATE", symbol, "imbalance", 0.0)
        data['mtf_score'] = self._get_json_val("TREND_ALIGNMENT_SIGNALS", symbol, "alignment_score", 0.0)
        
        # Original Spring Boot / Sentiment Signals (Mocked or real Redis keys)
        data['news_sentiment'] = float(self.redis_client.get(f"SENTIMENT_NEWS_{symbol}") or 0.5)
        data['behavior_sentiment'] = float(self.redis_client.get(f"SENTIMENT_BEHAVIOR_{symbol}") or 0.5)
        data['dashboard_score'] = float(self.redis_client.get("DASHBOARD_CONSENSUS_SCORE") or 50)
        
        return data

    def calculate_weighted_score(self, inputs):
        """
        Logic:
        - Meta-Model Probability (ML): 40% weight
        - MTF Alignment: 20% weight
        - OBI / Order Book: 15% weight
        - News/Behavior Sentiment: 15% weight
        - Dashboard Score: 10% weight
        """
        # Normalize all to 0-100
        meta_score = inputs['meta_prob'] * 100
        mtf_score = inputs['mtf_score']
        obi_score = (inputs['obi_imbalance'] + 1) * 50 # transform -1/1 to 0-100
        sent_score = (inputs['news_sentiment'] + inputs['behavior_sentiment']) * 50
        dash_score = inputs['dashboard_score']

        weighted_total = (
            (meta_score * 0.30) +
            (mtf_score * 0.25) +
            (obi_score * 0.25) +
            (sent_score * 0.10) +
            (dash_score * 0.10)
        )
        
        details = {
            "ml_layer": round(meta_score, 1),
            "trend_layer": round(mtf_score, 1),
            "book_layer": round(obi_score, 1),
            "sentiment_layer": round(sent_score, 1)
        }
        
        return weighted_total, details

    def check_vetoes(self, inputs):
        """Hard stops that override any score."""
        if not inputs['btc_filter']:
            return True, "BTC Market Filter: Global market unstable or bearish."
        if not inputs['risk_allowed']:
            return True, "Risk Engine: Drawdown limit reached or max positions open."
        return False, ""

    def _get_json_val(self, hash_key, key, field, default):
        try:
            raw = self.redis_client.hget(hash_key, key)
            if raw:
                val = json.loads(raw)
                return val.get(field, default)
        except:
            pass
        return default

if __name__ == "__main__":
    engine = ConsensusEngine()
    engine.run()
