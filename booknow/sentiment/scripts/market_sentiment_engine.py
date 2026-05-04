import ccxt
import time
import json
import redis
import logging
import statistics
import numpy as np
from datetime import datetime
import sys
import os

# Add parent directory to path for config imports
sys.path.insert(0, os.path.dirname(__file__))
from symbols_config import ACTIVE_SYMBOLS

# ==========================================
# 1. LOGGING & CONFIG
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MarketScanner")

class AdaptiveMarketEngine:
    def __init__(self, symbol="BTC/USDT"):
        self.symbol = symbol
        self.exchange = ccxt.binance({
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })
        self.r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        
        # Clear existing results for a fresh start
        self.clear_old_results()
        
        # Regime Weights Configuration
        self.regime_weights = {
            "TRENDING": {"momentum": 0.40, "volume": 0.20, "orderbook": 0.15, "trades": 0.15, "volatility": 0.10},
            "SIDEWAYS": {"momentum": 0.20, "volume": 0.15, "orderbook": 0.30, "trades": 0.25, "volatility": 0.10},
            "VOLATILE": {"momentum": 0.25, "volume": 0.30, "orderbook": 0.10, "trades": 0.25, "volatility": 0.10}
        }

    def clear_old_results(self):
        """Deletes all existing sentiment keys from Redis"""
        try:
            keys = self.r.keys('sentiment:market:adaptive:*')
            if keys:
                self.r.delete(*keys)
                logger.info(f"🧹 Cleared {len(keys)} old sentiment results from Redis.")
        except Exception as e:
            logger.error(f"Cleanup error: {e}")

    # ==========================================
    # 2. MARKET REGIME DETECTION
    # ==========================================
    
    def detect_regime(self, ohlcv_1h):
        """Classifies the market into TRENDING, SIDEWAYS, or VOLATILE"""
        closes = [x[4] for x in ohlcv_1h]
        highs = [x[2] for x in ohlcv_1h]
        lows = [x[3] for x in ohlcv_1h]
        
        # 1. Volatility (ATR-like)
        ranges = [(h - l) / l for h, l in zip(highs[-10:], lows[-10:])]
        avg_range = sum(ranges) / len(ranges)
        
        # 2. Trend Strength
        price_change = abs(closes[-1] - closes[-10]) / closes[-10]
        
        if avg_range > 0.015: # High volatility spike
            return "VOLATILE"
        if price_change > 0.02: # Strong trending
            return "TRENDING"
        return "SIDEWAYS"

    # ==========================================
    # 3. COMPONENT SCORING (Normalized 0-100)
    # ==========================================
    
    def get_scores_for_timeframe(self, timeframe='5m'):
        """Generates the 5 core behavioral scores for a specific timeframe"""
        try:
            ohlcv = self.exchange.fetch_ohlcv(self.symbol, timeframe=timeframe, limit=50)
            ticker = self.exchange.fetch_ticker(self.symbol)
            depth = self.exchange.fetch_order_book(self.symbol, limit=20)
            trades = self.exchange.fetch_trades(self.symbol, limit=100)

            # A. Momentum (Normalized)
            closes = [x[4] for x in ohlcv]
            mom = (closes[-1] - closes[-10]) / closes[-10]
            m_score = min(max(((mom + 0.02) / 0.04) * 100, 0), 100)

            # B. Volume Spike (Normalized)
            vols = [x[5] for x in ohlcv[-20:-1]]
            avg_v = sum(vols) / len(vols) if vols else 1
            v_score = min((ohlcv[-1][5] / avg_v) * 50, 100)

            # C. Order Book Imbalance
            b_sum = sum([b[1] for b in depth['bids']])
            a_sum = sum([a[1] for a in depth['asks']])
            o_score = (b_sum / (b_sum + a_sum)) * 100 if (b_sum + a_sum) > 0 else 50

            # D. Trade Aggression (Whale detection integrated)
            buy_v = sum([t['amount'] for t in trades if t['side'] == 'buy'])
            sell_v = sum([t['amount'] for t in trades if t['side'] == 'sell'])
            large_trades = sum(1 for t in trades if t['cost'] > 10000) # Whale check (>10k USD)
            
            a_score = (buy_v / (buy_v + sell_v)) * 100 if (buy_v + sell_v) > 0 else 50
            if large_trades > 2: a_score = min(a_score + 15, 100)

            # E. Volatility
            h, l = ticker['high'], ticker['low']
            vol_score = min(((h - l) / l / 0.02) * 100, 100)

            return {"momentum": m_score, "volume": v_score, "orderbook": o_score, "trades": a_score, "volatility": vol_score}
        except Exception as e:
            logger.error(f"Scoring error for {timeframe}: {e}")
            return None

    # ==========================================
    # 4. FUSION & CONFIDENCE
    # ==========================================

    def calculate_confidence(self, scores_5m, scores_15m, regime):
        """Calculates a Confidence Score (0-100) based on signal consistency"""
        # More consistency between 5m and 15m = higher confidence
        diffs = [abs(scores_5m[k] - scores_15m[k]) for k in scores_5m]
        consistency = 100 - (sum(diffs) / len(diffs))
        
        # Volume weight in confidence
        vol_strength = min(scores_5m['volume'] / 50 * 100, 100)
        
        return round((consistency * 0.7) + (vol_strength * 0.3), 2)

    def run_adaptive_analysis(self):
        try:
            # 1. Detect Regime (Using 1h candles)
            ohlcv_1h = self.exchange.fetch_ohlcv(self.symbol, timeframe='1h', limit=20)
            regime = self.detect_regime(ohlcv_1h)
            weights = self.regime_weights[regime]
            
            # 2. Get Multi-Timeframe Scores
            s5 = self.get_scores_for_timeframe('5m')
            s15 = self.get_scores_for_timeframe('15m')
            s1h = self.get_scores_for_timeframe('1h')
            
            if not s5 or not s15 or not s1h: return

            # 3. Calculate Weighted Score per Timeframe
            def weight_it(s): return sum(s[k] * weights[k] for k in weights)
            
            score_5m = weight_it(s5)
            score_15m = weight_it(s15)
            score_1h = weight_it(s1h)

            # 4. Timeframe Fusion (Final Sentiment)
            # 5m (50%) + 15m (30%) + 1h (20%)
            final_score = (score_5m * 0.5) + (score_15m * 0.3) + (score_1h * 0.2)
            
            # 5. Confidence Score
            confidence = self.calculate_confidence(s5, s15, regime)

            # 6. Classification
            status = "Neutral ⚖️"
            if final_score > 70: status = "Strong Bullish 🚀"
            elif final_score > 55: status = "Bullish 📈"
            elif final_score < 30: status = "Strong Bearish 🔻"
            elif final_score < 45: status = "Bearish 📉"

            result = {
                "symbol": self.symbol,
                "sentiment": status,
                "score": round(final_score, 2),
                "confidence": confidence,
                "regime": regime,
                "timeframes": {
                    "5m": round(score_5m, 2),
                    "15m": round(score_15m, 2),
                    "1h": round(score_1h, 2)
                },
                "timestamp": datetime.now().isoformat()
            }

            # Store in Redis
            self.r.set(f"sentiment:market:adaptive:{self.symbol}", json.dumps(result))
            
            # Only log Strong signals to console to keep it clean
            if "Strong" in status:
                logger.info(f"🧠 [{regime}] {self.symbol}: {status} (Score: {final_score:.2f} | Conf: {confidence}%)")
            return result

        except Exception as e:
            logger.error(f"Adaptive Analysis failed for {self.symbol}: {e}")

if __name__ == "__main__":
    engine = AdaptiveMarketEngine()
    logger.info(f"🚀 Starting Market Scanner for top {len(ACTIVE_SYMBOLS)} symbols...")
    
    try:
        while True:
            for s in ACTIVE_SYMBOLS:
                try:
                    engine.symbol = s.replace("/", "")
                    engine.run_adaptive_analysis()
                    # Sleep 2.5s between symbols to stay within rate limits (13 requests * 24 symbols/min < 1200 weight)
                    time.sleep(2.5) 
                except Exception:
                    continue
            
            logger.info("✅ Full market cycle complete. Waiting 60s...")
            time.sleep(60)
            
    except KeyboardInterrupt:
        logger.info("🛑 Scanner stopped.")
