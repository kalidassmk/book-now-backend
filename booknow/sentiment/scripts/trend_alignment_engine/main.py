import asyncio
import aiohttp
import logging
import json
import redis
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from symbols_config import ACTIVE_SYMBOLS
from data_fetcher import DataFetcher
from indicators import TrendIndicator
from alignment_engine import AlignmentEngine
from strategy import TrendAlignmentStrategy

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
log = logging.getLogger("trend_alignment.main")

class TrendAlignmentBot:
    def __init__(self, symbols=None, interval_sec=60):
        if symbols is None:
            symbols = ACTIVE_SYMBOLS
        self.symbols = symbols
        self.interval_sec = interval_sec
        self.alignment_engine = AlignmentEngine(alignment_threshold=70.0)
        self.strategy = TrendAlignmentStrategy()
        self.redis_client = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)

    async def run(self):
        log.info(f"📊 [INITIALIZING] Trend Alignment Engine (Resilient CCXT Mode) for {self.symbols}")
        fetcher = DataFetcher()
        try:
            while True:
                tasks = [self.process_symbol(fetcher, symbol) for symbol in self.symbols]
                await asyncio.gather(*tasks)
                log.info(f"Scanner cycle complete. Waiting {self.interval_sec}s...")
                await asyncio.sleep(self.interval_sec)
        finally:
            await fetcher.close()

    async def process_symbol(self, fetcher, symbol):
        try:
            # 1. Fetch Multi-Timeframe Data
            mtf_data = await fetcher.fetch_multi_timeframe(symbol)
            if not mtf_data: return

            # 2. Determine Trend for each Timeframe
            tf_trends = {}
            for tf, klines in mtf_data.items():
                score, detail = TrendIndicator.analyze(klines)
                tf_trends[tf] = score

            # 3. Calculate Alignment
            alignment = self.alignment_engine.calculate(tf_trends)

            # 4. Generate Signal
            signal_data = self.strategy.evaluate(symbol, alignment)

            # 5. Store in Redis
            self.redis_client.hset("TREND_ALIGNMENT_SIGNALS", symbol, json.dumps(signal_data))
            
            log.info(f"[{symbol}] Alignment: {signal_data['alignment_score']}% | Signal: {signal_data['signal']} | Reason: {signal_data['reason']}")

        except Exception as e:
            log.error(f"Error processing {symbol}: {e}", exc_info=True)

if __name__ == "__main__":
    bot = TrendAlignmentBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        log.info("Scanner stopped.")
