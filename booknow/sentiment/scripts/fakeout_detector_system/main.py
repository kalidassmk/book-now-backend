import asyncio
import aiohttp
import logging
import json
import redis
import pandas as pd
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from symbols_config import ACTIVE_SYMBOLS
from data_fetcher import DataFetcher
from levels import LevelsManager
from fakeout_detector import FakeoutDetector
from strategy import FakeoutStrategy

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
log = logging.getLogger("fakeout.main")

class FakeoutBot:
    def __init__(self, symbols=None, interval_sec=60):
        if symbols is None:
            symbols = ACTIVE_SYMBOLS
        self.symbols = symbols
        self.interval_sec = interval_sec
        self.detector = FakeoutDetector()
        self.strategy = FakeoutStrategy()
        self.redis_client = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)

    async def run(self):
        log.info(f"🕵️ [INITIALIZING] Fakeout Detector (Resilient CCXT Mode) for {self.symbols}")
        fetcher = DataFetcher()
        try:
            while True:
                tasks = [self.process_symbol(fetcher, symbol) for symbol in self.symbols]
                await asyncio.gather(*tasks)
                log.info(f"Cycle complete. Waiting {self.interval_sec}s...")
                await asyncio.sleep(self.interval_sec)
        finally:
            await fetcher.close()

    async def process_symbol(self, fetcher, symbol):
        try:
            # 1. Fetch Data
            klines = await fetcher.fetch_klines(symbol)
            if not klines: return

            df = pd.DataFrame(klines, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
            df['close'] = df['close'].astype(float)
            df['high'] = df['high'].astype(float)
            df['low'] = df['low'].astype(float)
            df['open'] = df['open'].astype(float)
            df['volume'] = df['volume'].astype(float)

            # 2. Identify Levels
            levels = LevelsManager.identify(df)

            # 3. Detect Fakeout
            analysis = self.detector.analyze(df, levels)

            # 4. Evaluate Strategy
            signal_data = self.strategy.evaluate(symbol, analysis, df['close'].iloc[-1])

            # 5. Store in Redis
            self.redis_client.hset("FAKEOUT_SIGNALS", symbol, json.dumps(signal_data))
            
            if signal_data['signal'] != "NEUTRAL":
                log.info(f"🔥 [{symbol}] Signal: {signal_data['signal']} | Reason: {signal_data['reason']}")

        except Exception as e:
            log.error(f"Error processing {symbol}: {e}", exc_info=True)

if __name__ == "__main__":
    bot = FakeoutBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        log.info("Bot stopped.")
