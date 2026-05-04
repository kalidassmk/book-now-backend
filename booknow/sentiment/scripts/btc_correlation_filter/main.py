import asyncio
import aiohttp
import logging
import json
import redis
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from symbols_config import BTC_FILTER_SYMBOLS
from data_fetcher import DataFetcher
from correlation import CorrelationEngine
from btc_filter import BTCFilter
from strategy_filter import StrategyFilter

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
log = logging.getLogger("btc_filter.main")

class BTCFilterBot:
    def __init__(self, symbols=None, interval_sec=60):
        if symbols is None:
            symbols = BTC_FILTER_SYMBOLS  # All alts (BTC excluded)
        self.symbols = symbols
        self.interval_sec = interval_sec
        self.redis_client = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)

    async def run(self):
        log.info(f"🚀 Starting BTC Correlation Filter (Resilient Mode) for: {len(self.symbols)} symbols")
        fetcher = DataFetcher()
        # Limit concurrency to 5 simultaneous requests to avoid IP bans
        semaphore = asyncio.Semaphore(5)
        
        try:
            while True:
                # 1. Fetch BTC data once per cycle
                log.info("📊 Fetching fresh BTCUSDT reference data...")
                btc_klines = await fetcher.fetch_single("BTCUSDT")
                
                if not btc_klines:
                    log.warning("⚠️ Could not fetch BTCUSDT data. Skipping cycle.")
                    await asyncio.sleep(10)
                    continue

                # 2. Process symbols with concurrency limit
                tasks = [self.process_symbol(fetcher, symbol, btc_klines, semaphore) for symbol in self.symbols]
                await asyncio.gather(*tasks)
                
                log.info(f"✨ Filter cycle complete ({len(self.symbols)} coins). Waiting {self.interval_sec}s...")
                await asyncio.sleep(self.interval_sec)
        finally:
            await fetcher.close()

    async def process_symbol(self, fetcher, symbol, btc_klines, semaphore):
        async with semaphore:
            try:
                # 1. Fetch Alt Data (BTC already provided)
                alt_klines = await fetcher.fetch_single(symbol)
                if not alt_klines: return

                # 2. Calculate Correlation
                correlation = CorrelationEngine.calculate(alt_klines, btc_klines)

                # 3. Analyze BTC Context
                btc_trend, btc_score = BTCFilter.analyze(btc_klines)

                # 4. Apply Strategy Filters
                filter_data = StrategyFilter.apply(symbol, btc_trend, btc_score, correlation)

                # 5. Store in Redis
                self.redis_client.hset("BTC_CORRELATION_FILTERS", symbol, json.dumps(filter_data))
                
                # Only log allowed trades or every 50th symbol to reduce log noise
                if filter_data['trade_allowed']:
                    log.info(f"✅ [{symbol}] Corr: {correlation} | BTC: {btc_trend} ({btc_score}) | ALLOWED")
                # else:
                #    log.debug(f"❌ [{symbol}] Corr: {correlation} | BTC: {btc_trend} | Restricted")

            except Exception as e:
                log.error(f"Error filtering {symbol}: {e}")

if __name__ == "__main__":
    bot = BTCFilterBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        log.info("Filter stopped.")
