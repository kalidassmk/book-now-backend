import asyncio
import aiohttp
import logging
import json
import redis
import time
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from symbols_config import ACTIVE_SYMBOLS
from data_fetcher import DataFetcher
from indicators import IndicatorCalculator
from strategy import FundingOIStrategy

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
log = logging.getLogger("funding_oi.main")

class FundingOIBot:
    def __init__(self, symbols=None, interval_sec=60):
        if symbols is None:
            symbols = ACTIVE_SYMBOLS
        self.symbols = symbols
        self.interval_sec = interval_sec
        self.strategy = FundingOIStrategy()
        self.redis_client = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)

    async def run(self):
        log.info(f"🚀 Starting Funding/OI Bot (Resilient CCXT Mode) for {len(self.symbols)} symbols")
        fetcher = DataFetcher()
        try:
            # ACTIVE_SYMBOLS is sourced from spot-pair discovery; many of those
            # don't have a futures listing. Resolve the perpetual-swap set
            # once, intersect, and skip the rest — otherwise every cycle
            # logs "Invalid symbol" for the same coins forever.
            futures_set = await fetcher.list_futures_symbols()
            if futures_set:
                tradable = [s for s in self.symbols if s in futures_set]
                dropped  = [s for s in self.symbols if s not in futures_set]
                if dropped:
                    log.info(
                        "Skipping %d symbol(s) without futures listing: %s",
                        len(dropped), ", ".join(dropped[:10]) + ("…" if len(dropped) > 10 else "")
                    )
                self.symbols = tradable
                log.info("🚀 Watching %d futures-listed symbols", len(self.symbols))

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
            klines, oi_hist, funding_data = await asyncio.gather(
                fetcher.fetch_klines(symbol),
                fetcher.fetch_open_interest_hist(symbol),
                fetcher.fetch_funding_rate(symbol, limit=1)
            )

            if not klines or not oi_hist or not funding_data:
                log.error(f"Missing data for {symbol}")
                return

            # Adapt CCXT data to IndicatorCalculator (which expects Binance REST format)
            # 1. Klines: CCXT returns 6 columns, Indicators expect 12
            # We only need 'close' for trend calculation
            adapted_klines = []
            for k in klines:
                # CCXT: [ts, o, h, l, c, v]
                # Pad to 12 columns to satisfy IndicatorCalculator DataFrame creation
                adapted_klines.append([k[0], k[1], k[2], k[3], k[4], k[5], 0, 0, 0, 0, 0, 0])

            # 2. OI Hist: CCXT 'openInterest' -> REST 'sumOpenInterest'
            adapted_oi = []
            for o in oi_hist:
                adapted_oi.append({'sumOpenInterest': o.get('openInterest', o.get('sumOpenInterest', 0))})

            # 3. Funding: CCXT 'fundingRate'
            # fetch_funding_rate_history returns a list, we take the last one
            latest_funding = funding_data[-1] if isinstance(funding_data, list) else funding_data
            rate = latest_funding.get('fundingRate', latest_funding.get('rate', 0))

            # 2. Compute Metrics
            price, trend, trend_strength = IndicatorCalculator.calculate_price_trend(adapted_klines)
            oi, oi_change = IndicatorCalculator.calculate_oi_change(adapted_oi)
            funding_status, funding_rate = IndicatorCalculator.normalize_funding(rate)

            # 3. Evaluate Strategy
            signal_data = self.strategy.evaluate(
                symbol, price, trend, trend_strength, oi_change, funding_status, funding_rate
            )

            # 4. Output & Store in Redis
            log.info(f"[{symbol}] Price: {price} | Trend: {trend} | Signal: {signal_data['signal']} | Reason: {signal_data['reason']}")
            
            # Save to Redis
            self.redis_client.hset("FUNDING_OI_SIGNALS", symbol, json.dumps(signal_data))
            self.redis_client.set(f"raw:funding:{symbol}", json.dumps(funding_data))
            
        except Exception as e:
            log.error(f"Error processing {symbol}: {e}", exc_info=True)

if __name__ == "__main__":
    bot = FundingOIBot()  # uses ACTIVE_SYMBOLS from symbols_config.py
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        log.info("Bot stopped by user.")
