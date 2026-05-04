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
try:
    import matplotlib.pyplot as plt
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
import numpy as np
from data_fetcher import DataFetcher
from volume_profile import VolumeProfileEngine
from strategy import VolumeProfileStrategy

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
log = logging.getLogger("volume_profile.main")

class VolumeProfileBot:
    def __init__(self, symbols=None, interval_sec=60):
        if symbols is None:
            symbols = ACTIVE_SYMBOLS
        self.symbols = symbols
        self.interval_sec = interval_sec
        self.engine = VolumeProfileEngine(num_bins=150)
        self.strategy = VolumeProfileStrategy()
        self.redis_client = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)

    async def run(self):
        log.info(f"🚀 Starting Volume Profile Bot (Resilient CCXT Mode) for: {self.symbols}")
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

            # 2. Calculate Profile
            data = self.engine.calculate(klines)
            if not data: return

            # 3. Evaluate Strategy
            signal_data = self.strategy.evaluate(symbol, data)

            # 4. Storage
            self.redis_client.hset("VOLUME_PROFILE_SIGNALS", symbol, json.dumps(signal_data))
            
            # 5. Visualization (save plot if available)
            if MATPLOTLIB_AVAILABLE:
                self.generate_plot(symbol, data)
            
            log.info(f"[{symbol}] POC: {data['poc']} | Signal: {signal_data['signal']} | {signal_data['reason']}")

        except Exception as e:
            log.error(f"Error processing {symbol}: {e}", exc_info=True)

    def generate_plot(self, symbol, data):
        """Generates a volume vs price histogram plot."""
        plt.figure(figsize=(10, 6))
        
        bins = np.array(data['bins'][:-1])
        profile = np.array(data['profile'])
        
        plt.barh(bins, profile, height=(bins[1]-bins[0]), color='skyblue', alpha=0.7)
        
        # Highlight POC, VAH, VAL
        plt.axhline(data['poc'], color='red', linestyle='--', label=f"POC: {data['poc']}")
        plt.axhline(data['vah'], color='green', linestyle=':', label=f"VAH: {data['vah']}")
        plt.axhline(data['val'], color='orange', linestyle=':', label=f"VAL: {data['val']}")
        plt.axhline(data['current_price'], color='black', label=f"Price: {data['current_price']}")
        
        plt.title(f"Volume Profile: {symbol}")
        plt.xlabel("Volume")
        plt.ylabel("Price")
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        plt.savefig(f"plots/{symbol}_profile.png")
        plt.close()

if __name__ == "__main__":
    bot = VolumeProfileBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        log.info("Bot stopped.")
