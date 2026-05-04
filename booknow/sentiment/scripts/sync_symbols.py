#!/usr/bin/env python3
"""
sync_symbols.py — Standalone script to fetch top USDT symbols from Binance API.
This script ranks coins by 24h volume and stores them in Redis.
"""

import requests
import redis
import json
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("SymbolSync")

# Configuration
BINANCE_TICKER_URL = "https://api.binance.com/api/v3/ticker/24hr"
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
TOP_N = 200

def fetch_and_sync():
    try:
        # 1. Connect to Redis
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping()
        logger.info("Connected to Redis.")

        # 2. Fetch all 24h tickers from Binance
        logger.info("Fetching market data from Binance API...")
        response = requests.get(BINANCE_TICKER_URL, timeout=15)
        response.raise_for_status()
        tickers = response.json()

        # 3. Filter for USDT pairs and sort by volume
        usdt_pairs = []
        for t in tickers:
            symbol = t['symbol']
            # Only USDT pairs, excluding leveraged tokens (UP/DOWN) and stablecoin pairs
            if symbol.endswith("USDT") and "UP" not in symbol and "DOWN" not in symbol:
                if any(stable in symbol for stable in ["USDC", "BUSD", "TUSD", "DAI", "EUR"]):
                    if symbol != "BTCUSDT" and symbol != "ETHUSDT": # Keep main pairs
                        continue
                
                usdt_pairs.append({
                    "symbol": symbol,
                    "volume": float(t['quoteVolume']),
                    "price": float(t['lastPrice'])
                })

        # Sort by volume descending
        usdt_pairs.sort(key=lambda x: x['volume'], reverse=True)
        
        # Take Top N
        top_symbols = [x['symbol'] for x in usdt_pairs[:TOP_N]]
        logger.info(f"Identified top {len(top_symbols)} symbols by 24h volume.")

        # 4. Store in Redis
        # Main list for all algorithms
        r.set("SYMBOLS:ACTIVE", json.dumps(top_symbols))
        
        # OBI Subset (Top 20)
        r.set("SYMBOLS:OBI", json.dumps(top_symbols[:20]))
        
        # BTC Filter (Exclude BTC)
        r.set("SYMBOLS:BTC_FILTER", json.dumps([s for s in top_symbols if s != "BTCUSDT"]))
        
        # Metadata for last update
        r.set("SYMBOLS:LAST_UPDATED", json.dumps({"timestamp": str(datetime.now()), "count": len(top_symbols)}))

        logger.info("Successfully updated Redis with live Binance symbols.")
        print(f"✅ SYNC COMPLETE: {len(top_symbols)} coins stored in Redis.")
        print(f"Top 10: {top_symbols[:10]}")

    except Exception as e:
        logger.error(f"Sync failed: {e}")
        return False
    return True

if __name__ == "__main__":
    fetch_and_sync()
