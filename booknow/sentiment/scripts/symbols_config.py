"""
symbols_config.py — Centralized symbol list for all trading algorithms.

This file now dynamically pulls the top 200 ranked symbols from Redis.
The Redis data is populated by 'symbol_discovery_engine.py'.
If Redis is unavailable, it falls back to a high-quality static list.
"""

import logging
import os
import sys

# Add current directory to path so we can import symbol_discovery_engine
sys.path.insert(0, os.path.dirname(__file__))

try:
    from symbol_discovery_engine import RedisSymbolClient
    _client = RedisSymbolClient()
    
    # ── Dynamic Lists from Redis ───────────────────────────────────────────
    ACTIVE_SYMBOLS     = _client.get_active_symbols()
    OBI_SYMBOLS        = _client.get_obi_symbols()
    BTC_FILTER_SYMBOLS = _client.get_btc_filter_symbols()
    
    _last_updated = _client.get_last_updated()
    _count = len(ACTIVE_SYMBOLS)
    
    # Simple status log (optional)
    # print(f"✅ Loaded {_count} symbols from Redis (Last refresh: {_last_updated})")

except ImportError:
    # ── Static Fallback (if Redis discovery engine is missing) ─────────────
    ACTIVE_SYMBOLS = [
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
        "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT",
        "MATICUSDT", "LINKUSDT", "LTCUSDT", "SHIBUSDT", "UNIUSDT",
        "ATOMUSDT", "NEARUSDT", "XLMUSDT", "ETCUSDT", "BCHUSDT",
    ]
    OBI_SYMBOLS = ACTIVE_SYMBOLS[:10]
    BTC_FILTER_SYMBOLS = [s for s in ACTIVE_SYMBOLS if s != "BTCUSDT"]
except Exception as e:
    # ── Error Fallback ─────────────────────────────────────────────────────
    ACTIVE_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    OBI_SYMBOLS = ["BTCUSDT"]
    BTC_FILTER_SYMBOLS = ["ETHUSDT", "SOLUSDT"]
