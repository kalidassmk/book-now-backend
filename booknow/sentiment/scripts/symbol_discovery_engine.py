#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════╗
║           SYMBOL DISCOVERY ENGINE  —  Adaptive Market-Behavior Stack    ║
║  Fetches ALL active USDT pairs from Binance, ranks them by a composite  ║
║  score (volume, liquidity, momentum, trade count), stores the top 200   ║
║  in Redis so every downstream algorithm reads from a single source of   ║
║  truth instead of a static config file.                                 ║
║                                                                         ║
║  Redis Keys written:                                                    ║
║    SYMBOLS:ACTIVE          → JSON list of top 200 symbols (ordered)    ║
║    SYMBOLS:OBI             → JSON list of top 20 (for OBI WebSockets)  ║
║    SYMBOLS:BTC_FILTER      → ACTIVE minus BTCUSDT                      ║
║    SYMBOLS:METADATA        → Hash  symbol → JSON metadata              ║
║    SYMBOLS:LAST_UPDATED    → ISO timestamp of last successful refresh   ║
║    SYMBOLS:REFRESH_INTERVAL→ Seconds between refreshes (default 3600)  ║
║                                                                         ║
║  Usage:                                                                 ║
║    python symbol_discovery_engine.py           # runs forever (1h loop)║
║    python symbol_discovery_engine.py --once    # single run & exit      ║
║    python symbol_discovery_engine.py --top 100 # limit to top N         ║
║    python symbol_discovery_engine.py --interval 1800  # 30min refresh  ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

import asyncio
import os
import ccxt.async_support as ccxt
import redis
import json
import logging
import argparse
import ssl
import time
from datetime import datetime, timezone
from typing import List, Dict, Any

_DEFAULT_REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
_DEFAULT_REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("SymbolDiscovery")

# ── Constants ──────────────────────────────────────────────────────────────
BINANCE_SPOT_TICKER   = "https://api.binance.com/api/v3/ticker/24hr"

# Scoring weights (must sum to 1.0). Spot-only — futures open-interest
# weight was removed when the engine moved to spot-only trading.
WEIGHT_QUOTE_VOLUME   = 0.50   # 24h USDT volume — liquidity king
WEIGHT_TRADE_COUNT    = 0.25   # number of trades — retail activity
WEIGHT_PRICE_CHANGE   = 0.25   # absolute % price change — momentum

# Hard filters
MIN_QUOTE_VOLUME_USDT = 2_000_000      # at least $2M 24h volume (2026-05-12: lowered from $5M to widen discovery)
EXCLUDED_SYMBOLS      = {              # stablecoins & wrapped tokens to skip
    "USDCUSDT", "BUSDUSDT", "TUSDUSDT", "USDPUSDT", "DAIUSDT",
    "WBTCUSDT", "WETHUSDT", "STETHUSDT", "LDOUSDT",
}

# OBI Trader: top N symbols for dedicated WebSocket per coin
OBI_TOP_N = 20


class SymbolDiscoveryEngine:
    """
    Fetches, ranks, and caches the top USDT trading pairs to Redis.
    """

    def __init__(
        self,
        redis_host: str = _DEFAULT_REDIS_HOST,
        redis_port: int = _DEFAULT_REDIS_PORT,
        top_n: int = 200,
        refresh_interval_sec: int = 3600,
    ):
        self.top_n = top_n
        self.refresh_interval = refresh_interval_sec

        # Redis connection
        try:
            self._redis = redis.Redis(
                host=redis_host, port=redis_port,
                decode_responses=True,
                socket_connect_timeout=5,
            )
            self._redis.ping()
            log.info("🔗 [REDIS] Connected to Redis at %s:%d", redis_host, redis_port)
        except redis.ConnectionError:
            log.critical("❌ [REDIS] Cannot connect to Redis. Is it running?")
            raise

        # Store refresh interval in Redis so other services can read it
        self._redis.set("SYMBOLS:REFRESH_INTERVAL", str(refresh_interval_sec))

    # ── Public API ─────────────────────────────────────────────────────────

    async def run_once(self):
        """Perform a single discovery + ranking cycle."""
        log.info("=" * 65)
        log.info("  🔍 Symbol Discovery Engine — Starting Scan")
        log.info("  Target: Top %d USDT pairs by composite score", self.top_n)
        log.info("=" * 65)

        # Initialize CCXT (spot-only — futures client removed with spot-only switch).
        self.spot_client = ccxt.binance({
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })

        try:
            # 1. Fetch spot ticker data
            spot_tickers = await self.spot_client.fetch_tickers()

            # Convert CCXT tickers to the internal format
            spot_data = []
            for sym, t in spot_tickers.items():
                if not sym.endswith("/USDT"): continue
                base_sym = sym.replace("/", "")
                if base_sym in EXCLUDED_SYMBOLS: continue
                volume = float(t.get('quoteVolume', 0))
                if volume < MIN_QUOTE_VOLUME_USDT: continue
                spot_data.append({
                    "symbol": base_sym,
                    "quote_volume": volume,
                    "price": float(t.get('last', 0)),
                    "price_change_pct": abs(float(t.get('percentage', 0))),
                    "trade_count": int(t.get('info', {}).get('count', 0)),
                    "high": float(t.get('high', 0)),
                    "low": float(t.get('low', 0)),
                    "source": "spot"
                })

            # 2. Rank
            ranked = self._rank_symbols(spot_data)

            # 3. Dynamic Injection: FAST_MOVE Priority
            try:
                fast_movers = self._redis.hkeys("FAST_MOVE")
                if fast_movers:
                    log.info("🔥 [FAST_MOVE] Detected breakout symbols from Java Backend. Ensuring they are ACTIVE.")
                    existing_symbols = {r["symbol"] for r in ranked}
                    for fm in fast_movers:
                        if fm not in existing_symbols:
                            log.info("✨ [FAST_MOVE] Injecting breakout: %s", fm)
                            ranked.append({
                                "symbol": fm, "rank": 999, "score": 100.0,
                                "price": 0.0, "quote_volume": 0.0, "price_change_pct": 0.0,
                                "trade_count": 0,
                                "high_24h": 0, "low_24h": 0,
                                "norm_volume": 0.0, "norm_trades": 0.0, "norm_momentum": 0.0,
                            })
            except Exception as e:
                log.warning("⚠️ Failed to inject Fast Movers: %s", e)

        except Exception as e:
            log.error("❌ Discovery scan failed: %s", e)
            ranked = []
        finally:
            await self.close()

        if not ranked:
            log.error("❌ No symbols ranked — aborting Redis write.")
            return

        # 3. Write to Redis
        self._publish_to_redis(ranked)

        log.info("=" * 65)
        log.info("  ✅ Discovery Complete | %d symbols ranked & stored", len(ranked))
        log.info("  🥇 Top 5: %s", [r["symbol"] for r in ranked[:5]])
        log.info("  🔄 Next refresh in %ds (%s)",
                 self.refresh_interval,
                 f"{self.refresh_interval // 3600}h {(self.refresh_interval % 3600) // 60}m")
        log.info("=" * 65)

    async def run_forever(self):
        """Continuously refresh symbols on the configured interval."""
        log.info("🚀 Symbol Discovery Engine starting in continuous mode")
        while True:
            try:
                await self.run_once()
            except Exception as e:
                log.error("❌ Discovery cycle failed: %s", e)

            log.info("😴 Sleeping %ds until next refresh...", self.refresh_interval)
            await asyncio.sleep(self.refresh_interval)

    # ── Data Fetching ──────────────────────────────────────────────────────

    async def close(self):
        await self.spot_client.close()

    # ── Ranking Logic ──────────────────────────────────────────────────────

    def _rank_symbols(self, spot_data: List[Dict]) -> List[Dict]:
        """
        Compute a composite ranking score from spot ticker data.

        Score formula (0–100):
            score = 0.50 × norm(quote_volume)
                  + 0.25 × norm(trade_count)
                  + 0.25 × norm(abs_price_change_pct)
        """
        merged = {}
        for s in spot_data:
            sym = s["symbol"]
            merged[sym] = {
                "symbol":           sym,
                "price":            s["price"],
                "quote_volume":     s["quote_volume"],
                "price_change_pct": s["price_change_pct"],
                "trade_count":      s["trade_count"],
                "high_24h":         s.get("high", 0),
                "low_24h":          s.get("low", 0),
            }

        records = list(merged.values())
        if not records:
            return []

        # Min-max normalise each dimension to [0, 100]
        def norm(values: List[float]) -> List[float]:
            mn, mx = min(values), max(values)
            if mx == mn:
                return [50.0] * len(values)
            return [((v - mn) / (mx - mn)) * 100 for v in values]

        volumes  = [r["quote_volume"]     for r in records]
        trades   = [r["trade_count"]      for r in records]
        momentum = [r["price_change_pct"] for r in records]

        norm_vol = norm(volumes)
        norm_trd = norm(trades)
        norm_mom = norm(momentum)

        for i, rec in enumerate(records):
            rec["score"] = round(
                WEIGHT_QUOTE_VOLUME   * norm_vol[i]
                + WEIGHT_TRADE_COUNT  * norm_trd[i]
                + WEIGHT_PRICE_CHANGE * norm_mom[i],
                2,
            )
            rec["norm_volume"]   = round(norm_vol[i], 2)
            rec["norm_trades"]   = round(norm_trd[i], 2)
            rec["norm_momentum"] = round(norm_mom[i], 2)

        # Sort by composite score descending
        records.sort(key=lambda r: r["score"], reverse=True)

        # Assign rank
        for rank, rec in enumerate(records, start=1):
            rec["rank"] = rank

        return records[:self.top_n]

    # ── Redis Publishing ───────────────────────────────────────────────────

    def _publish_to_redis(self, ranked: List[Dict]):
        """Write all symbol lists and metadata to Redis atomically."""
        log.info("💾 [REDIS] Publishing %d symbols to Redis...", len(ranked))

        ts = datetime.now(timezone.utc).isoformat()
        pipe = self._redis.pipeline()

        # 1. SYMBOLS:ACTIVE — ordered list of all top-N symbols
        active_list = [r["symbol"] for r in ranked]
        pipe.set("SYMBOLS:ACTIVE", json.dumps(active_list))

        # 2. SYMBOLS:OBI — top 20 by score (for OBI WebSocket trader)
        obi_list = active_list[:OBI_TOP_N]
        pipe.set("SYMBOLS:OBI", json.dumps(obi_list))

        # 3. SYMBOLS:BTC_FILTER — all active except BTCUSDT
        btc_filter_list = [s for s in active_list if s != "BTCUSDT"]
        pipe.set("SYMBOLS:BTC_FILTER", json.dumps(btc_filter_list))

        # 4. SYMBOLS:METADATA — rich per-symbol JSON (hash)
        pipe.delete("SYMBOLS:METADATA")
        for rec in ranked:
            meta = {
                "rank":             rec["rank"],
                "score":            rec["score"],
                "price":            rec["price"],
                "quote_volume_24h": rec["quote_volume"],
                "trade_count_24h":  rec["trade_count"],
                "price_change_pct": rec["price_change_pct"],
                "high_24h":         rec["high_24h"],
                "low_24h":          rec["low_24h"],
                "norm_volume":      rec["norm_volume"],
                "norm_trades":      rec["norm_trades"],
                "norm_momentum":    rec["norm_momentum"],
                "last_updated":     ts,
            }
            pipe.hset("SYMBOLS:METADATA", rec["symbol"], json.dumps(meta))

        # 5. SYMBOLS:LAST_UPDATED — refresh timestamp
        pipe.set("SYMBOLS:LAST_UPDATED", ts)

        # 6. Store top 200 ranked list as JSON for dashboard
        pipe.set("SYMBOLS:RANKED_FULL", json.dumps(ranked), ex=7200)  # 2h TTL

        pipe.execute()

        log.info("✅ [REDIS] Written: SYMBOLS:ACTIVE (%d), SYMBOLS:OBI (%d), SYMBOLS:BTC_FILTER (%d)",
                 len(active_list), len(obi_list), len(btc_filter_list))
        log.info("📊 [REDIS] Top 10 by score:")
        log.info("   %-14s  %6s  %20s  %10s  %8s",
                 "Symbol", "Rank", "24h Vol (USDT)", "Trades", "Score")
        log.info("   " + "─" * 65)
        for rec in ranked[:10]:
            log.info("   %-14s  %6d  %20s  %10s  %8.2f",
                     rec["symbol"], rec["rank"], 
                     f"{rec['quote_volume']:,.0f}",
                     f"{rec['trade_count']:,d}",
                     rec["score"])


# ── Redis Helper (imported by all other algorithms) ────────────────────────

class RedisSymbolClient:
    """
    Lightweight helper used by all downstream algorithms to read
    their symbol lists from Redis (with static fallback on error).
    """
    FALLBACK = [
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
        "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT",
    ]

    def __init__(self, redis_host=_DEFAULT_REDIS_HOST, redis_port=_DEFAULT_REDIS_PORT):
        try:
            self._r = redis.Redis(
                host=redis_host, port=redis_port,
                decode_responses=True, socket_connect_timeout=3,
            )
            self._r.ping()
            self._ok = True
        except Exception:
            self._ok = False
            self._r = None
            log.warning("⚠️  RedisSymbolClient: Redis not available — using fallback symbols")

    def get_active_symbols(self) -> List[str]:
        return self._get_list("SYMBOLS:ACTIVE", self.FALLBACK)

    def get_obi_symbols(self) -> List[str]:
        return self._get_list("SYMBOLS:OBI", self.FALLBACK[:10])

    def get_btc_filter_symbols(self) -> List[str]:
        fallback = [s for s in self.FALLBACK if s != "BTCUSDT"]
        return self._get_list("SYMBOLS:BTC_FILTER", fallback)

    def get_symbol_metadata(self, symbol: str) -> Dict:
        if not self._ok:
            return {}
        try:
            raw = self._r.hget("SYMBOLS:METADATA", symbol)
            return json.loads(raw) if raw else {}
        except Exception:
            return {}

    def get_last_updated(self) -> str:
        if not self._ok:
            return "N/A"
        return self._r.get("SYMBOLS:LAST_UPDATED") or "Never"

    def is_stale(self, max_age_sec: int = 7200) -> bool:
        """Returns True if the symbol list hasn't been refreshed recently."""
        if not self._ok:
            return True
        ts_str = self._r.get("SYMBOLS:LAST_UPDATED")
        if not ts_str:
            return True
        try:
            last = datetime.fromisoformat(ts_str)
            age = (datetime.now(timezone.utc) - last).total_seconds()
            return age > max_age_sec
        except Exception:
            return True

    def _get_list(self, key: str, fallback: List[str]) -> List[str]:
        if not self._ok:
            return fallback
        try:
            raw = self._r.get(key)
            if raw:
                result = json.loads(raw)
                if result:
                    return result
        except Exception:
            pass
        log.warning("⚠️  [%s] not found in Redis — using fallback (%d coins)", key, len(fallback))
        return fallback


# ── Entry Point ────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Symbol Discovery Engine — ranks top USDT pairs and stores to Redis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python symbol_discovery_engine.py               # continuous refresh every 1h
  python symbol_discovery_engine.py --once        # run once and exit
  python symbol_discovery_engine.py --top 100     # keep top 100 only
  python symbol_discovery_engine.py --interval 1800  # refresh every 30min
        """,
    )
    p.add_argument("--once",     action="store_true", help="Run a single scan and exit")
    p.add_argument("--top",      type=int, default=200, help="Number of symbols to keep (default: 200)")
    p.add_argument("--interval", type=int, default=3600, help="Refresh interval in seconds (default: 3600)")
    p.add_argument("--redis-host", default=_DEFAULT_REDIS_HOST, help=f"Redis host (default from REDIS_HOST env, currently: {_DEFAULT_REDIS_HOST})")
    p.add_argument("--redis-port", type=int, default=_DEFAULT_REDIS_PORT, help=f"Redis port (default from REDIS_PORT env, currently: {_DEFAULT_REDIS_PORT})")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    engine = SymbolDiscoveryEngine(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        top_n=args.top,
        refresh_interval_sec=args.interval,
    )
    try:
        if args.once:
            asyncio.run(engine.run_once())
        else:
            asyncio.run(engine.run_forever())
    except KeyboardInterrupt:
        log.info("👋 Symbol Discovery Engine stopped.")
    except Exception as e:
        log.critical("🔥 Fatal error: %s", e)
        raise
