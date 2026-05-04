#!/usr/bin/env python3
"""
volume_price_analyzer.py
─────────────────────────────────────────────────────────────────────────────
Multi-Timeframe Volume & Price Trading Algorithm for Binance.

Analyzes 12 timeframes (5m → 1M) to produce a 0–100 BUY score.
Uses direct Binance REST API (no ccxt dependency for data fetching).

Usage:
  python3 volume_price_analyzer.py --symbol KITE/USDT
  python3 volume_price_analyzer.py --scan
  python3 volume_price_analyzer.py --symbols BTC/USDT ETH/USDT SOL/USDT
─────────────────────────────────────────────────────────────────────────────
"""

import asyncio
import time
import json
import logging
import requests
import redis
import pandas as pd
import numpy as np
import urllib3
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Optional WebSocket-backed kline cache (used by --daemon mode).
# Falls back gracefully if the module is missing in older deployments.
try:
    from klines_ws_cache import KlinesCache  # type: ignore
except Exception:
    KlinesCache = None  # type: ignore

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("VolumePriceAnalyzer")

# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

BINANCE_BASE = "https://api.binance.com"

# (display_name, binance_interval, candle_limit, weight, category)
TIMEFRAME_CONFIG = [
    ("1m",  "1m",  60,  0.25, "micro"), # High weight for micro-trend
    ("5m",  "5m",  60,  0.15, "short"),
    ("15m", "15m", 48,  0.10, "short"),
    ("1h",  "1h",  48,  0.10, "short"),
    ("4h",  "4h",  48,  0.10, "mid"),
    ("1d",  "1d",  60,  0.10, "long"),
    ("1w",  "1w",  30,  0.10, "long"),
    ("1M",  "1M",  24,  0.10, "long"),
]

BUY_THRESHOLD  = 70
HOLD_THRESHOLD = 40
REDIS_PREFIX   = "sentiment:market:volume"

# Daemon-mode tuning
DAEMON_SCAN_INTERVAL_S = 600        # Re-score every 10 min (matches start_utilities cadence)
DAEMON_REFRESH_WATCH_S = 30         # Refresh subscribed symbols from FAST_MOVE every 30s
DAEMON_TOP_N           = 20         # Track at most this many fast-moving symbols
# Timeframes we drive over WS. The slow ones (1d/1w/1M) are too expensive to
# keep alive on hundreds of streams and barely change between scans, so they
# stay on REST with a per-(symbol, interval) cache.
LIVE_INTERVALS         = ("1m", "5m", "15m", "1h", "4h")
SLOW_REST_TTL_S        = {"1d": 1800, "1w": 21600, "1M": 86400}

# ═══════════════════════════════════════════════════════════════════════════════
#  DATA FETCHING — Direct Binance REST API
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_klines(symbol: str, interval: str, limit: int = 50, cache=None) -> pd.DataFrame:
    """
    Fetch OHLCV klines for a symbol/interval.

    Order of preference:
      1. If a `KlinesCache` is supplied AND it has data for this pair,
         return a slice from the in-memory buffer (zero REST calls).
      2. Otherwise fall back to a direct Binance REST `/api/v3/klines` call.
    """
    if cache is not None and getattr(cache, "has", lambda *_: False)(symbol, interval):
        df = cache.get_klines(symbol, interval, limit)
        if not df.empty:
            return df

    api_symbol = symbol.replace("/", "")
    url = f"{BINANCE_BASE}/api/v3/klines"
    params = {"symbol": api_symbol, "interval": interval, "limit": limit}

    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=10, verify=False)
            if resp.status_code == 429:
                wait = 2 ** (attempt + 1)
                logger.warning(f"Rate limited, waiting {wait}s …")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()

            df = pd.DataFrame(data, columns=[
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_vol", "trades", "taker_buy_vol",
                "taker_buy_quote_vol", "ignore",
            ])
            for col in ["open", "high", "low", "close", "volume"]:
                df[col] = df[col].astype(float)
            df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms")
            return df[["timestamp", "open", "high", "low", "close", "volume"]]

        except requests.exceptions.RequestException as e:
            logger.warning(f"Network error fetching {symbol} {interval}: {e}")
            time.sleep(2)
        except Exception as e:
            logger.error(f"Error fetching {symbol} {interval}: {e}")
            break

    return pd.DataFrame()

# ═══════════════════════════════════════════════════════════════════════════════
#  INDICATOR COMPUTATION
# ═══════════════════════════════════════════════════════════════════════════════

def compute_indicators(df: pd.DataFrame) -> Optional[Dict]:
    """Compute volume ratios, price momentum, SMA cross, divergence flags."""
    if df.empty or len(df) < 5:
        return None

    volumes = df["volume"].values
    closes  = df["close"].values
    opens   = df["open"].values

    avg_volume   = float(np.mean(volumes[:-1])) if len(volumes) > 1 else float(volumes[0])
    latest_vol   = float(volumes[-1])
    volume_ratio = latest_vol / avg_volume if avg_volume > 0 else 1.0

    # Price change of latest candle
    price_change_pct = ((closes[-1] - opens[-1]) / opens[-1] * 100) if opens[-1] != 0 else 0.0
    # Overall trend
    trend_pct = ((closes[-1] - opens[0]) / opens[0] * 100) if opens[0] != 0 else 0.0

    price_increasing   = trend_pct > 0
    volume_above_avg   = volume_ratio > 1.2
    bearish_divergence = volume_above_avg and price_change_pct < -0.5

    sma_bullish = False
    if len(closes) >= 20:
        sma_bullish = float(np.mean(closes[-10:])) > float(np.mean(closes[-20:]))

    return {
        "volume_ratio":       round(float(volume_ratio), 4),
        "price_change_pct":   round(float(price_change_pct), 4),
        "trend_pct":          round(float(trend_pct), 4),
        "price_increasing":   bool(price_increasing),
        "volume_above_avg":   bool(volume_above_avg),
        "bearish_divergence": bool(bearish_divergence),
        "sma_bullish":        bool(sma_bullish),
    }

# ═══════════════════════════════════════════════════════════════════════════════
#  SCORING ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

def score_timeframe(ind: Dict, category: str) -> float:
    """Score a single timeframe 0–100."""
    score = 0.0

    # A. Volume Strength (0–35)
    vr = ind["volume_ratio"]
    if   vr >= 3.0: score += 35
    elif vr >= 2.0: score += 28
    elif vr >= 1.5: score += 22
    elif vr >= 1.2: score += 15
    elif vr >= 1.0: score += 8
    else:           score += max(0, vr * 8)

    # B. Price Momentum (0–35)
    t = ind["trend_pct"]
    if   t > 5:  score += 35
    elif t > 2:  score += 28
    elif t > 0.5: score += 20
    elif t > 0:  score += 12
    elif t > -1: score += 5

    # C. SMA Confirmation (0–20)
    if ind["sma_bullish"]:       score += 20
    elif ind["price_increasing"]: score += 10

    # D. Bearish Divergence Penalty
    if ind["bearish_divergence"]:
        score -= min(20, abs(ind["price_change_pct"]) * 5)

    # E. Long-term bearish override
    if category == "long" and t < -3:
        score = min(score, 25)

    return max(0, min(100, score))


def compute_final_score(tf_scores):
    """Weighted final score → (score, decision)."""
    weighted_sum = sum(s * w for _, w, s, _, _ in tf_scores)
    total_weight = sum(w for _, w, _, _, _ in tf_scores)
    final = weighted_sum / total_weight if total_weight > 0 else 50

    if   final > BUY_THRESHOLD:  decision = "BUY 🟢"
    elif final >= HOLD_THRESHOLD: decision = "HOLD 🟡"
    else:                         decision = "AVOID 🔴"

    return round(final, 2), decision

# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN ANALYZER
# ═══════════════════════════════════════════════════════════════════════════════

class VolumePriceAnalyzer:
    def __init__(self, use_redis=True, klines_cache=None):
        self.use_redis = use_redis
        # Optional KlinesCache — when set, fetch_klines() reads from the
        # WebSocket buffer instead of issuing a REST call. Provided by the
        # --daemon mode; one-shot calls (--scan, --symbol) leave it None.
        self.klines_cache = klines_cache
        if use_redis:
            try:
                self.redis = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
                self.redis.ping()
            except Exception:
                logger.warning("Redis not available — results will only be printed.")
                self.use_redis = False

    def analyze(self, symbol: str) -> Optional[Dict]:
        # logger.info(f"  Analyzing: {symbol}")

        tf_results = []
        tf_breakdown = {}

        for tf_name, interval, limit, weight, category in TIMEFRAME_CONFIG:
            # logger.info(f"  ⏱  Fetching {tf_name:>4s} ({limit} candles) …")
            df = fetch_klines(symbol, interval, limit, cache=self.klines_cache)
            indicators = compute_indicators(df)

            if indicators is None:
                logger.warning(f"  ⚠  {tf_name}: insufficient data, skipping")
                continue

            raw_score = score_timeframe(indicators, category)
            tf_results.append((tf_name, weight, raw_score, category, indicators))
            tf_breakdown[tf_name] = {
                "category": category, "weight": weight, "score": raw_score,
                "volume_ratio": indicators["volume_ratio"],
                "price_change": indicators["price_change_pct"],
                "trend_pct": indicators["trend_pct"],
                "vol_above_avg": indicators["volume_above_avg"],
                "bearish_div": indicators["bearish_divergence"],
                "sma_bullish": indicators["sma_bullish"],
            }
            # Pace REST calls to avoid the 1200/min IP weight cap. Cache hits
            # are free, so no sleep is needed when the cache served us.
            if self.klines_cache is None or not self.klines_cache.has(symbol, interval):
                time.sleep(0.5)

        if not tf_results:
            logger.error(f"  No valid data for {symbol}")
            return None

        final_score, decision = compute_final_score(tf_results)

        # Category sub-scores
        cat_scores = {}
        for cat in ("short", "mid", "long"):
            entries = [(w, s) for (_, w, s, c, _) in tf_results if c == cat]
            if entries:
                cw = sum(w for w, _ in entries)
                cat_scores[cat] = round(sum(w * s for w, s in entries) / cw, 2) if cw > 0 else 0

        result = {
            "symbol": symbol, "decision": decision, "final_score": final_score,
            "category_scores": cat_scores, "timeframes": tf_breakdown,
            "timestamp": datetime.now().isoformat(),
        }

        # Only print full breakdown for BUY decisions
        if "BUY" in decision:
            self._print_breakdown(result)
            logger.info(f"🎯 BUY SIGNAL FOUND: {symbol} (Score: {final_score})")

        if self.use_redis:
            self._store_redis(symbol, result)

        return result

    def _store_redis(self, symbol: str, result: Dict):
        """
        Store volume analysis results in Redis:
          1. Full JSON per symbol  → sentiment:market:volume:{SYMBOL}  (10 min TTL)
          2. Summary hash entry   → VOLUME_SCORE hash field {SYMBOL}  (for dashboard)
        """
        try:
            # 1. Full result as a JSON string key (with TTL)
            key = f"{REDIS_PREFIX}:{symbol}"
            self.redis.set(key, json.dumps(result), ex=600)

            # 2. Also store in a Redis hash for fast dashboard lookups
            #    Key: VOLUME_SCORE, Field: KITEUSDT, Value: JSON summary
            hash_field = symbol.replace("/", "")
            summary = {
                "symbol":       symbol,
                "decision":     result["decision"],
                "score":        result["final_score"],
                "short":        result["category_scores"].get("short", 0),
                "mid":          result["category_scores"].get("mid", 0),
                "long":         result["category_scores"].get("long", 0),
                "timestamp":    result["timestamp"],
            }
            self.redis.hset("VOLUME_SCORE", hash_field, json.dumps(summary))

            logger.info(f"  💾 Redis: stored {key} + VOLUME_SCORE:{hash_field} (score={result['final_score']})")
        except Exception as e:
            logger.error(f"  ❌ Redis write error for {symbol}: {e}")

    def _print_breakdown(self, result: Dict):
        print(f"\n  ╔{'═' * 62}╗")
        print(f"  ║  {result['symbol']:^58s}  ║")
        print(f"  ╠{'═' * 62}╣")
        print(f"  ║  Decision:    {result['decision']:<46s}  ║")
        print(f"  ║  Final Score: {result['final_score']:<46.2f}  ║")
        print(f"  ╠{'═' * 62}╣")

        for cat, s in result["category_scores"].items():
            label = {"short": "Short-term", "mid": "Mid-term", "long": "Long-term"}[cat]
            bar = "█" * int(s / 100 * 30)
            print(f"  ║  {label:>12s}: {s:>6.2f}  {bar:<30s}  ║")

        print(f"  ╠{'═' * 62}╣")
        print(f"  ║  {'TF':>4s} │ {'Score':>5s} │ {'VolRatio':>8s} │ {'PriceChg':>8s} │ {'Trend':>7s} │ Flags        ║")
        print(f"  ╟{'─' * 62}╢")

        for tf, d in result["timeframes"].items():
            flags = []
            if d["vol_above_avg"]: flags.append("📊")
            if d["bearish_div"]:   flags.append("⚠️")
            if d["sma_bullish"]:   flags.append("✅")
            f_str = " ".join(flags) if flags else "—"
            print(f"  ║  {tf:>4s} │ {d['score']:>5.1f} │ {d['volume_ratio']:>8.2f} │ "
                  f"{d['price_change']:>+7.2f}% │ {d['trend_pct']:>+6.2f}% │ {f_str:<12s} ║")

        print(f"  ╚{'═' * 62}╝\n")


def scan_fast_movers(analyzer, symbols=None):
    """Scan multiple symbols and print ranked summary."""
    if symbols is None:
        try:
            r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
            fm = r.hkeys("FAST_MOVE")
            symbols = [k.replace("USDT", "/USDT") for k in fm[:20]] if fm else ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
        except Exception:
            symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]

    results = []
    cache_hot = analyzer.klines_cache is not None
    for sym in symbols:
        try:
            res = analyzer.analyze(sym)
            if res: results.append(res)
            # Pace REST scans; cache-driven daemon scans don't need this gap.
            if not cache_hot:
                time.sleep(2.0)
        except Exception as e:
            logger.error(f"Error analyzing {sym}: {e}")

    if not results: return

    results.sort(key=lambda r: r["final_score"], reverse=True)
    print("\n" + "═" * 72)
    print(f"  {'RANK':>4s}  {'SYMBOL':<12s}  {'SCORE':>6s}  {'DECISION':<12s}  {'SHORT':>6s}  {'MID':>6s}  {'LONG':>6s}")
    print("─" * 72)
    for i, r in enumerate(results, 1):
        cs = r.get("category_scores", {})
        print(f"  {i:>4d}  {r['symbol']:<12s}  {r['final_score']:>6.1f}  {r['decision']:<12s}  "
              f"{cs.get('short', 0):>6.1f}  {cs.get('mid', 0):>6.1f}  {cs.get('long', 0):>6.1f}")
    print("═" * 72 + "\n")


async def _daemon_loop(analyzer: "VolumePriceAnalyzer", cache, top_n: int = DAEMON_TOP_N):
    """
    Long-running scan loop.

    Workflow per cycle:
      1. Read top-N hot symbols from Redis hash `FAST_MOVE` (populated by the
         Spring backend's allRollingWindowTicker pipeline).
      2. Subscribe each one to the live-interval kline streams via the
         shared cache; release any symbol that's no longer hot.
      3. Wait long enough for fresh klines to arrive (initial seed runs in
         parallel for all symbols; ~5s buffer time after first registration).
      4. Score every subscribed symbol — these calls are free because every
         live timeframe reads from the WS buffer rather than REST.
      5. Sleep until the next cycle.
    """
    if analyzer.use_redis:
        r = analyzer.redis
    else:
        r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

    last_scan = 0.0
    while True:
        try:
            # Pull current hot list
            symbols_raw = r.hkeys("FAST_MOVE") or []
            symbols = [s for s in symbols_raw[:top_n] if s.endswith("USDT")]
            if not symbols:
                logger.info("[Daemon] FAST_MOVE empty — sleeping %ds", DAEMON_REFRESH_WATCH_S)
                await asyncio.sleep(DAEMON_REFRESH_WATCH_S)
                continue

            # Add any new symbols to the cache; drop stale ones
            for sym in symbols:
                await cache.ensure(sym, LIVE_INTERVALS)
            await cache.release(set(symbols))

            now = time.time()
            if now - last_scan >= DAEMON_SCAN_INTERVAL_S:
                logger.info("[Daemon] Scoring %d symbols (cache-driven, no REST for live timeframes)…",
                            len(symbols))
                # Convert to slash format for the existing scoring code
                slash_syms = [s.replace("USDT", "/USDT") for s in symbols]
                for sym in slash_syms:
                    try:
                        analyzer.analyze(sym)
                    except Exception as e:
                        logger.error("[Daemon] analyze %s failed: %s", sym, e)
                last_scan = now

            await asyncio.sleep(DAEMON_REFRESH_WATCH_S)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("[Daemon] iteration error: %s", e)
            await asyncio.sleep(5)


async def _run_daemon(use_redis: bool):
    if KlinesCache is None:
        logger.error("KlinesCache not importable — install `websockets` and ensure "
                     "klines_ws_cache.py is on PYTHONPATH.")
        return
    cache = KlinesCache(intervals=LIVE_INTERVALS, buffer_size=200)
    await cache.start()
    analyzer = VolumePriceAnalyzer(use_redis=use_redis, klines_cache=cache)
    try:
        await _daemon_loop(analyzer, cache)
    finally:
        await cache.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Multi-Timeframe Volume & Price Analyzer")
    parser.add_argument("--symbol", "-s", default=None)
    parser.add_argument("--scan", action="store_true",
                        help="One-shot scan of FAST_MOVE symbols (REST-only).")
    parser.add_argument("--daemon", action="store_true",
                        help="Long-running scanner driven by a WS kline cache. "
                             "Replaces the 10-min subprocess spawn.")
    parser.add_argument("--symbols", nargs="+")
    parser.add_argument("--no-redis", action="store_true")
    args = parser.parse_args()

    if args.daemon:
        try:
            asyncio.run(_run_daemon(use_redis=not args.no_redis))
        except KeyboardInterrupt:
            logger.info("[Daemon] interrupted, exiting")
    else:
        analyzer = VolumePriceAnalyzer(use_redis=not args.no_redis)
        if args.symbol:
            analyzer.analyze(args.symbol)
        elif args.scan or args.symbols:
            scan_fast_movers(analyzer, args.symbols)
        else:
            sym = input("Enter symbol (e.g. BTC/USDT): ").strip().upper() or "BTC/USDT"
            analyzer.analyze(sym)
