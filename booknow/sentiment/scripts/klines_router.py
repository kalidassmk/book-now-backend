"""
klines_router.py
─────────────────────────────────────────────────────────────────────────────
Sync, fault-tolerant OHLCV fetcher with multi-source fallback for the
sentiment subprocess scripts.

Why this exists
---------------
Hot-path scripts (profit_020_trend_analyzer.py, success_pattern_recorder.py)
were calling ``ccxt.binance().fetch_ohlcv()`` directly. With many active
symbols and frequent profit-hit events that hammered Binance's IP weight
budget and produced 418/429 bans.

Source priority
---------------
0. Binance WebSocket kline cache (preferred — zero REST cost after one
   per-(symbol, interval) seed; subscriptions stay warm)
1. Binance via CCXT (REST fallback when WS cache is cold or seed fails)
2. Bybit via CCXT  ┐
3. OKX via CCXT    │ round-robin rotation across these three so a single
4. KuCoin via CCXT ┘ exchange's rate limit doesn't bottleneck the engine
5. CryptoCompare REST  (final fallback; requires CRYPTOCOMPARE_API_KEY env)

The router rotates the start position on each successful call so load is
distributed across exchanges. On RateLimitExceeded / DDoSProtection /
NetworkError it falls through to the next source automatically.

Cache
-----
Results are cached for ``CACHE_TTL_SEC`` seconds keyed by
``(symbol, timeframe, limit)`` so duplicate calls within the same window
don't re-hit the network.

Output shape
------------
``fetch_ohlcv()`` returns a list of ``[timestamp_ms, open, high, low,
close, volume]`` rows, matching ``ccxt.fetch_ohlcv()``. Empty list on
total failure.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import List, Optional

import ccxt
import requests

log = logging.getLogger("booknow.klines_router")

# Order matters: Binance is primary, others rotate behind it.
PRIMARY = "binance"
ROTATION = ("bybit", "okx", "kucoin")

# CryptoCompare timeframe -> (endpoint, aggregate). 1s isn't available there.
_CC_ENDPOINT = {
    "1m":  ("histominute", 1),
    "3m":  ("histominute", 3),
    "5m":  ("histominute", 5),
    "15m": ("histominute", 15),
    "30m": ("histominute", 30),
    "1h":  ("histohour",   1),
    "2h":  ("histohour",   2),
    "4h":  ("histohour",   4),
    "1d":  ("histoday",    1),
}

# Quotes the normalizer recognizes when collapsing concatenated symbols.
_KNOWN_QUOTES = ("USDT", "USDC", "BUSD", "FDUSD", "TUSD", "BTC", "ETH", "BNB")

CACHE_TTL_SEC = 30


class KlinesRouter:
    def __init__(self, use_ws_cache: bool = True):
        self._exchanges: dict[str, ccxt.Exchange] = {}
        for name in (PRIMARY, *ROTATION):
            try:
                cls = getattr(ccxt, name)
                self._exchanges[name] = cls({
                    "enableRateLimit": True,
                    "timeout": 10000,
                    "options": {"defaultType": "spot"},
                })
            except Exception as e:
                log.warning("Could not init exchange %s: %s", name, e)

        self._markets_loaded: dict[str, bool] = {n: False for n in self._exchanges}
        self._cooldown_until: dict[str, float] = {n: 0.0 for n in self._exchanges}
        self._rotation_offset = 0
        self._cc_key = os.getenv("CRYPTOCOMPARE_API_KEY", "").strip() or None

        self._cache: dict[tuple, tuple[float, list]] = {}
        self._lock = threading.Lock()

        # Optional WS-backed kline cache (preferred source — zero REST cost
        # after a one-time per-(symbol, interval) seed). The router stays
        # functional if the WS cache can't be built (missing package, etc.).
        self._ws_cache = None
        if use_ws_cache:
            try:
                from ws_klines_cache import get_default_ws_cache
                self._ws_cache = get_default_ws_cache()
            except Exception as e:
                log.warning("[KlinesRouter] WS cache import failed, REST-only: %s", e)

        log.info(
            "[KlinesRouter] ready — ws=%s primary=%s rotation=%s cryptocompare=%s",
            "enabled" if self._ws_cache else "disabled",
            PRIMARY, list(ROTATION),
            "enabled" if self._cc_key else "disabled (no CRYPTOCOMPARE_API_KEY)",
        )

    # ── Public API ────────────────────────────────────────────────────────

    def fetch_ohlcv(self, symbol: str, timeframe: str = "1m", limit: int = 60) -> List[list]:
        ccxt_symbol = self._normalize(symbol)
        cache_key = (ccxt_symbol, timeframe, limit)

        with self._lock:
            entry = self._cache.get(cache_key)
            if entry and time.time() - entry[0] < CACHE_TTL_SEC:
                return entry[1]

        # 0. WS cache (preferred). Binance uses concatenated symbols like BTCUSDT.
        if self._ws_cache:
            ws_symbol = ccxt_symbol.replace("/", "")
            # Warm path: buffer already populated → free read.
            if self._ws_cache.has(ws_symbol, timeframe):
                data = self._ws_cache.get_ohlcv(ws_symbol, timeframe, limit)
                if data:
                    return self._cache_and_return(cache_key, data)
            # Cold path: one-time REST seed + WS subscribe, then read.
            if self._ws_cache.ensure(ws_symbol, [timeframe], timeout=10):
                data = self._ws_cache.get_ohlcv(ws_symbol, timeframe, limit)
                if data:
                    return self._cache_and_return(cache_key, data)

        # 1. Binance REST.
        data = self._try_ccxt(PRIMARY, ccxt_symbol, timeframe, limit)
        if data:
            return self._cache_and_return(cache_key, data)

        # Then rotate through Bybit / OKX / KuCoin.
        n = len(ROTATION)
        for offset in range(n):
            name = ROTATION[(self._rotation_offset + offset) % n]
            data = self._try_ccxt(name, ccxt_symbol, timeframe, limit)
            if data:
                self._rotation_offset = (self._rotation_offset + offset + 1) % n
                return self._cache_and_return(cache_key, data)

        # Final fallback: CryptoCompare.
        if self._cc_key and "/" in ccxt_symbol:
            base, quote = ccxt_symbol.split("/", 1)
            cc = self._try_cryptocompare(base, quote, timeframe, limit)
            if cc:
                return self._cache_and_return(cache_key, cc)

        log.error("All kline sources exhausted for %s %s limit=%d", symbol, timeframe, limit)
        return []

    # ── Internals ─────────────────────────────────────────────────────────

    def _cache_and_return(self, key: tuple, data: list) -> list:
        with self._lock:
            self._cache[key] = (time.time(), data)
            # Cheap bound on cache size.
            if len(self._cache) > 4096:
                # Drop the oldest 1024 entries.
                cutoff = sorted((ts, k) for k, (ts, _) in self._cache.items())[1024][0]
                self._cache = {k: v for k, v in self._cache.items() if v[0] >= cutoff}
        return data

    def _normalize(self, symbol: str) -> str:
        """BTCUSDT / BTC-USDT / BTC/USDT -> BTC/USDT (CCXT canonical)."""
        s = symbol.upper().replace("-", "").replace("/", "")
        for q in _KNOWN_QUOTES:
            if s.endswith(q):
                base = s[: -len(q)]
                if base:
                    return f"{base}/{q}"
        return symbol  # last-resort fallthrough

    def _ensure_markets(self, name: str) -> bool:
        if self._markets_loaded[name]:
            return True
        try:
            self._exchanges[name].load_markets()
            self._markets_loaded[name] = True
            return True
        except Exception as e:
            log.warning("[%s] load_markets failed: %s", name, e)
            return False

    def _try_ccxt(self, name: str, ccxt_symbol: str, timeframe: str, limit: int) -> Optional[list]:
        ex = self._exchanges.get(name)
        if not ex:
            return None

        # Honour per-exchange cooldown after a rate-limit hit.
        now = time.time()
        if now < self._cooldown_until[name]:
            return None

        if not self._ensure_markets(name):
            return None

        # Some exchanges don't list every coin or every timeframe.
        if ccxt_symbol not in (ex.symbols or []):
            return None
        if timeframe not in (ex.timeframes or {}):
            return None

        try:
            return ex.fetch_ohlcv(ccxt_symbol, timeframe, limit=limit)
        except (ccxt.RateLimitExceeded, ccxt.DDoSProtection) as e:
            cooldown = 60 if name == PRIMARY else 30
            self._cooldown_until[name] = time.time() + cooldown
            log.warning("[%s] rate-limited (%s); cooling %ds", name, e.__class__.__name__, cooldown)
            return None
        except (ccxt.NetworkError, ccxt.ExchangeNotAvailable) as e:
            self._cooldown_until[name] = time.time() + 10
            log.warning("[%s] network/unavailable: %s", name, e)
            return None
        except Exception as e:
            log.warning("[%s] fetch_ohlcv %s %s failed: %s", name, ccxt_symbol, timeframe, e)
            return None

    def _try_cryptocompare(self, base: str, quote: str, timeframe: str, limit: int) -> Optional[list]:
        ep = _CC_ENDPOINT.get(timeframe)
        if not ep:
            return None  # CC doesn't have 1s, etc.
        endpoint, agg = ep
        url = f"https://min-api.cryptocompare.com/data/v2/{endpoint}"
        try:
            r = requests.get(
                url,
                params={
                    "fsym": base,
                    "tsym": quote,
                    "limit": max(1, limit),
                    "aggregate": agg,
                    "api_key": self._cc_key,
                },
                timeout=10,
            )
            r.raise_for_status()
            payload = r.json()
        except Exception as e:
            log.warning("CryptoCompare fetch failed %s/%s %s: %s", base, quote, timeframe, e)
            return None

        if payload.get("Response") != "Success":
            log.warning("CryptoCompare error %s/%s: %s", base, quote, payload.get("Message"))
            return None

        rows = payload.get("Data", {}).get("Data") or []
        if not rows:
            return None

        return [
            [
                int(c["time"]) * 1000,
                float(c.get("open") or 0),
                float(c.get("high") or 0),
                float(c.get("low") or 0),
                float(c.get("close") or 0),
                float(c.get("volumeto") or c.get("volumefrom") or 0),
            ]
            for c in rows
        ]


# ── Singleton accessor ────────────────────────────────────────────────────

_default: Optional[KlinesRouter] = None
_default_lock = threading.Lock()


def get_default_router() -> KlinesRouter:
    global _default
    if _default is None:
        with _default_lock:
            if _default is None:
                _default = KlinesRouter()
    return _default
