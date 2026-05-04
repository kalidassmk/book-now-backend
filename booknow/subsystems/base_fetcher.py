"""
base_fetcher.py
─────────────────────────────────────────────────────────────────────────────
Shared kline-fetcher used by every Phase-13 subsystem.

Replaces the legacy CCXT-backed ``DataFetcher`` classes in
binance-sentiment-engine/{risk_management_engine,fakeout_detector_system,
volume_profile_trader,trend_alignment_engine}. They all had the same
shape:

  1. Try the multiplexed ``KlinesCache`` first (one shared WS connection
     across the whole engine).
  2. On cold-start (cache not yet warm) fall back to a single REST hit
     to ``/api/v3/klines``.
  3. Return CCXT-shape ``[[open_time_ms, o, h, l, c, v], ...]`` so the
     subsystem strategy code that already expects that shape doesn't
     have to change.

Differences from the legacy version:
  * No CCXT dependency — REST fallback uses ``httpx`` directly.
  * Only one ``KlinesCache`` instance per engine (owned by the registry)
    so the trading core's klines cache, the trend-alignment fetcher and
    the volume-profile fetcher all share the same WebSocket.
  * Honours :class:`RateLimitGuard` on the REST fallback so a Binance
    ban anywhere in the engine pauses these fetches too.
"""

from __future__ import annotations

import logging
from typing import List, Optional

import httpx

from booknow.binance.klines_cache import KlinesCache
from booknow.binance.rate_limit import get_default as _get_rate_limit_guard


logger = logging.getLogger("booknow.subsystems.fetcher")


_BINANCE_REST = "https://api.binance.com"
_REST_TIMEOUT_S = 10.0


class KlinesFetcher:
    """Async kline fetcher, cache-first.

    Subclasses just override the default ``interval`` / ``limit`` to
    match the legacy subsystem they replace.
    """

    default_interval: str = "5m"
    default_limit: int = 200

    def __init__(
        self,
        cache: KlinesCache,
        http_client: Optional[httpx.AsyncClient] = None,
        log_name: str = "booknow.subsystems.fetcher",
    ):
        self._cache = cache
        # If the registry didn't pass a client, build one. We keep it
        # private so callers can still treat the fetcher as standalone.
        self._http = http_client or httpx.AsyncClient(
            timeout=_REST_TIMEOUT_S,
            verify=False,  # match other Binance modules' SSL convention
            headers={"User-Agent": "booknow-engine/0.1"},
        )
        self._owns_http = http_client is None
        self._guard = _get_rate_limit_guard()
        self.log = logging.getLogger(log_name)

    # ── Public ────────────────────────────────────────────────────────────

    async def fetch_klines(
        self,
        symbol: str,
        interval: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Optional[list]:
        """Return up to ``limit`` candles for ``(symbol, interval)``.

        ``symbol`` accepts both ``"BTCUSDT"`` and ``"BTC/USDT"``; both
        are normalised to Binance's API form.
        """
        api_symbol = self._normalise_symbol(symbol)
        iv = interval or self.default_interval
        n  = limit if limit is not None else self.default_limit

        # 1) Cache path. ensure() seeds via the cache's own REST helper
        # the first time we see the pair; subsequent calls are pure WS.
        try:
            await self._cache.ensure(api_symbol, [iv])
            if self._cache.has(api_symbol, iv):
                df = self._cache.get_klines(api_symbol, iv, n)
                if not df.empty:
                    return self._df_to_ccxt(df)
        except Exception as e:
            self.log.debug("cache path failed for %s %s: %s", api_symbol, iv, e)

        # 2) REST fallback — direct, no CCXT.
        return await self._fetch_rest(api_symbol, iv, n)

    async def close(self) -> None:
        """Close any resources owned by this fetcher.

        The cache is owned by the registry, so we only close our own
        HTTP client when we created it.
        """
        if self._owns_http and self._http is not None:
            try:
                await self._http.aclose()
            except Exception:
                pass

    # ── Internals ─────────────────────────────────────────────────────────

    @staticmethod
    def _normalise_symbol(symbol: str) -> str:
        return symbol.replace("/", "").upper()

    @staticmethod
    def _df_to_ccxt(df) -> List[list]:
        """``KlinesCache.get_klines`` returns a DataFrame of dict rows;
        translate it back to ``[[ts, o, h, l, c, v], ...]``."""
        return [
            [
                int(row.timestamp.value // 1_000_000),
                float(row.open),
                float(row.high),
                float(row.low),
                float(row.close),
                float(row.volume),
            ]
            for row in df.itertuples(index=False)
        ]

    async def _fetch_rest(self, symbol: str, interval: str, limit: int) -> Optional[list]:
        if self._guard.is_banned():
            self.log.warning(
                "REST kline skipped for %s %s — Binance ban active for %ds",
                symbol, interval, self._guard.ban_remaining_seconds(),
            )
            return None
        try:
            r = await self._http.get(
                f"{_BINANCE_REST}/api/v3/klines",
                params={"symbol": symbol, "interval": interval, "limit": limit},
            )
            if r.status_code in (418, 429) or (
                r.status_code >= 400 and "banned" in r.text.lower()
            ):
                self._guard.report_if_banned(RuntimeError(r.text))
                return None
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            if self._guard.report_if_banned(e):
                return None
            self.log.warning("REST kline failed for %s %s: %s", symbol, interval, e)
            return None

        # Binance REST returns 12-element rows. We keep the 6 we need.
        return [
            [int(k[0]), float(k[1]), float(k[2]), float(k[3]),
             float(k[4]), float(k[5])]
            for k in data
        ]
