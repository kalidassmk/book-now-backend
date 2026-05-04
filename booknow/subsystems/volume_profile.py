"""
volume_profile.py
─────────────────────────────────────────────────────────────────────────────
Phase-13 port of ``volume_profile_trader/data_fetcher.py``.

Volume-profile analysis wants up to 500 candles per call (5m default)
so the histogram has enough samples to find a meaningful POC. The
shared :class:`KlinesCache` is sized to 600 in the registry to give
this fetcher a fully in-memory read path after the first seed.

Adds a thin "minimum useful history" guard mirroring the legacy
fetcher: if the cache holds fewer than ``min_samples`` rows we fall
through to REST so the caller doesn't compute on a half-filled window.
"""

from typing import Optional

from booknow.subsystems.base_fetcher import KlinesFetcher


class VolumeProfileFetcher(KlinesFetcher):
    default_interval = "5m"
    default_limit = 500
    min_samples = 50

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("log_name", "booknow.subsystems.volume_profile")
        super().__init__(*args, **kwargs)

    async def fetch_klines(
        self,
        symbol: str,
        interval: Optional[str] = None,
        limit: Optional[int] = None,
    ):
        api_symbol = self._normalise_symbol(symbol)
        iv = interval or self.default_interval
        n  = limit if limit is not None else self.default_limit

        try:
            await self._cache.ensure(api_symbol, [iv])
            if self._cache.has(api_symbol, iv):
                df = self._cache.get_klines(api_symbol, iv, n)
                # Same gate as legacy: require enough rows for VP math.
                if not df.empty and len(df) >= min(n, self.min_samples):
                    return self._df_to_ccxt(df)
        except Exception as e:
            self.log.debug("cache path failed for %s %s: %s", api_symbol, iv, e)

        return await self._fetch_rest(api_symbol, iv, n)
