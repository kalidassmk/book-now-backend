import logging
import sys
from pathlib import Path

import ccxt.async_support as ccxt

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
try:
    from klines_ws_cache import KlinesCache  # type: ignore
except Exception:
    KlinesCache = None  # type: ignore

log = logging.getLogger("volume_profile.fetcher")


class DataFetcher:
    """
    Fetches kline data from Binance Spot.

    Volume-profile analysis pulls up to 500 candles per call, so the cache
    is sized at 600 to keep the read path purely in-memory after the first
    seed. Falls back to CCXT REST when the buffer is empty or disabled.
    """

    def __init__(self, apiKey=None, secret=None, use_ws_cache: bool = True):
        self.client = ccxt.binance({
            'apiKey': apiKey,
            'secret': secret,
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })
        self.cache = (
            KlinesCache(buffer_size=600)
            if (use_ws_cache and KlinesCache is not None)
            else None
        )
        self._cache_started = False

    async def _ensure_cache(self, api_symbol: str, interval: str):
        if self.cache is None:
            return
        if not self._cache_started:
            await self.cache.start()
            self._cache_started = True
        await self.cache.ensure(api_symbol, [interval])

    @staticmethod
    def _df_to_ccxt(df) -> list:
        return [
            [int(row.timestamp.value // 1_000_000),
             float(row.open), float(row.high), float(row.low),
             float(row.close), float(row.volume)]
            for row in df.itertuples(index=False)
        ]

    async def fetch_klines(self, symbol, interval="5m", limit=500):
        try:
            ccxt_symbol = symbol if "/" in symbol else f"{symbol[:-4]}/{symbol[-4:]}"

            if self.cache is not None:
                api_symbol = ccxt_symbol.replace("/", "")
                await self._ensure_cache(api_symbol, interval)
                if self.cache.has(api_symbol, interval):
                    df = self.cache.get_klines(api_symbol, interval, limit)
                    if not df.empty and len(df) >= min(limit, 50):
                        return self._df_to_ccxt(df)

            return await self.client.fetch_ohlcv(ccxt_symbol, timeframe=interval, limit=limit)
        except Exception as e:
            log.error(f"CCXT Request failed for {symbol}: {e}")
            return None

    async def close(self):
        if self.cache is not None:
            try:
                await self.cache.stop()
            except Exception:
                pass
        await self.client.close()
