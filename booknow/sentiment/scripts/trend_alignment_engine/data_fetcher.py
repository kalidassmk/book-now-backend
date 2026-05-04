import asyncio
import logging
import sys
from pathlib import Path

import ccxt.async_support as ccxt

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
try:
    from klines_ws_cache import KlinesCache  # type: ignore
except Exception:
    KlinesCache = None  # type: ignore

log = logging.getLogger("trend_alignment.fetcher")


class DataFetcher:
    """
    Fetches multi-timeframe kline data from Binance Spot.

    Reads from a multiplexed WebSocket buffer when warm. The trend-
    alignment engine pulls 6 intervals (5m … 1w) per scan, so the buffer
    is sized at 250 (enough for 1w with comfortable headroom and >> the
    usual limit=100 ask). REST is the fallback during cold start.
    """

    LIVE_INTERVALS = ("5m", "15m", "1h", "4h", "1d", "1w")

    def __init__(self, apiKey=None, secret=None, use_ws_cache: bool = True):
        self.client = ccxt.binance({
            'apiKey': apiKey,
            'secret': secret,
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })
        self.cache = (
            KlinesCache(intervals=self.LIVE_INTERVALS, buffer_size=250)
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

    async def fetch_timeframe(self, symbol, interval, limit=100):
        try:
            ccxt_symbol = symbol if "/" in symbol else f"{symbol[:-4]}/{symbol[-4:]}"

            if self.cache is not None:
                api_symbol = ccxt_symbol.replace("/", "")
                await self._ensure_cache(api_symbol, interval)
                if self.cache.has(api_symbol, interval):
                    df = self.cache.get_klines(api_symbol, interval, limit)
                    if not df.empty:
                        return interval, self._df_to_ccxt(df)

            data = await self.client.fetch_ohlcv(ccxt_symbol, timeframe=interval, limit=limit)
            return interval, data
        except Exception as e:
            log.error(f"CCXT Request failed for {symbol} @ {interval}: {e}")
            return interval, None

    async def fetch_multi_timeframe(self, symbol, intervals=None):
        if intervals is None:
            intervals = list(self.LIVE_INTERVALS)
        tasks = [self.fetch_timeframe(symbol, interval) for interval in intervals]
        results = await asyncio.gather(*tasks)
        return {interval: data for interval, data in results if data}

    async def close(self):
        if self.cache is not None:
            try:
                await self.cache.stop()
            except Exception:
                pass
        await self.client.close()
