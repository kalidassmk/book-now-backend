import asyncio
import logging
import sys
from pathlib import Path

import ccxt.async_support as ccxt

# Reuse the shared KlinesCache via the parent sentiment-scripts shim.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
try:
    from klines_ws_cache import KlinesCache  # type: ignore
except Exception:
    KlinesCache = None  # type: ignore

log = logging.getLogger("btc_filter.fetcher")


class DataFetcher:
    """
    Fetches synchronized kline data for target symbol and BTCUSDT.

    Spot klines are read from a multiplexed WebSocket buffer when warm
    (zero REST cost), with a per-(symbol, interval) one-time REST seed.
    Falls back to CCXT REST when the buffer is empty or WS disabled.
    """

    def __init__(self, apiKey=None, secret=None, use_ws_cache: bool = True):
        self.client = ccxt.binance({
            'apiKey': apiKey,
            'secret': secret,
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })
        self.cache = KlinesCache() if (use_ws_cache and KlinesCache is not None) else None
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

    async def fetch_pair(self, symbol, interval="15m", limit=100):
        """
        Fetches both the target symbol and BTC in parallel.
        (Deprecated efficiency-wise, use fetch_single + cached BTC instead)
        """
        tasks = [
            self.fetch_single(symbol, interval, limit),
            self.fetch_single("BTCUSDT", interval, limit)
        ]
        results = await asyncio.gather(*tasks)
        return results[0], results[1]

    async def fetch_single(self, symbol, interval="15m", limit=100):
        """
        Fetches kline data for a single symbol via the WS cache, REST fallback.
        """
        try:
            ccxt_symbol = symbol if "/" in symbol else f"{symbol[:-4]}/{symbol[-4:]}"

            if self.cache is not None:
                api_symbol = ccxt_symbol.replace("/", "")
                await self._ensure_cache(api_symbol, interval)
                if self.cache.has(api_symbol, interval):
                    df = self.cache.get_klines(api_symbol, interval, limit)
                    if not df.empty:
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
