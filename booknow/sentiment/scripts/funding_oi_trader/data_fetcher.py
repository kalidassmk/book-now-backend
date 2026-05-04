import ccxt.async_support as ccxt
import asyncio
import logging
import time

log = logging.getLogger("funding_oi.fetcher")

class DataFetcher:
    """
    Fetches market data from Binance Futures using CCXT.
    """
    def __init__(self, apiKey=None, secret=None):
        self.client = ccxt.binance({
            'apiKey': apiKey,
            'secret': secret,
            'enableRateLimit': True,
            'options': {'defaultType': 'swap'} # Perpetuals
        })

    async def fetch_klines(self, symbol, interval="5m", limit=100):
        try:
            ccxt_symbol = symbol if "/" in symbol else f"{symbol[:-4]}/{symbol[-4:]}"
            return await self.client.fetch_ohlcv(ccxt_symbol, timeframe=interval, limit=limit)
        except Exception as e:
            log.error(f"Error fetching klines for {symbol}: {e}")
            return None

    async def fetch_funding_rate(self, symbol, limit=1):
        try:
            ccxt_symbol = symbol if "/" in symbol else f"{symbol[:-4]}/{symbol[-4:]}"
            # CCXT fetch_funding_rate returns recent funding rate
            rates = await self.client.fetch_funding_rate_history(ccxt_symbol, limit=limit)
            return rates
        except Exception as e:
            log.error(f"Error fetching funding rate for {symbol}: {e}")
            return None

    async def fetch_open_interest(self, symbol):
        try:
            ccxt_symbol = symbol if "/" in symbol else f"{symbol[:-4]}/{symbol[-4:]}"
            return await self.client.fetch_open_interest(ccxt_symbol)
        except Exception as e:
            log.error(f"Error fetching OI for {symbol}: {e}")
            return None

    async def fetch_open_interest_hist(self, symbol, interval="5m", limit=30):
        try:
            ccxt_symbol = symbol if "/" in symbol else f"{symbol[:-4]}/{symbol[-4:]}"
            return await self.client.fetch_open_interest_history(ccxt_symbol, timeframe=interval, limit=limit)
        except Exception as e:
            log.error(f"Error fetching OI history for {symbol}: {e}")
            return None

    async def list_futures_symbols(self):
        """
        Return the set of perpetual-swap symbols actually available on
        Binance Futures (in REST naming, e.g. 'BTCUSDT'). Used to pre-filter
        a spot-derived watch list so we don't spam -1121 errors every cycle
        for symbols that simply have no futures market.
        """
        try:
            markets = await self.client.load_markets()
            available = set()
            for ccxt_id, m in markets.items():
                # Keep only USDT-margined perpetual swaps that are currently active.
                if m.get("type") == "swap" and m.get("active") and m.get("quote") == "USDT":
                    base = m.get("base") or ""
                    available.add(f"{base}USDT")
            return available
        except Exception as e:
            log.error(f"Failed to load futures markets: {e}")
            return None

    async def close(self):
        await self.client.close()
