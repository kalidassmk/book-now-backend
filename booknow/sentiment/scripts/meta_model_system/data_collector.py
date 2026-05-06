import asyncio
import sys
from pathlib import Path

import ccxt.async_support as ccxt
import logging

# Optional imports
try:
    import pandas as pd
    import numpy as np
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

# Reuse the parent sentiment-scripts dir's KlinesCache shim — same pattern
# as volume_profile_trader / fakeout_detector_system / risk_management_engine.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
try:
    from klines_ws_cache import KlinesCache  # type: ignore
except Exception:
    KlinesCache = None  # type: ignore

log = logging.getLogger("meta_model.collector")


class DataCollector:
    """
    Collects features from multiple trading sub-systems.

    Spot klines are read from a multiplexed WebSocket buffer when warm
    (zero REST cost), with a per-(symbol, interval) one-time REST seed.
    Futures-only endpoints (funding rate, open interest) stay on REST
    since no shared WS cache exists for them.
    """
    def __init__(self, apiKey=None, secret=None, use_ws_cache: bool = True):
        self.spot_client = ccxt.binance({
            'apiKey': apiKey,
            'secret': secret,
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })
        self.futures_client = ccxt.binance({
            'apiKey': apiKey,
            'secret': secret,
            'enableRateLimit': True,
            'options': {'defaultType': 'swap'}
        })
        # Buffer-size 1100 so the 1000-limit fetch_historical_klines call
        # below has headroom for the in-progress candle.
        self.cache = (
            KlinesCache(buffer_size=1100)
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

    async def fetch_all_features(self, symbol):
        """
        Gathers features across all categories.
        """
        try:
            klines = await self._fetch_klines(symbol)
            funding = await self._fetch_funding(symbol)
            oi = await self._fetch_oi(symbol)
            
            if not klines: return None
            
            # CCXT klines: [ts, o, h, l, c, v]
            current_price = float(klines[-1][4])
            
            if HAS_PANDAS:
                # CCXT returns 6 columns
                df = pd.DataFrame(klines, columns=['t', 'o', 'h', 'l', 'c', 'v'])
                df[['o','h','l','c','v']] = df[['o','h','l','c','v']].astype(float)
                rsi = self._calculate_rsi(df['c'])
                volatility = df['h'].iloc[-20:].max() - df['l'].iloc[-20:].min()
                volume_spike = df['v'].iloc[-1] / df['v'].iloc[-20:-1].mean()
            else:
                rsi = 50.0
                volatility = 0.0
                volume_spike = 1.0

            # Adapt funding and OI
            # funding from CCXT fetch_funding_rate_history
            f_rate = 0.0
            if funding:
                latest_f = funding[-1] if isinstance(funding, list) else funding
                f_rate = latest_f.get('fundingRate', latest_f.get('rate', 0))

            # OI from CCXT fetch_open_interest
            oi_val = 0.0
            if oi:
                oi_val = oi.get('openInterest', oi.get('sumOpenInterest', 0))

            features = {
                "symbol": symbol,
                "price": current_price,
                "rsi": rsi,
                "price_change_5m": (current_price - float(klines[-2][4])) / float(klines[-2][4]) if len(klines) > 1 else 0.0,
                "funding_rate": float(f_rate),
                "oi_change": float(oi_val), # Simplified as direct value for now
                "volatility": volatility,
                "volume_spike": volume_spike
            }
            return features
        except Exception as e:
            log.error(f"Error collecting features for {symbol}: {e}")
            return None

    async def _fetch_klines(self, symbol):
        try:
            ccxt_symbol = symbol if "/" in symbol else f"{symbol[:-4]}/{symbol[-4:]}"
            interval = '5m'
            limit = 100

            if self.cache is not None:
                api_symbol = ccxt_symbol.replace("/", "")
                await self._ensure_cache(api_symbol, interval)
                if self.cache.has(api_symbol, interval):
                    df = self.cache.get_klines(api_symbol, interval, limit)
                    if not df.empty:
                        return self._df_to_ccxt(df)

            return await self.spot_client.fetch_ohlcv(ccxt_symbol, timeframe=interval, limit=limit)
        except Exception as e:
            log.error(f"Error fetching klines for {symbol}: {e}")
            return None

    async def _fetch_funding(self, symbol):
        try:
            ccxt_symbol = symbol if "/" in symbol else f"{symbol[:-4]}/{symbol[-4:]}"
            return await self.futures_client.fetch_funding_rate_history(ccxt_symbol, limit=1)
        except Exception as e:
            log.error(f"Error fetching funding for {symbol}: {e}")
            return None

    async def _fetch_oi(self, symbol):
        try:
            ccxt_symbol = symbol if "/" in symbol else f"{symbol[:-4]}/{symbol[-4:]}"
            return await self.futures_client.fetch_open_interest(ccxt_symbol)
        except Exception as e:
            # log.error(f"Error fetching OI for {symbol}: {e}")
            return None

    async def fetch_historical_klines(self, symbol, interval="5m", limit=1000):
        """Fetches a larger batch of klines for training.

        Routes through the WS cache when warm; falls back to REST for the
        first call (large training fetches usually only run once per
        symbol per training cycle, so the cold-start cost is acceptable)."""
        try:
            ccxt_symbol = symbol if "/" in symbol else f"{symbol[:-4]}/{symbol[-4:]}"
            log.info(f"[{symbol}] Fetching {limit} historical klines for training...")

            if self.cache is not None:
                api_symbol = ccxt_symbol.replace("/", "")
                await self._ensure_cache(api_symbol, interval)
                if self.cache.has(api_symbol, interval):
                    df = self.cache.get_klines(api_symbol, interval, limit)
                    if not df.empty and len(df) >= min(limit, 100):
                        rows = self._df_to_ccxt(df)
                        log.info(f"[{symbol}] Served {len(rows)} klines from WS cache.")
                        return rows

            data = await self.spot_client.fetch_ohlcv(ccxt_symbol, timeframe=interval, limit=limit)
            log.info(f"[{symbol}] Successfully fetched {len(data)} klines via REST.")
            return data
        except Exception as e:
            log.error(f"[{symbol}] Failed to fetch klines: {e}")
            return None

    async def close(self):
        if self.cache is not None:
            try:
                await self.cache.stop()
            except Exception:
                pass
        await self.spot_client.close()
        await self.futures_client.close()

    def _calculate_rsi(self, series, period=14):
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs)).iloc[-1]

    def _calculate_rsi(self, series, period=14):
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs)).iloc[-1]
