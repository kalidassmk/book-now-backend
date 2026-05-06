"""
meta_model.py
─────────────────────────────────────────────────────────────────────────────
Phase-13 port of ``meta_model_system/data_collector.py``.

The meta-model wants a single feature dict per ``(symbol, tick)`` of
spot kline-derived TA features (RSI, volatility, volume spike). This
version reads spot klines from the shared :class:`KlinesCache` (one
shared WS connection across the whole engine) and computes TA
features with pandas if it's available, falling back to safe defaults
if it isn't.

Funding-rate and open-interest features were removed when the engine
moved to spot-only trading (those signals come from Binance Futures).

Output dict:

    {
        "symbol": str,
        "price": float,
        "rsi": float,
        "price_change_5m": float,
        "volatility": float,
        "volume_spike": float,
    }
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

try:
    import pandas as pd
    _HAS_PANDAS = True
except ImportError:  # pragma: no cover
    pd = None  # type: ignore
    _HAS_PANDAS = False

from booknow.subsystems.base_fetcher import KlinesFetcher


logger = logging.getLogger("booknow.subsystems.meta_model")


class MetaModelFetcher(KlinesFetcher):
    """Spot-only feature collector for the meta-model.

    Inherits the cache-first kline path from :class:`KlinesFetcher`.
    """

    default_interval = "5m"
    default_limit = 100

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("log_name", "booknow.subsystems.meta_model")
        super().__init__(*args, **kwargs)

    async def fetch_all_features(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Build one feature row for ``symbol``. ``None`` if klines fail."""
        api_symbol = self._normalise_symbol(symbol)

        klines = await self.fetch_klines(
            api_symbol, interval=self.default_interval, limit=self.default_limit,
        )
        if not klines:
            return None

        try:
            current_price = float(klines[-1][4])
        except (IndexError, TypeError, ValueError):
            return None

        rsi, volatility, volume_spike = self._ta_features(klines)

        prev_close = float(klines[-2][4]) if len(klines) > 1 else current_price
        price_change = (current_price - prev_close) / prev_close if prev_close else 0.0

        return {
            "symbol": api_symbol,
            "price": current_price,
            "rsi": rsi,
            "price_change_5m": price_change,
            "volatility": volatility,
            "volume_spike": volume_spike,
        }

    async def fetch_historical_klines(
        self,
        symbol: str,
        interval: str = "5m",
        limit: int = 1000,
    ) -> Optional[List[list]]:
        """Larger pull used by the meta-model training pipeline.

        The cache caps each (symbol, interval) deque at ~600 rows so
        anything bigger goes straight to REST. Mirrors the legacy
        collector's ``fetch_historical_klines``.
        """
        if limit <= self._cache.buffer_size:
            return await self.fetch_klines(symbol, interval=interval, limit=limit)
        api_symbol = self._normalise_symbol(symbol)
        return await self._fetch_rest(api_symbol, interval, limit)

    # ── TA helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _ta_features(klines: List[list]) -> tuple:
        """Compute (rsi, volatility, volume_spike). Safe defaults if pandas
        isn't installed or there isn't enough history."""
        if not _HAS_PANDAS or len(klines) < 21:
            return 50.0, 0.0, 1.0
        try:
            df = pd.DataFrame(klines, columns=["t", "o", "h", "l", "c", "v"])
            df[["o", "h", "l", "c", "v"]] = df[["o", "h", "l", "c", "v"]].astype(float)
            rsi = MetaModelFetcher._rsi(df["c"])
            volatility = float(df["h"].iloc[-20:].max() - df["l"].iloc[-20:].min())
            base = float(df["v"].iloc[-20:-1].mean()) if len(df) >= 21 else 0.0
            volume_spike = float(df["v"].iloc[-1] / base) if base > 0 else 1.0
            return rsi, volatility, volume_spike
        except Exception as e:
            logger.debug("TA feature calc failed: %s", e)
            return 50.0, 0.0, 1.0

    @staticmethod
    def _rsi(series, period: int = 14) -> float:
        """Wilder-style RSI on a closing-price series."""
        try:
            delta = series.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            rs = gain / loss
            value = 100 - (100 / (1 + rs))
            last = float(value.iloc[-1])
            if last != last:  # NaN
                return 50.0
            return last
        except Exception:
            return 50.0
