"""
trend_alignment.py
─────────────────────────────────────────────────────────────────────────────
Phase-13 port of ``trend_alignment_engine/data_fetcher.py``.

Trend-alignment scans 6 timeframes simultaneously: 5m / 15m / 1h /
4h / 1d / 1w. ``fetch_multi_timeframe`` runs every interval in
parallel through ``asyncio.gather`` and returns a dict keyed by
interval (matching the legacy contract).
"""

from __future__ import annotations

import asyncio
from typing import Dict, Iterable, List, Optional, Tuple

from booknow.subsystems.base_fetcher import KlinesFetcher


class TrendAlignmentFetcher(KlinesFetcher):
    LIVE_INTERVALS: Tuple[str, ...] = ("5m", "15m", "1h", "4h", "1d", "1w")
    default_interval = "5m"
    default_limit = 100

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("log_name", "booknow.subsystems.trend_alignment")
        super().__init__(*args, **kwargs)

    async def fetch_timeframe(
        self,
        symbol: str,
        interval: str,
        limit: int = 100,
    ) -> Tuple[str, Optional[List[list]]]:
        """Single-tf fetch returned as ``(interval, klines_or_None)``.

        The tuple shape matches the legacy fetcher so call sites that
        gather multiple timeframes can unpack without changes.
        """
        data = await self.fetch_klines(symbol, interval=interval, limit=limit)
        return interval, data

    async def fetch_multi_timeframe(
        self,
        symbol: str,
        intervals: Optional[Iterable[str]] = None,
        limit: int = 100,
    ) -> Dict[str, List[list]]:
        """Fetch every interval in parallel.

        Intervals that fail to return data are dropped from the result,
        same as the legacy fetcher (so the caller can detect partial
        coverage by ``len(result) < len(intervals)``).
        """
        ivs = tuple(intervals) if intervals is not None else self.LIVE_INTERVALS
        results = await asyncio.gather(
            *(self.fetch_timeframe(symbol, iv, limit) for iv in ivs)
        )
        return {iv: data for iv, data in results if data}
