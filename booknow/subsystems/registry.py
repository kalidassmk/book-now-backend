"""
registry.py
─────────────────────────────────────────────────────────────────────────────
Single owner of the resources every subsystem fetcher shares:

  * one :class:`KlinesCache` (one WS connection multiplexing every
    (symbol, interval) pair across the whole engine — sized to 600
    candles so the volume-profile fetcher reads purely in-memory),
  * one ``httpx.AsyncClient`` for the spot REST kline fallback.

Wire it in :func:`booknow.main._bootstrap` once and pass ``registry``
to anything that wants subsystem features.

The fetchers stay independent classes so subsystem code can hold a
reference to just the one it needs — no central god-object.
"""

from __future__ import annotations

import logging
from typing import Optional

import httpx

from booknow.binance.klines_cache import KlinesCache
from booknow.subsystems.fakeout_detector import FakeoutDetectorFetcher
from booknow.subsystems.meta_model import MetaModelFetcher
from booknow.subsystems.risk_management import RiskManagementFetcher
from booknow.subsystems.trend_alignment import TrendAlignmentFetcher
from booknow.subsystems.volume_profile import VolumeProfileFetcher


logger = logging.getLogger("booknow.subsystems.registry")


# Cover every interval any subsystem cares about. Cheap to leave them
# all on — KlinesCache only opens a stream for pairs we actually
# ``ensure()``. Largest is volume_profile's 500-candle ask, so we size
# the deque to 600 for headroom.
_LIVE_INTERVALS = ("1m", "5m", "15m", "1h", "4h", "1d", "1w")
_BUFFER_SIZE = 600


class SubsystemRegistry:
    """Boot, share, and tear down the fetcher fleet.

    Lifecycle:
        registry = SubsystemRegistry()
        await registry.start()
        # ... fetchers in use ...
        await registry.stop()
    """

    def __init__(
        self,
        cache: Optional[KlinesCache] = None,
        http_client: Optional[httpx.AsyncClient] = None,
    ):
        self._cache_owned = cache is None
        self._http_owned  = http_client is None

        self.cache: KlinesCache = cache or KlinesCache(
            intervals=_LIVE_INTERVALS, buffer_size=_BUFFER_SIZE,
        )
        self._http: httpx.AsyncClient = http_client or httpx.AsyncClient(
            timeout=10.0,
            verify=False,
            headers={"User-Agent": "booknow-engine/0.1"},
        )

        # All fetchers share the cache + http client. They take
        # ownership=False so their close() is a no-op.
        self.risk_management = RiskManagementFetcher(self.cache, http_client=self._http)
        self.fakeout_detector = FakeoutDetectorFetcher(self.cache, http_client=self._http)
        self.volume_profile  = VolumeProfileFetcher(self.cache, http_client=self._http)
        self.trend_alignment = TrendAlignmentFetcher(self.cache, http_client=self._http)
        self.meta_model      = MetaModelFetcher(self.cache, http_client=self._http)

    async def start(self) -> None:
        """Open the shared WS. Idempotent."""
        if self._cache_owned:
            await self.cache.start()
            logger.info(
                "[subsystems] registry started — KlinesCache live (intervals=%s, buffer=%d)",
                ",".join(_LIVE_INTERVALS), _BUFFER_SIZE,
            )

    async def stop(self) -> None:
        """Tear down anything we own. Resources passed in are left alone."""
        if self._cache_owned:
            try:
                await self.cache.stop()
            except Exception as e:
                logger.warning("[subsystems] cache stop error: %s", e)
        if self._http_owned:
            try:
                await self._http.aclose()
            except Exception:
                pass
        logger.info("[subsystems] registry stopped")
