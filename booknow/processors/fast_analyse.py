"""
fast_analyse.py
─────────────────────────────────────────────────────────────────────────────
Speed-based signal classifier. Direct port of FastAnalyse.java.

For coins already in the 2-7% zone, this checks whether they skipped
lower bands entirely (signalling unusually fast momentum):

  SUPER_FAST>2<3        at 2-3% but never appeared in >0<1 or >1<2
  ULTRA_FAST>3<5        at 3-5% but never appeared in >1<2 or >2<3
  ULTRA_SUPER_FAST>5<7  at 5-7% but never appeared in >2<3 or >3<5

Each detection is recorded once per process lifetime — re-saves are
idempotent.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Set

import redis.asyncio as aioredis

from booknow.processors.base import AsyncProcessor
from booknow.repository import redis_keys


class FastAnalyse(AsyncProcessor):
    name = "fast_analyse"
    sleep_s = 0.5

    def __init__(self, redis_client: aioredis.Redis):
        super().__init__()
        self._redis = redis_client
        self._recorded_sf: Set[str] = set()
        self._recorded_uf: Set[str] = set()
        self._recorded_usf: Set[str] = set()

    async def _tick(self) -> None:
        # SUPER_FAST: at >2<3 but skipped >0<1 and >1<2
        symbols_in_2_3 = await self._symbols_in(redis_keys.BUCKET_G2L3)
        for symbol in symbols_in_2_3:
            if symbol in self._recorded_sf:
                continue
            if await self._is_super_fast(symbol):
                self._recorded_sf.add(symbol)
                payload = await self._get_percentage(redis_keys.BUCKET_G2L3, symbol)
                if payload:
                    await self._save_percentage(redis_keys.SUPER_FAST_2_3, symbol, payload)
                    self.log.info("SUPER_FAST>2<3: %s (skipped 0-1 and 1-2 bands)", symbol)

        # ULTRA_FAST: at >3<5 but skipped >1<2 and >2<3
        symbols_in_3_5 = await self._symbols_in(redis_keys.BUCKET_G3L5)
        for symbol in symbols_in_3_5:
            if symbol in self._recorded_uf:
                continue
            if await self._is_ultra_fast(symbol):
                self._recorded_uf.add(symbol)
                payload = await self._get_percentage(redis_keys.BUCKET_G3L5, symbol)
                if payload:
                    await self._save_percentage(redis_keys.ULTRA_FAST_3_5, symbol, payload)
                    self.log.info("ULTRA_FAST>3<5: %s (skipped 1-2 and 2-3 bands)", symbol)

        # ULTRA_SUPER_FAST: at >5<7 but skipped >2<3 and >3<5
        symbols_in_5_7 = await self._symbols_in(redis_keys.BUCKET_G5L7)
        for symbol in symbols_in_5_7:
            if symbol in self._recorded_usf:
                continue
            if await self._is_ultra_super_fast(symbol):
                self._recorded_usf.add(symbol)
                payload = await self._get_percentage(redis_keys.BUCKET_G5L7, symbol)
                if payload:
                    await self._save_percentage(redis_keys.ULTRA_SUPER_FAST_5_7, symbol, payload)
                    self.log.info("ULTRA_SUPER_FAST>5<7: %s (skipped 2-3 and 3-5 bands)", symbol)

    # ── Speed checks ─────────────────────────────────────────────────────

    async def _is_super_fast(self, symbol: str) -> bool:
        """Symbol is at >2<3 with NO entry in >0<1 OR >1<2."""
        in_g0l1, in_g1l2 = await self._exists_in(symbol, redis_keys.BUCKET_G0L1, redis_keys.BUCKET_G1L2)
        return not in_g0l1 and not in_g1l2

    async def _is_ultra_fast(self, symbol: str) -> bool:
        """Symbol is at >3<5 with NO entry in >1<2 OR >2<3."""
        in_g1l2, in_g2l3 = await self._exists_in(symbol, redis_keys.BUCKET_G1L2, redis_keys.BUCKET_G2L3)
        return not in_g1l2 and not in_g2l3

    async def _is_ultra_super_fast(self, symbol: str) -> bool:
        """Symbol is at >5<7 with NO entry in >2<3 OR >3<5."""
        in_g2l3, in_g3l5 = await self._exists_in(symbol, redis_keys.BUCKET_G2L3, redis_keys.BUCKET_G3L5)
        return not in_g2l3 and not in_g3l5

    # ── Helpers ──────────────────────────────────────────────────────────

    async def _symbols_in(self, hash_key: str) -> list[str]:
        return list(await self._redis.hkeys(hash_key))

    async def _exists_in(self, symbol: str, *hash_keys: str) -> tuple[bool, ...]:
        async with self._redis.pipeline(transaction=False) as pipe:
            for hk in hash_keys:
                pipe.hexists(hk, symbol)
            results = await pipe.execute()
        return tuple(bool(r) for r in results)

    async def _get_percentage(self, hash_key: str, symbol: str) -> Dict[str, Any] | None:
        raw = await self._redis.hget(hash_key, symbol)
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None

    async def _save_percentage(self, hash_key: str, symbol: str, payload: Dict[str, Any]) -> None:
        await self._redis.hset(hash_key, symbol, json.dumps(payload))
