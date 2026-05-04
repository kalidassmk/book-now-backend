"""
ulf_0_to_3.py
─────────────────────────────────────────────────────────────────────────────
Ultra-Low-Frequency 0→3% detector. Direct port of ULF0To3.java.

Reads three percentage buckets (>0<1, >1<2, >2<3) every 500 ms and
writes four classification keys when the patterns match:

  LT2MIN_0>3        coin climbed 0% → 3% with each step in <2 minutes
  ULTRA_FAST0>2     symbol is at 1<2 but never appeared in >0<1
  ULTRA_FAST2>3     symbol is at 2<3 but never appeared in >1<2
  ULTRA_FAST0>3     symbol is at 2<3 but never appeared in >0<1

Each detection is recorded once per process lifetime (in-memory set
guard). Re-saving the same Redis field with the same Percentage row
is idempotent so a duplicate detection on restart is harmless.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Set

import redis.asyncio as aioredis

from booknow.processors.base import AsyncProcessor
from booknow.repository import redis_keys


# Two minutes in milliseconds — gap limit between consecutive band crossings.
_MAX_GAP_MS = 2 * 60 * 1000


def _last_timestamp(percentage: Dict[str, Any]) -> int:
    """Pull the most recent timestamp from a Percentage entry.

    Mirrors Java's ``lastTimestamp(Percentage)``: prefers the last
    ``previousCountList`` entry, falls back to ``currentTimeStamp``.
    """
    history = percentage.get("previousCountList") or []
    if history:
        last = history[-1] or {}
        ts = last.get("timestamp")
        if ts is not None:
            try:
                return int(ts)
            except (TypeError, ValueError):
                pass
    cur_ts = percentage.get("currentTimeStamp")
    try:
        return int(cur_ts) if cur_ts is not None else 0
    except (TypeError, ValueError):
        return 0


class UlfZeroToThree(AsyncProcessor):
    name = "ulf_0_to_3"
    sleep_s = 0.5

    def __init__(self, redis_client: aioredis.Redis):
        super().__init__()
        self._redis = redis_client
        self._recorded_lt2: Set[str] = set()
        self._recorded_uf_0_to_2: Set[str] = set()
        self._recorded_uf_2_to_3: Set[str] = set()
        self._recorded_uf_0_to_3: Set[str] = set()

    async def _tick(self) -> None:
        g0l1, g1l2, g2l3 = await self._load_buckets()

        # Pattern 1: full 0→1→2→3, each step under 2 minutes.
        for symbol, p1 in g0l1.items():
            p2 = g1l2.get(symbol)
            p3 = g2l3.get(symbol)
            if p2 is None or p3 is None:
                continue

            t1 = _last_timestamp(p1)
            t2 = _last_timestamp(p2)
            t3 = _last_timestamp(p3)

            step1_fast = (t2 - t1) <= _MAX_GAP_MS
            step2_fast = (t3 - t2) <= _MAX_GAP_MS

            if step1_fast and step2_fast and symbol not in self._recorded_lt2:
                self._recorded_lt2.add(symbol)
                await self._save_percentage(redis_keys.LT2MIN_0_TO_3, symbol, p3)
                self.log.info(
                    "LT2MIN_0>3 detected: %s (%d → %d → %d ms)", symbol, t1, t2, t3,
                )

        # Pattern 2: at >1<2 but never seen at >0<1.
        for symbol, p in g1l2.items():
            if symbol not in g0l1 and symbol not in self._recorded_uf_0_to_2:
                self._recorded_uf_0_to_2.add(symbol)
                await self._save_percentage(redis_keys.ULTRA_FAST_0_TO_2, symbol, p)
                self.log.info("ULTRA_FAST0>2 detected: %s", symbol)

        # Pattern 3: at >2<3 but never seen at >1<2.
        for symbol, p in g2l3.items():
            if symbol not in g1l2 and symbol not in self._recorded_uf_2_to_3:
                self._recorded_uf_2_to_3.add(symbol)
                await self._save_percentage(redis_keys.ULTRA_FAST_2_TO_3, symbol, p)
                self.log.info("ULTRA_FAST2>3 detected: %s", symbol)

        # Pattern 4: at >2<3 but never seen at >0<1.
        for symbol, p in g2l3.items():
            if symbol not in g0l1 and symbol not in self._recorded_uf_0_to_3:
                self._recorded_uf_0_to_3.add(symbol)
                await self._save_percentage(redis_keys.ULTRA_FAST_0_TO_3, symbol, p)
                self.log.info("ULTRA_FAST0>3 detected: %s", symbol)

    # ── Helpers ──────────────────────────────────────────────────────────

    async def _load_buckets(self):
        # One pipeline per tick = three round-trips to Redis. Cheap.
        async with self._redis.pipeline(transaction=False) as pipe:
            pipe.hgetall(redis_keys.BUCKET_G0L1)
            pipe.hgetall(redis_keys.BUCKET_G1L2)
            pipe.hgetall(redis_keys.BUCKET_G2L3)
            raw = await pipe.execute()
        return tuple(_parse_hash(h) for h in raw)

    async def _save_percentage(self, hash_key: str, symbol: str, payload: Dict[str, Any]) -> None:
        await self._redis.hset(hash_key, symbol, json.dumps(payload))


def _parse_hash(raw: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for sym, val in raw.items():
        try:
            out[sym] = json.loads(val)
        except json.JSONDecodeError:
            continue
    return out
