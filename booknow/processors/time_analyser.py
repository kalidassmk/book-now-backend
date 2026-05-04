"""
time_analyser.py
─────────────────────────────────────────────────────────────────────────────
Records how long (in seconds) a coin takes to move between percentage
bands. Direct port of TimeAnalyser.java.

Storage layout (Redis hashes):

    ST0   field=0T1   value={"name":"0T1","shortestTimeList":[{"symbol":"X","timeTook":12.3}]}
    ST0   field=0T2   …
    ST0   field=0T3   …
    ST0   field=0T5   …
    ST0   field=0T7   …
    ST1   field=1T2   …
    ST1   field=1T3   …
    ST1   field=1T5   …
    ST1   field=1T7   …
    ST2   field=2T3   …
    ST2   field=2T5   …
    ST2   field=2T7   …
    ST3   field=3T5   …
    ST3   field=3T7   …

The Rule engines (Phase 11) poll these to detect "moved 0% → 5% in
under N seconds" patterns. Each (symbol, transition) is saved once
per process lifetime — duplicates would only re-save the same value.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional, Set, Tuple

import redis.asyncio as aioredis

from booknow.processors.base import AsyncProcessor
from booknow.repository import redis_keys


class TimeAnalyser(AsyncProcessor):
    name = "time_analyser"
    sleep_s = 1.0  # heavier loop than the others; matches Java cadence

    def __init__(self, redis_client: aioredis.Redis):
        super().__init__()
        self._redis = redis_client
        self._saved: Set[str] = set()  # guards: f"{symbol}::{store}::{label}"

    async def _tick(self) -> None:
        # Load every band watch-list once per iteration. Python's
        # asyncio.gather + Redis pipeline gives us six round-trips in one.
        keys = [
            self._wkey(redis_keys.BUCKET_G0L1),
            self._wkey(redis_keys.BUCKET_G1L2),
            self._wkey(redis_keys.BUCKET_G2L3),
            self._wkey(redis_keys.BUCKET_G3L5),
            self._wkey(redis_keys.BUCKET_G5L7),
            self._wkey(redis_keys.BUCKET_G7L10),
        ]
        async with self._redis.pipeline(transaction=False) as pipe:
            for k in keys:
                pipe.hgetall(k)
            raw = await pipe.execute()

        g0l1, g1l2, g2l3, g3l5, g5l7, g7l10 = (_parse_hash(h) for h in raw)

        for symbol, w0 in g0l1.items():
            t0 = _ts(w0)
            if t0 is None:
                continue

            # Transitions rooted at band 0 (>0<1)
            await self._save_if_present(symbol, t0, g1l2,  redis_keys.ST0, "0T1")
            await self._save_if_present(symbol, t0, g2l3,  redis_keys.ST0, "0T2")
            await self._save_if_present(symbol, t0, g3l5,  redis_keys.ST0, "0T3")
            await self._save_if_present(symbol, t0, g5l7,  redis_keys.ST0, "0T5")
            await self._save_if_present(symbol, t0, g7l10, redis_keys.ST0, "0T7")

            # Transitions rooted at band 1 (>1<2)
            t1 = _ts(g1l2.get(symbol))
            if t1 is not None:
                await self._save_if_present(symbol, t1, g2l3,  redis_keys.ST1, "1T2")
                await self._save_if_present(symbol, t1, g3l5,  redis_keys.ST1, "1T3")
                await self._save_if_present(symbol, t1, g5l7,  redis_keys.ST1, "1T5")
                await self._save_if_present(symbol, t1, g7l10, redis_keys.ST1, "1T7")

            # Transitions rooted at band 2 (>2<3)
            t2 = _ts(g2l3.get(symbol))
            if t2 is not None:
                await self._save_if_present(symbol, t2, g3l5,  redis_keys.ST2, "2T3")
                await self._save_if_present(symbol, t2, g5l7,  redis_keys.ST2, "2T5")
                await self._save_if_present(symbol, t2, g7l10, redis_keys.ST2, "2T7")

            # Transitions rooted at band 3 (>3<5)
            t3 = _ts(g3l5.get(symbol))
            if t3 is not None:
                await self._save_if_present(symbol, t3, g5l7,  redis_keys.ST3, "3T5")
                await self._save_if_present(symbol, t3, g7l10, redis_keys.ST3, "3T7")

    # ── Helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _wkey(bucket: str) -> str:
        return f"{redis_keys.WATCH_PREFIX}{bucket}{redis_keys.WATCH_SUFFIX}"

    async def _save_if_present(
        self,
        symbol: str,
        from_ts_ms: int,
        target_map: Dict[str, Dict[str, Any]],
        store_key: str,
        label: str,
    ) -> None:
        target = target_map.get(symbol)
        if target is None:
            return
        guard = f"{symbol}::{store_key}::{label}"
        if guard in self._saved:
            return
        to_ts_ms = _ts(target)
        if to_ts_ms is None:
            return
        seconds = (to_ts_ms - from_ts_ms) / 1000.0
        if seconds <= 0:  # clock skew guard
            return

        self._saved.add(guard)

        payload = {
            "name": label,
            "shortestTimeList": [
                {"symbol": symbol, "timeTook": round(seconds, 1)},
            ],
        }
        await self._redis.hset(store_key, label, json.dumps(payload))
        self.log.debug(
            "Transition %s %s → %s = %.1fs", symbol, store_key, label, seconds,
        )


# ── Module-level helpers (kept private) ────────────────────────────────────


def _parse_hash(raw: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for sym, val in raw.items():
        try:
            out[sym] = json.loads(val)
        except json.JSONDecodeError:
            continue
    return out


def _ts(watchlist: Optional[Dict[str, Any]]) -> Optional[int]:
    """Pull ``timestamp`` (epoch ms) from a WatchList entry."""
    if not watchlist:
        return None
    ts = watchlist.get("timestamp")
    try:
        return int(ts) if ts is not None else None
    except (TypeError, ValueError):
        return None
