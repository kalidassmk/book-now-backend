"""
fast_move_filter.py
─────────────────────────────────────────────────────────────────────────────
Top-N momentum publisher. Direct port of FastMoveFilter.java.

Every 500 ms, scans the FAST_MOVE Redis hash, picks the five symbols
with the highest ``overAllCount`` and republishes them under FM-5
(constant ``FAST_MOVE_TOP5``). Used by dashboards and consensus
agents that want the "hottest right now" view without scanning the
full set.

Atomicity: the Java version did delete + N saves; we use a Redis
transaction (MULTI/EXEC) so dashboards never see an empty FM-5 mid-
update.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple

import redis.asyncio as aioredis

from booknow.processors.base import AsyncProcessor
from booknow.repository import redis_keys


class FastMoveFilter(AsyncProcessor):
    name = "fast_move_filter"
    sleep_s = 0.5
    top_n = 5

    def __init__(self, redis_client: aioredis.Redis, top_n: int = 5):
        super().__init__()
        self._redis = redis_client
        self.top_n = top_n

    async def _tick(self) -> None:
        raw = await self._redis.hgetall(redis_keys.FAST_MOVE)
        if not raw:
            return

        ranked = self._rank(raw)
        if not ranked:
            return

        # Atomic swap: replace the entire FM-5 hash inside one transaction.
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.delete(redis_keys.FAST_MOVE_TOP5)
            for sym, payload_json in ranked:
                pipe.hset(redis_keys.FAST_MOVE_TOP5, sym, payload_json)
            await pipe.execute()

        self.log.debug("FM-5 updated: %s", [sym for sym, _ in ranked])

    def _rank(self, raw: Dict[str, str]) -> List[Tuple[str, str]]:
        """Sort by overAllCount desc, return top-N as (symbol, json_str)."""
        scored: List[Tuple[float, str, str]] = []
        for sym, value in raw.items():
            try:
                obj = json.loads(value)
            except json.JSONDecodeError:
                continue
            try:
                score = float(obj.get("overAllCount") or 0)
            except (TypeError, ValueError):
                score = 0.0
            scored.append((score, sym, value))

        # Sort descending. Stable + deterministic for equal scores.
        scored.sort(key=lambda t: t[0], reverse=True)
        return [(sym, value) for _score, sym, value in scored[: self.top_n]]
