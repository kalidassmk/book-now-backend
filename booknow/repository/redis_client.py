"""
redis_client.py
─────────────────────────────────────────────────────────────────────────────
Single async Redis client factory for the engine.

Every engine task that needs Redis goes through ``get_redis()``. This
gives us one connection pool process-wide instead of each module spinning
up its own. The pool is sized for ~50 concurrent ops which is plenty
for our trade frequency.

Decoded responses (``decode_responses=True``) so callers get ``str``
not ``bytes`` — matches what the dashboard's Node Redis client emits
into the same hashes, so JSON values round-trip cleanly between the
two languages.
"""

from __future__ import annotations

import logging
from typing import Optional

import redis.asyncio as aioredis

from booknow.config.settings import get_settings


logger = logging.getLogger("booknow.redis")

_client: Optional[aioredis.Redis] = None


def get_redis() -> aioredis.Redis:
    """Cached async Redis client for the whole process."""
    global _client
    if _client is None:
        s = get_settings()
        _client = aioredis.Redis(
            host=s.redis_host,
            port=s.redis_port,
            db=s.redis_db,
            decode_responses=True,
            max_connections=50,
            health_check_interval=30,
        )
        logger.info(
            "[redis] client created — %s:%d db=%d",
            s.redis_host, s.redis_port, s.redis_db,
        )
    return _client


async def close_redis() -> None:
    """Close the pooled connections. Called from main.py shutdown."""
    global _client
    if _client is not None:
        try:
            await _client.aclose()
        except Exception:
            pass
        _client = None
