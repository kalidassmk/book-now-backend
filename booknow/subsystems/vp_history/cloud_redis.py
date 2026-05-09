"""AnalyseDB Redis connection helper.

Same target as the analyse-side scripts (pattern_matching_engine,
success_pattern_recorder, init_analyse_db). Defaults to the on-EC2
``redis-analyse`` container created in compose; env vars override.

Module name kept as ``cloud_redis`` for backwards compatibility, but
the data lives on a colocated EC2 container now, not Redis Cloud.
"""

from __future__ import annotations

import os

import redis


def get_cloud_redis() -> redis.Redis:
    """Return a client for the analyse-side Redis (env-driven, on-EC2 default)."""
    return redis.Redis(
        host=os.getenv("REDIS_ANALYSE_HOST", os.getenv("REDIS_CLOUD_HOST", "redis-analyse")),
        port=int(os.getenv("REDIS_ANALYSE_PORT", os.getenv("REDIS_CLOUD_PORT", "6379"))),
        password=os.getenv("REDIS_ANALYSE_PASS") or os.getenv("REDIS_CLOUD_PASS") or None,
        decode_responses=True,
        socket_keepalive=True,
        socket_connect_timeout=5,
        socket_timeout=5,
        health_check_interval=30,
    )
