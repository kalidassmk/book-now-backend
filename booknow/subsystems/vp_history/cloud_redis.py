"""Redis Cloud connection helper.

Mirrors the credential pattern already in
``booknow/sentiment/scripts/init_analyse_db.py`` — same host the user
designated for AnalyseDB. Falls back to the hardcoded values when env
overrides are absent so this module works whether or not the operator
has rolled the password into ``.env``.
"""

from __future__ import annotations

import os

import redis


# Defaults match init_analyse_db.py exactly so a fresh checkout works
# without extra env wiring; production should override via .env.
_DEFAULT_HOST = "redis-18144.c89.us-east-1-3.ec2.cloud.redislabs.com"
_DEFAULT_PORT = 18144
_DEFAULT_PASS = "Gn9jKtL0SBkMLYynSjXbblmkjkIGrdPS"


def get_cloud_redis() -> redis.Redis:
    return redis.Redis(
        host=os.getenv("REDIS_CLOUD_HOST", _DEFAULT_HOST),
        port=int(os.getenv("REDIS_CLOUD_PORT", _DEFAULT_PORT)),
        password=os.getenv("REDIS_CLOUD_PASS", _DEFAULT_PASS),
        decode_responses=True,
        socket_keepalive=True,
        socket_connect_timeout=5,
        socket_timeout=5,
        health_check_interval=30,
    )
