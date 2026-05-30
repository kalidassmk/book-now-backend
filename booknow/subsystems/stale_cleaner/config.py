"""Tunables for the stale watchlist cleaner. All env-overridable."""

import os


def _f(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _i(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


# How often to scan the watchlist for stale rows. The sweep itself is
# cheap (one HGETALL + a pipeline of HDELs), so 15 s is fine.
POLL_INTERVAL_SEC = _f("STALE_CLEANER_POLL_SEC", 15.0)

# A symbol's WATCH_ALL row is "stale" if its `timestamp` field hasn't
# been refreshed in this many seconds. Default 30 min — comfortably
# longer than the WebSocket reconnect window so we never delete an
# active symbol just because the stream blipped for 60 s.
STALE_THRESHOLD_SEC = _i("STALE_CLEANER_THRESHOLD_SEC", 30 * 60)

# Local Redis (operational) — the same one the scanner writes to.
LOCAL_REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
LOCAL_REDIS_PORT = _i("REDIS_PORT", 6379)
LOCAL_REDIS_DB   = _i("REDIS_DB", 0)
