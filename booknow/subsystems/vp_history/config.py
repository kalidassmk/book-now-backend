"""Tunables for vp_history recorder. All overridable via env."""

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


# ── Loop ─────────────────────────────────────────────────────────────────
POLL_INTERVAL_SEC = _f("VP_HISTORY_POLL_SEC", 1.0)

# ── Compression ──────────────────────────────────────────────────────────
# When |new_vol_pct - last_vol_pct| < EPSILON, overwrite last entry instead
# of appending. Expressed as percentage points (0.10 == 0.10%).
VOL_PCT_EPSILON = _f("VP_HISTORY_VOL_EPSILON_PCT", 0.10)

# ── Retention ────────────────────────────────────────────────────────────
# Drop entries older than this many seconds. 4d horizon + safety buffer.
RETENTION_SEC = _i("VP_HISTORY_RETENTION_SEC", 5 * 24 * 3600)
# Hard per-symbol cap (defensive, prevents memory blow-up if epsilon misset).
MAX_POINTS_PER_SYMBOL = _i("VP_HISTORY_MAX_POINTS", 50_000)

# ── Local Redis (read side) ──────────────────────────────────────────────
LOCAL_REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
LOCAL_REDIS_PORT = _i("REDIS_PORT", 6379)
LOCAL_REDIS_DB   = _i("REDIS_DB", 0)
