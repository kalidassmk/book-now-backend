"""
trade_archive
─────────────────────────────────────────────────────────────────────────────
Shared helper for archiving closed trades to the analyse Redis.

Both scalpers (Virtual + Fast) call ``archive_closed_trade(...)`` from
their close path. Trades land in:

    TRADE_HISTORY:{date}:{kind}:{symbol}:{ts_ms}    (HASH per trade)
    TRADE_HISTORY:INDEX:{date}                      (LIST of those keys, newest first)

  ``date`` is the closing date in YYYY-MM-DD (UTC).
  ``kind`` is "VIRTUAL" or "FAST" so dashboards can split simulated
  from live without inspecting fields.

The helper is best-effort: if the analyse Redis is unreachable we log
a warning and return, never block the scalper's hot path. The
operational scalper history (VIRTUAL_HISTORY_KEY, etc.) stays where
it is — the archive is additive.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Mapping, Optional

import redis


logger = logging.getLogger(__name__)


_ARCHIVE_PREFIX = "TRADE_HISTORY"
_INDEX_TTL_SEC = 90 * 24 * 3600  # 90-day index retention; trade hashes themselves can be longer-lived


def _get_analyse_redis() -> redis.Redis:
    """Return a client for the analyse Redis (env-driven, on-EC2 default)."""
    return redis.Redis(
        host=os.getenv("REDIS_ANALYSE_HOST", "redis-analyse"),
        port=int(os.getenv("REDIS_ANALYSE_PORT", "6379")),
        password=os.getenv("REDIS_ANALYSE_PASS") or None,
        decode_responses=True,
        socket_keepalive=True,
        socket_connect_timeout=3,
        socket_timeout=3,
    )


_client: Optional[redis.Redis] = None


def _client_or_none() -> Optional[redis.Redis]:
    """Lazily create + cache a single connection. None if connect fails."""
    global _client
    if _client is None:
        try:
            c = _get_analyse_redis()
            c.ping()
            _client = c
        except Exception as e:
            logger.debug("trade_archive: analyse redis not reachable yet: %s", e)
            return None
    return _client


def archive_closed_trade(symbol: str, kind: str, record: Mapping) -> bool:
    """Write a closed-trade record to the analyse Redis.

    Returns True on success, False if the analyse Redis is not reachable
    (caller treats that as a soft failure — archive is best-effort).

    ``kind`` should be "VIRTUAL" or "FAST".
    ``record`` is a dict with whatever exit/PnL/fee fields the caller
    wants preserved; it gets serialized to JSON and stored alongside
    the metadata fields below.
    """
    c = _client_or_none()
    if c is None:
        return False

    ts_ms = int(time.time() * 1000)
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"{_ARCHIVE_PREFIX}:{date_str}:{kind}:{symbol}:{ts_ms}"
    index_key = f"{_ARCHIVE_PREFIX}:INDEX:{date_str}"

    try:
        # Store the record as a single JSON blob plus a few flat fields
        # so it's queryable without parsing if the dashboard wants
        # a quick scan.
        payload = {
            "symbol": symbol,
            "kind": kind,
            "closed_at_ms": ts_ms,
            "closed_date": date_str,
            "record": json.dumps(record, default=str),
        }
        for flat in ("entry_price", "exit_price", "qty", "investment",
                     "pnl_usdt", "pnl_pct", "fees_paid", "reason"):
            if flat in record and record[flat] is not None:
                payload[flat] = str(record[flat])

        pipe = c.pipeline(transaction=False)
        pipe.hset(key, mapping=payload)
        pipe.lpush(index_key, key)
        pipe.expire(index_key, _INDEX_TTL_SEC)
        pipe.execute()
        return True
    except Exception as e:
        # Best-effort — don't let archiving failures break the scalper.
        logger.warning("trade_archive: write failed for %s (%s): %s", symbol, kind, e)
        return False
