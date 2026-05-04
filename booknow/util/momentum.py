"""
momentum.py
─────────────────────────────────────────────────────────────────────────────
Pure utility functions ported from BookNowUtility.java:

  - get_percentage(base, current)        — handle negative bases correctly
  - get_price(base, current)             — Decimal-safe price delta
  - get_bucket(increased_pct, fast_move) — map gain to a Redis bucket and
                                            update the FastMove counters
  - momentum_score_positive(delta)       — weighted positive momentum
  - momentum_score_negative(delta)       — weighted negative momentum
  - get_hms()                            — current HH:MM:SS for logs
  - DEFAULT_DELIST_SEED                  — known-bad coins (legacy seed)

These helpers stay decoupled from Redis / Binance / asyncio so they
can be unit-tested in isolation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Optional, Tuple

from booknow.repository import redis_keys


# ── FastMove model (Python equivalent of FastMove.java) ─────────────────────


@dataclass
class FastMove:
    """Per-symbol momentum counter mirroring the Java POJO.

    Field names match the Java getters/setters so the JSON serialization
    is wire-compatible with what the dashboard already reads from the
    ``FAST_MOVE`` Redis hash.
    """

    symbol: str = ""
    countG0L1: float = 0.0
    countG1L2: float = 0.0
    countG2L3: float = 0.0
    countG3L5: float = 0.0
    countG5L7: float = 0.0
    countG7L10: float = 0.0
    countG10: float = 0.0
    overAllCount: float = 0.0


# ── Percentage / price math ────────────────────────────────────────────────


def get_percentage(base_pct: float, current_pct: float) -> float:
    """How much current_pct has changed relative to base_pct.

    Mirrors the Java helper exactly. The negative-bases branch matches
    the Java BigDecimal arithmetic so two tracker implementations agree
    bit-for-bit during the migration cutover.
    """
    if base_pct < 0 and current_pct < 0 and base_pct < current_pct:
        # Both negative, current less negative ⇒ improvement.
        return float(abs(Decimal(str(base_pct)) - Decimal(str(current_pct))))
    return float(Decimal(str(current_pct)) - Decimal(str(base_pct)))


def get_price(base_price: Decimal, current_price: Decimal) -> Decimal:
    """Raw price delta with the same sign-handling as the Java helper."""
    if base_price < current_price and current_price < Decimal(0):
        return abs(base_price - current_price)
    return current_price - base_price


def to_decimal(value) -> Decimal:
    """Best-effort conversion to Decimal — accepts str / int / float / Decimal."""
    if isinstance(value, Decimal):
        return value
    if value is None:
        return Decimal(0)
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal(0)


# ── Bucket + FastMove update ───────────────────────────────────────────────


# Threshold table (lower-bound, upper-bound, bucket key, fast_move attr).
# Order is significant: first match wins.
_BUCKETS: Tuple[Tuple[float, float, str, str], ...] = (
    (0.30,  1.0,  redis_keys.BUCKET_G0L1,  "countG0L1"),
    (1.0,   2.0,  redis_keys.BUCKET_G1L2,  "countG1L2"),
    (2.0,   3.0,  redis_keys.BUCKET_G2L3,  "countG2L3"),
    (3.0,   5.0,  redis_keys.BUCKET_G3L5,  "countG3L5"),
    (5.0,   7.0,  redis_keys.BUCKET_G5L7,  "countG5L7"),
    (7.0,  10.0,  redis_keys.BUCKET_G7L10, "countG7L10"),
    (10.0, float("inf"), redis_keys.BUCKET_G10, "countG10"),
)


def get_bucket(
    increased_pct: float,
    fast_move: FastMove,
    momentum_score: float,
) -> Optional[str]:
    """Map a percentage gain to its bucket key + accumulate momentum.

    Returns the bucket name (e.g. ``">0<1"``) or ``None`` when the gain
    is below the 0.30% floor. Mutates the ``fast_move`` instance so the
    caller can persist the updated counters.
    """
    for lo, hi, key, attr in _BUCKETS:
        if lo <= increased_pct < hi:
            current = getattr(fast_move, attr)
            setattr(fast_move, attr, current + momentum_score)
            fast_move.overAllCount = fast_move.overAllCount + momentum_score
            return key
    return None


# ── Momentum scoring ───────────────────────────────────────────────────────


def momentum_score_positive(delta: float) -> float:
    """Weighted positive score for a tick-to-tick percentage gain."""
    if delta >= 3.0:    return 4.0
    if delta >= 2.0:    return 3.0
    if delta >= 1.5:    return 2.0
    if delta >= 1.0:    return 1.5
    if delta >= 0.75:   return 1.0
    if delta >= 0.50:   return 0.75
    if delta >= 0.30:   return 0.50
    if delta >= 0.10:   return 0.25
    if delta >= 0.05:   return 0.075
    if delta >= 0.01:   return 0.05
    return 0.0


def momentum_score_negative(delta: float) -> float:
    """Weighted negative penalty for a tick-to-tick drop."""
    if delta <= -3.0:   return -4.0
    if delta <= -2.0:   return -3.0
    if delta <= -1.5:   return -2.0
    if delta <= -1.0:   return -1.5
    if delta <= -0.75:  return -1.0
    if delta <= -0.50:  return -0.75
    if delta <= -0.30:  return -0.50
    if delta <= -0.09:  return -0.25
    if delta <= -0.05:  return -0.075
    if delta <= -0.01:  return -0.05
    return 0.0


def momentum_score(delta: float) -> float:
    """Combined helper: pick positive or negative scoring by sign."""
    return momentum_score_positive(delta) if delta >= 0 else momentum_score_negative(delta)


# ── Time helpers ───────────────────────────────────────────────────────────


def get_hms() -> str:
    """Current HH:MM:SS for log lines + Redis timestamps."""
    now = datetime.now()
    return f"{now.hour:02d}:{now.minute:02d}:{now.second:02d}"


# ── Delist seed (legacy) ───────────────────────────────────────────────────
#
# Static seed copied verbatim from BookNowUtility.deListCoins(). Most are
# historical — the live source of truth is BinanceDelistService writing
# BINANCE:DELIST:<symbol> = "true" (Phase 6 will port that). This seed
# is the safety net so even before Phase 6, BTCUSDT/ETHUSDT etc. don't
# get scalped.
DEFAULT_DELIST_SEED = frozenset({
    "MITHUSDT", "TRIBEBUSD", "REPUSDT", "BTCSTBUSD",
    "BUSDUSDT", "BTCUSDT", "ETHUSDT", "BTTCUSDT",
    "CHESSUSDT", "CVPUSDT", "MDTUSDT", "RAREUSDT",
    "DREPUSDT", "LOKAUSDT", "MOBUSDT", "TORNUSDT",
    "FETUSDT", "FORTHUSDT", "KEYUSDT", "MBOXUSDT",
    "WINUSDT", "AIONUSDT", "BTSUSDT", "GALAUSD",
    "ZILUSD", "VETUSD", "TKOUSDT", "ATAUSDT",
    "DEXEUSDT", "HIGHUSDT", "STPTUSDT", "WANUSDT",
    "HOOKUSDT", "RAYUSDT",
})
