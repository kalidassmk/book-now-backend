"""
indicators.py
─────────────────────────────────────────────────────────────────────────────
Technical-analysis primitives ported from Indicators.java.

Two helpers:
    - rsi(closes, period=14)   Relative Strength Index, Wilder's smoothing
    - ema(closes, period)      Exponential Moving Average

Both accept a flat sequence of close prices (oldest → newest). They
return a single scalar — the latest indicator value — matching the
Java contract. Vectorised pandas/numpy implementations would be a
one-liner each, but we keep the explicit Wilder loop so unit tests
agree with the Java values bit-for-bit during the migration.
"""

from __future__ import annotations

from typing import Iterable, Sequence


def _to_floats(values: Iterable) -> list[float]:
    out: list[float] = []
    for v in values:
        try:
            out.append(float(v))
        except (TypeError, ValueError):
            continue
    return out


def rsi(closes: Sequence, period: int = 14) -> float:
    """Wilder-smoothed RSI on a sequence of close prices.

    Returns the canonical neutral 50.0 when the series is too short
    (matches Indicators.java line 20). Returns 100.0 on flat-up
    sequences (no losses) — also matches Java.
    """
    arr = _to_floats(closes)
    if len(arr) <= period:
        return 50.0

    last = arr[0]
    avg_gain = 0.0
    avg_loss = 0.0

    # 1) initial SMA of gains / losses across the first `period` deltas
    for i in range(1, period + 1):
        diff = arr[i] - last
        if diff >= 0:
            avg_gain += diff
        else:
            avg_loss += abs(diff)
        last = arr[i]
    avg_gain /= period
    avg_loss /= period

    # 2) Wilder's smoothing for the remaining bars
    for i in range(period + 1, len(arr)):
        diff = arr[i] - last
        gain = diff if diff >= 0 else 0.0
        loss = abs(diff) if diff < 0 else 0.0
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        last = arr[i]

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def ema(closes: Sequence, period: int) -> float:
    """Exponential Moving Average on a sequence of close prices.

    Returns the latest EMA value. When the series is shorter than
    ``period`` the function returns the latest close (matches Java
    line 67-69). When the series is empty, returns 0.0.
    """
    arr = _to_floats(closes)
    if not arr:
        return 0.0
    if len(arr) < period:
        return arr[-1]

    multiplier = 2.0 / (period + 1.0)

    # 1) seed with SMA of the first `period` closes
    seed = sum(arr[:period]) / period
    val = seed

    # 2) apply EMA recurrence for the remaining bars
    for i in range(period, len(arr)):
        val = (arr[i] - val) * multiplier + val
    return val
