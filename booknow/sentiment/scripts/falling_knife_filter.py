"""falling_knife_filter.py
─────────────────────────────────────────────────────────────────────────────
Pre-buy filter that rejects signals on coins likely to be a "falling knife"
(price has already pumped → bought near a peak → drops -3% to -8% after fill).

Derived empirically from the 2026-05-10 backtest of Option B (-0.65% offset,
+1% TP, no stop).  Twelve fills produced 5 winners and 4 deep losses.
The 4 deep losers (XEC×2, LUNC, LUMIA) all matched at least one of:

    1. 24h change   > +8%        (already pumped, top-heavy)
    2. 1h hi-lo range > 6%       (casino-mode volatility)
    3. 24h > 0% AND 60m > +1.5%  (overbought momentum)

Layering this filter on the same 12 signals would have skipped all 4 deep
losers without losing a single winner — net +4 / 0 trade.

Usage
─────

    >>> features = await compute_features(client, "XEC/USDT")
    >>> verdict  = evaluate(features, config)
    >>> if not verdict.passed:
    ...     log.info(verdict.reason)
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional

logger = logging.getLogger("booknow.falling_knife")


@dataclass
class MarketFeatures:
    """Pre-signal market snapshot for one symbol."""
    symbol: str
    price: float
    change_24h_pct: float
    high_24h: float
    low_24h: float
    quote_volume_24h: float

    # 1-hour candle aggregates
    high_1h: float
    low_1h: float
    range_1h_pct: float
    change_1h_pct: float

    # Distance markers
    from_24h_high_pct: float    # e.g. -5.9 = 5.9% below 24h peak
    from_1h_high_pct: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class FilterVerdict:
    """Result of running a MarketFeatures snapshot through the filter."""
    passed: bool
    rule: str           # which rule triggered (or 'pass')
    reason: str         # human-readable explanation
    features: MarketFeatures

    def to_dict(self) -> Dict[str, Any]:
        return {
            "passed": self.passed,
            "rule": self.rule,
            "reason": self.reason,
            "features": self.features.to_dict(),
        }


async def compute_features(client, symbol: str) -> Optional[MarketFeatures]:
    """Fetch a 24h ticker + 60×1m candles and compute features.

    Returns ``None`` if either call fails — caller should treat that as
    "no decision" and fall back to its default behaviour (we don't want a
    Binance hiccup to silently disable the filter).
    """
    try:
        ticker = await client.fetch_ticker(symbol)
    except Exception as exc:
        logger.debug("fetch_ticker(%s) failed: %s", symbol, exc)
        return None

    try:
        candles = await client.fetch_ohlcv(symbol, timeframe="1m", limit=60)
    except Exception as exc:
        logger.debug("fetch_ohlcv(%s) failed: %s", symbol, exc)
        return None

    if not candles or len(candles) < 10:
        return None

    last_price = float(ticker.get("last") or candles[-1][4] or 0)
    high_24h = float(ticker.get("high") or 0)
    low_24h = float(ticker.get("low") or 0)
    change_24h_pct = float(ticker.get("percentage") or 0)
    quote_volume = float(ticker.get("quoteVolume") or 0)

    highs = [float(c[2] or 0) for c in candles]
    lows = [float(c[3] or 0) for c in candles if float(c[3] or 0) > 0]
    if not highs or not lows:
        return None

    high_1h = max(highs)
    low_1h = min(lows)
    range_1h_pct = (high_1h - low_1h) / low_1h * 100 if low_1h else 0
    first_close = float(candles[0][4] or 0)
    change_1h_pct = (last_price - first_close) / first_close * 100 if first_close else 0
    from_24h_high_pct = (last_price - high_24h) / high_24h * 100 if high_24h else 0
    from_1h_high_pct = (last_price - high_1h) / high_1h * 100 if high_1h else 0

    return MarketFeatures(
        symbol=symbol,
        price=last_price,
        change_24h_pct=change_24h_pct,
        high_24h=high_24h,
        low_24h=low_24h,
        quote_volume_24h=quote_volume,
        high_1h=high_1h,
        low_1h=low_1h,
        range_1h_pct=range_1h_pct,
        change_1h_pct=change_1h_pct,
        from_24h_high_pct=from_24h_high_pct,
        from_1h_high_pct=from_1h_high_pct,
    )


def evaluate(features: MarketFeatures, *,
             enabled: bool = True,
             max_change_24h_pct: float = 8.0,
             max_range_1h_pct: float = 6.0,
             overbought_skip: bool = True,
             overbought_60m_pct: float = 1.5) -> FilterVerdict:
    """Apply the three falling-knife rules.

    Returns :class:`FilterVerdict` with ``passed=True`` if no rule fires.
    The ``rule`` field reports *which* rule blocked the signal (for metrics)
    or ``"pass"`` when everything checked out.
    """
    if not enabled:
        return FilterVerdict(True, "disabled", "filter disabled in config", features)

    if features.change_24h_pct > max_change_24h_pct:
        return FilterVerdict(
            False, "pump_24h",
            f"24h change {features.change_24h_pct:+.2f}% exceeds {max_change_24h_pct:.1f}%",
            features,
        )

    if features.range_1h_pct > max_range_1h_pct:
        return FilterVerdict(
            False, "volatile_1h",
            f"1h range {features.range_1h_pct:.2f}% exceeds {max_range_1h_pct:.1f}%",
            features,
        )

    if (overbought_skip and
            features.change_24h_pct > 0 and
            features.change_1h_pct > overbought_60m_pct):
        return FilterVerdict(
            False, "overbought",
            f"overbought (24h {features.change_24h_pct:+.2f}% AND "
            f"60m {features.change_1h_pct:+.2f}%)",
            features,
        )

    return FilterVerdict(True, "pass", "ok", features)
