"""
coin_analyzer.py
─────────────────────────────────────────────────────────────────────────────
Async port of CoinAnalyzer.java.

Fetches up to 250 daily candles via REST and produces a 0-to-7 ``buy_score``:

    +1 current price below 2-month average (buy the dip)
    +1 price in the lower 40 % of the 2-month range
    +1 7-day trend positive
    +1 30-day trend positive
    +1 24h volume > 1.5× 30-day average
    +1 24h volume > 7-day average
    +1 volume increased ≥3 of the last 7 days
    +1 EMA-50 > EMA-200 *and* RSI in (40, 70)  (Golden Cross)

    -2 7-day trend > +40 %             (overextended)
    -2 24h-vol / 30d-avg > 4×          (parabolic spike)
    -1 24h vol < $1 M USDT             (illiquid)

Final recommendation:
    score ≥ 5  → STRONG_BUY (should_buy=True)
    score == 4 → BUY        (should_buy=True)
    score == 3 → NEUTRAL    (should_buy=False)
    score == 2 → WAIT       (should_buy=False)
    else       → DONT_BUY   (should_buy=False)

Plus a hard parabolic safety override via TradingSafety.

Caching: results are kept for 1 hour per symbol so the 250-candle REST
call doesn't fire on every tick. The shared RateLimitGuard is honoured
on every fetch.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

import httpx

from booknow.analysis.indicators import ema, rsi
from booknow.analysis.safety import TradingSafety
from booknow.binance.rate_limit import get_default as _get_rate_limit_guard
from booknow.binance.rest_api import API_BASE, BinanceIpBannedException


logger = logging.getLogger("booknow.coin_analyzer")

CANDLE_LIMIT = 250
CACHE_TTL_S = 3600  # 1 hour, matches Java


# ── Result type ──────────────────────────────────────────────────────────


@dataclass
class CoinAnalysisResult:
    symbol: str
    current_price: float = 0.0
    buy_score: int = 0
    recommendation: str = "NEUTRAL"
    should_buy: bool = False
    reason: str = ""
    days_analyzed: int = 0

    high_2m: float = 0.0
    low_2m: float = 0.0
    avg_2m: float = 0.0
    price_position: float = 50.0      # % of 2m range (0=low, 100=high)

    trend_7d: float = 0.0
    trend_30d: float = 0.0

    vol_24h_usdt: float = 0.0
    vol_30d_avg_usdt: float = 0.0
    vol_7d_avg_usdt: float = 0.0
    volume_ratio: float = 1.0

    rsi: float = 50.0
    ema_50: float = 0.0
    ema_200: float = 0.0

    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "currentPrice": self.current_price,
            "buyScore": self.buy_score,
            "recommendation": self.recommendation,
            "shouldBuy": self.should_buy,
            "reason": self.reason,
            "daysAnalyzed": self.days_analyzed,
            "high2m": self.high_2m,
            "low2m": self.low_2m,
            "avg2m": self.avg_2m,
            "pricePosition": self.price_position,
            "trend7d": self.trend_7d,
            "trend30d": self.trend_30d,
            "vol24hUsdt": self.vol_24h_usdt,
            "vol30dAvgUsdt": self.vol_30d_avg_usdt,
            "vol7dAvgUsdt": self.vol_7d_avg_usdt,
            "volumeRatio": self.volume_ratio,
            "rsi": self.rsi,
            "ema50": self.ema_50,
            "ema200": self.ema_200,
        }


@dataclass
class _CachedResult:
    result: CoinAnalysisResult
    cached_at: float


# ── Analyzer ─────────────────────────────────────────────────────────────


class CoinAnalyzer:
    """Per-symbol scorer with a 1-hour in-memory cache."""

    def __init__(
        self,
        rest_client: Optional[httpx.AsyncClient] = None,
        safety: Optional[TradingSafety] = None,
    ):
        # If no client passed, build a private one (engine usually shares
        # the RestApiClient's underlying httpx via dependency injection,
        # but CoinAnalyzer is fine standalone for tests).
        self._owns_client = rest_client is None
        self._client = rest_client or httpx.AsyncClient(timeout=10.0, verify=False)
        self._guard = _get_rate_limit_guard()
        self._safety = safety or TradingSafety()
        self._cache: Dict[str, _CachedResult] = {}
        self._cache_lock = asyncio.Lock()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    # ── Public API ───────────────────────────────────────────────────────

    async def should_buy(self, symbol: str, current_price: float) -> bool:
        """Quick boolean gate used by TradeExecutor (returns True iff score ≥ 4)."""
        result = await self.analyze(symbol, current_price)
        return result.should_buy

    async def analyze(self, symbol: str, current_price: float) -> CoinAnalysisResult:
        """Score a symbol. Cached for 1 hour by symbol (price is updated on hit)."""
        # Cache hit?
        cached = self._cache.get(symbol)
        if cached and (time.time() - cached.cached_at) < CACHE_TTL_S:
            cached.result.current_price = current_price
            return cached.result

        result = CoinAnalysisResult(symbol=symbol, current_price=current_price)

        if self._guard.is_banned():
            result.reason = (
                f"Skipped — Binance ban active for {self._guard.ban_remaining_seconds()}s"
            )
            return result

        try:
            candles = await self._fetch_daily_klines(symbol, CANDLE_LIMIT)
        except BinanceIpBannedException as e:
            result.reason = f"Skipped — Binance ban: {e}"
            return result
        except Exception as e:
            if self._guard.report_if_banned(e):
                result.reason = "Skipped — Binance ban detected during fetch"
                return result
            logger.error("[Analyzer] error fetching %s: %s", symbol, e)
            result.reason = f"Analysis error: {e}"
            return result

        if not candles or len(candles) < 14:
            result.reason = "Insufficient history data"
            return result

        # ── Score ────────────────────────────────────────────────────
        result = self._score(candles, result)

        # Cache (under lock to avoid duplicate fetches racing)
        async with self._cache_lock:
            self._cache[symbol] = _CachedResult(result=result, cached_at=time.time())

        logger.info(
            "[Analyzer] %s score=%d/7 → %s | %s",
            symbol, result.buy_score, result.recommendation, result.reason,
        )
        return result

    # ── Scoring ──────────────────────────────────────────────────────────

    def _score(
        self,
        candles: List[List[Any]],
        result: CoinAnalysisResult,
    ) -> CoinAnalysisResult:
        """Compute the 0-7 score from raw candle rows.

        ``candles`` is Binance's klines schema: each row is
        ``[open_time, open, high, low, close, volume, close_time,
           quote_volume, trades, taker_buy_base, taker_buy_quote, ignore]``
        """
        # Normalise columns
        highs:   List[float] = [float(c[2]) for c in candles]
        lows:    List[float] = [float(c[3]) for c in candles]
        closes:  List[float] = [float(c[4]) for c in candles]
        quote_v: List[float] = [float(c[7]) for c in candles]
        n = len(candles)
        cur = result.current_price

        # ── Price stats ──────────────────────────────────────────────
        high_2m = max(highs)
        low_2m  = min(lows)
        avg_2m  = sum(closes) / n
        price_range = high_2m - low_2m
        price_pos = ((cur - low_2m) / price_range * 100.0) if price_range > 0 else 50.0

        result.high_2m = high_2m
        result.low_2m = low_2m
        result.avg_2m = avg_2m
        result.price_position = round(price_pos, 2)
        result.days_analyzed = n

        # ── Trend ────────────────────────────────────────────────────
        avg_last7  = _avg_slice(closes, n - 7,  n)
        avg_prev7  = _avg_slice(closes, n - 14, n - 7)
        avg_last30 = _avg_slice(closes, n - 30, n)
        avg_prev30 = _avg_slice(closes, max(0, n - 60), n - 30)

        trend_7d  = _pct_change(avg_prev7,  avg_last7)
        trend_30d = _pct_change(avg_prev30, avg_last30)
        result.trend_7d = round(trend_7d, 2)
        result.trend_30d = round(trend_30d, 2)

        # ── Volume ───────────────────────────────────────────────────
        vol_24h     = quote_v[-1]
        vol_30d_avg = sum(quote_v[-30:]) / min(30, n)
        vol_7d_avg  = sum(quote_v[-7:])  / min(7, n)
        volume_ratio = vol_24h / vol_30d_avg if vol_30d_avg > 0 else 1.0

        result.vol_24h_usdt     = round(vol_24h, 2)
        result.vol_30d_avg_usdt = round(vol_30d_avg, 2)
        result.vol_7d_avg_usdt  = round(vol_7d_avg, 2)
        result.volume_ratio     = round(volume_ratio, 2)

        # Volume-increasing-days count over the last 6 deltas
        vol_inc_days = 0
        for i in range(max(1, n - 6), n):
            if quote_v[i] > quote_v[i - 1]:
                vol_inc_days += 1

        # ── RSI / EMA ────────────────────────────────────────────────
        result.rsi = round(rsi(closes, 14), 2)
        result.ema_50 = round(ema(closes, 50), 2)
        result.ema_200 = round(ema(closes, 200), 2)

        # ── Safety override pre-computation ──────────────────────────
        is_safe = self._safety.is_safe_to_buy(
            result.symbol, cur, avg_2m, vol_24h, vol_30d_avg, result.rsi,
        )

        # ── Score table (mirrors CoinAnalyzer.java line-for-line) ────
        score = 0
        reasons: List[str] = []

        if cur < avg_2m:
            score += 1
            reasons.append("✅ Price below 2-month avg (buy the dip)")
        else:
            reasons.append("⚠️ Price above 2-month avg")

        if price_pos < 40.0:
            score += 1
            reasons.append("✅ Price in lower 40% of 2m range")
        elif price_pos > 80.0:
            reasons.append("🔴 Price near 2-month high (risky)")

        if trend_7d > 0:
            score += 1
            reasons.append(f"✅ 7d trend +{trend_7d:.2f}%")
        else:
            reasons.append(f"⚠️ 7d trend {trend_7d:.2f}%")

        if trend_30d > 0:
            score += 1
            reasons.append(f"✅ 30d trend +{trend_30d:.2f}%")
        else:
            reasons.append(f"⚠️ 30d trend {trend_30d:.2f}%")

        if volume_ratio >= 1.5:
            score += 1
            reasons.append(f"✅ Volume {volume_ratio:.1f}× above 30d avg")
        elif volume_ratio < 0.5:
            reasons.append("⚠️ Very low volume (avoid)")
        else:
            reasons.append(f"ℹ️ Volume ratio {volume_ratio:.2f} (normal)")

        # ── Risk penalties ───────────────────────────────────────────
        if trend_7d > 40.0:
            score -= 2
            reasons.append(f"🔴 RISK: overextended 7d (+{trend_7d:.1f}%)")
        if volume_ratio > 4.0:
            score -= 2
            reasons.append(f"🔴 RISK: parabolic volume spike ({volume_ratio:.1f}×)")
        if vol_24h < 1_000_000.0:
            score -= 1
            reasons.append("🔴 RISK: low liquidity (<1M USDT 24h)")

        # ── Volume confirmations ─────────────────────────────────────
        if vol_24h > vol_7d_avg:
            score += 1
            reasons.append("✅ 24h volume above 7d avg")
        if vol_inc_days >= 3:
            score += 1
            reasons.append(f"✅ Volume up {vol_inc_days}/6 days (building)")

        # ── Trend-following bonus (Golden Cross) ─────────────────────
        if result.ema_50 > result.ema_200 and 40 < result.rsi < 70:
            score += 1
            reasons.append("✅ TREND: Golden Cross (EMA50 > EMA200)")

        # ── Recommendation ──────────────────────────────────────────
        result.buy_score = score
        result.reason = " | ".join(reasons)

        if score >= 5:
            result.recommendation = "STRONG_BUY"
            result.should_buy = True
        elif score == 4:
            result.recommendation = "BUY"
            result.should_buy = True
        elif score == 3:
            result.recommendation = "NEUTRAL"
            result.should_buy = False
        elif score == 2:
            result.recommendation = "WAIT"
            result.should_buy = False
        else:
            result.recommendation = "DONT_BUY"
            result.should_buy = False

        # Hard safety override
        if not is_safe:
            result.should_buy = False
            result.recommendation = "RISKY_PARABOLIC"
            result.reason += " | ❌ BLOCKED: Parabolic Risk detected."

        return result

    # ── REST seed for daily klines (no WS equivalent for historical) ─

    async def _fetch_daily_klines(self, symbol: str, limit: int) -> List[List[Any]]:
        """One REST call to /api/v3/klines. Honour the rate-limit guard."""
        # Pace ourselves slightly so multiple analyze() calls don't hammer.
        await asyncio.sleep(0.05)
        url = f"{API_BASE}/api/v3/klines"
        r = await self._client.get(
            url,
            params={"symbol": symbol, "interval": "1d", "limit": limit},
        )
        if r.status_code in (418, 429):
            self._guard.record_ban_until(
                int(time.time() * 1000) + 120 * 1000,
                f"klines {symbol} returned HTTP {r.status_code}",
            )
            raise BinanceIpBannedException(
                r.status_code, 120, f"klines {symbol}: {r.text[:160]}",
            )
        if r.status_code >= 400:
            raise RuntimeError(f"klines {symbol} HTTP {r.status_code}: {r.text[:160]}")
        return r.json()


# ── Helpers ──────────────────────────────────────────────────────────────


def _avg_slice(xs: List[float], start: int, end: int) -> float:
    start = max(0, start)
    end = min(len(xs), end)
    if start >= end:
        return 0.0
    seg = xs[start:end]
    return sum(seg) / len(seg)


def _pct_change(frm: float, to: float) -> float:
    if frm == 0:
        return 0.0
    return (to - frm) / frm * 100.0
