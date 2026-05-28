#!/usr/bin/env python3
"""
low_mcap_explosive.py — iter 70 (2026-05-24)
─────────────────────────────────────────────────────────────────────────────
Low Market Cap + High Volume = Explosive Move Chance (LMC) detector.

Thesis:
  Small-cap coins (low 7-day avg quote volume) suddenly getting big
  volume = explosive-move chance.  Thinly-capitalised assets move much
  more easily than majors — a $1M buy on a $500K/day coin can pump it
  20% in minutes.

We don't have direct market cap data from Binance.  Instead we use
7-day average 24h quote volume as a robust MCAP proxy:
  • Small caps trade $100K–$5M/day
  • Mid caps trade $5M–$50M/day
  • Large caps trade $50M+/day

Pipeline per poll cycle (every 30s):
  1. Fetch /api/v3/ticker/24hr ONCE (returns all symbols)
  2. For each USDT pair within liquidity bracket
     [lmcMinVol24h, lmcMaxAvgVol7d]:
       a. Fetch 7-day daily klines (cached 1h)
       b. Check volume surge today vs 7d avg
       c. Check trade-count surge, % move, etc.
  3. Compute explosive score 0-100.
  4. Classify direction (PUMP/DUMP/NEUTRAL) via hourly candles + VWAP.
  5. Paper mode: record would-be buy to LMC:PAPER_TRADES:<date>.
  6. Live mode: EXPLOSIVE_PUMP + score >= lmcLiveScore → delegate buy.

Redis output:
  LMC:DETECTIONS:<date>   — RPUSH per fire
  LMC:LATEST              — HSET symbol → latest event
  LMC:PAPER_TRADES:<date> — would-be buys
  LMC:OUTCOMES:<date>     — actual price chg at +15/+60/+240/+1440 min
  LMC:COOLDOWN:<sym>      — per-symbol re-fire cooldown
"""
from __future__ import annotations

import json
import logging
import os
import statistics
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import redis
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("LMC")

CONFIG_KEY = "TRADING_CONFIG"
DEFAULTS: Dict[str, Any] = {
    "lmcEnabled": False,
    "lmcPaperMode": True,
    "lmcPaperModeEndDate": "2026-05-31",
    "lmcPollIntervalSec": 30,
    "lmcCooldownSec": 1800,                # 30min per-symbol

    # Universe brackets (in USD)
    "lmcMinVol24h": 2_000_000,             # iter 75 — bumped 50K→2M (false-signal noise)
    "lmcMaxAvgVol7d": 10_000_000,          # above = not small cap

    # Entry triggers
    "lmcMinVolSurge": 3.0,                 # today_vol / 7d_avg >= 3x
    "lmcMinTradeSurge": 3.0,
    "lmcMinAbsChg24h": 2.0,                # |chg_24h| >= 2%

    # iter 92 — Fast-path 5m vol-surge gate (catches sharp dumps the
    # rolling 24h gate takes 2-3 min to see).
    "lmcMin5mVolSurge": 3.0,               # last 5m avg vol/min vs prior 60m
    "lmcMin5mAbsChg": 0.8,                 # |last 5m price chg| >= 0.8%
    "lmcFastPrefilterChg24h": 1.0,         # skip 1m-kline fetch if |chg_24h|<1%
    "lmcWeight5mSurge": 20.0,              # bonus points when 5m surge fires

    # iter 93 — Flash-dump path: compares ticker between 10s polls and
    # fires on sub-30s price drops. Latency 10-30s from dump start.
    "lmcFlashMinDropPct": 0.5,             # |price chg in window| >= 0.5%
    "lmcFlashMinVolDeltaUsd": 50_000.0,    # vol traded in window (sanity)
    "lmcFlashCooldownSec": 300,            # 5 min re-fire lockout

    # Scoring weights
    "lmcWeightMcap": 25,
    "lmcWeightVolSurge": 25,
    "lmcWeightTradeSurge": 15,
    "lmcWeightTodayMove": 20,
    "lmcWeightPrePumpQuiet": 10,
    "lmcWeightLiquiditySanity": 5,

    # Classification
    "lmcExplosiveScore": 75,
    "lmcWatchScore": 50,

    # Live-mode buy
    "lmcLiveScore": 75,
    "lmcSellPctLabel": 5.0,
    "lmcOutcomeCheckMinutes": [15, 60, 240, 1440],
}

POLL_INTERVAL_S = float(os.getenv("LMC_POLL_S", "10"))   # iter 93 — was 30, now 10s

# iter 93 — per-symbol ticker snapshot tracker for flash-dump detection.
# Maps symbol -> (epoch_seconds, last_price, quote_volume) from the prior
# tick. On every new tick we compare current ticker against this snapshot
# and fire FLASH_DUMP if price dropped >= flash threshold within the
# tick interval, with at least some real volume.  This catches sudden
# drops that the 5m-window gate would miss for another 30-60 seconds.
_PREV_TICKERS: Dict[str, Tuple[float, float, float]] = {}
BACKEND_BASE = os.getenv("BOOKNOW_BACKEND_BASE", "http://backend:8083")
BINANCE_BASE = "https://api.binance.com"

REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


def get_redis() -> redis.Redis:
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def load_config(r: redis.Redis) -> Dict[str, Any]:
    cfg = dict(DEFAULTS)
    try:
        raw = r.get(CONFIG_KEY)
        if raw:
            cfg.update(json.loads(raw))
    except Exception as exc:
        log.warning(f"config load failed: {exc}")
    return cfg


def _safe_div(a: float, b: float) -> float:
    return a / b if b > 0 else 0.0


# ── Binance API ─────────────────────────────────────────────────────────

def fetch_all_24h_tickers() -> List[Dict[str, Any]]:
    """One call returns 24hr ticker for ALL symbols."""
    try:
        r = requests.get(
            f"{BINANCE_BASE}/api/v3/ticker/24hr", timeout=8,
        )
        if r.status_code != 200:
            return []
        return r.json() or []
    except Exception as exc:
        log.warning(f"all tickers fetch failed: {exc}")
        return []


def fetch_daily_klines_7d(symbol: str) -> Optional[List[List[Any]]]:
    """Last 8 daily klines (covers 7 closed + today partial)."""
    try:
        r = requests.get(
            f"{BINANCE_BASE}/api/v3/klines",
            params={"symbol": symbol, "interval": "1d", "limit": 8},
            timeout=4,
        )
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


def fetch_hourly_klines(symbol: str, limit: int = 24) -> Optional[List[List[Any]]]:
    """Last N hourly klines for direction analysis."""
    try:
        r = requests.get(
            f"{BINANCE_BASE}/api/v3/klines",
            params={"symbol": symbol, "interval": "1h", "limit": limit},
            timeout=4,
        )
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


# iter 92 — fast-path support: 1m klines for short-window vol-surge
# detection. The 24h-rolling vol-surge gate (>=3.0x) takes 2-3 min to
# react to a sharp dump like BCH 2026-05-28 16:45 IST because the
# rolling 24h denominator absorbs the spike slowly. By also fetching
# the last ~65 minutes of 1m klines we can detect a 5m-window surge
# the moment it starts, which catches dumps minutes earlier.
def fetch_minute_klines(symbol: str, limit: int = 65) -> Optional[List[List[Any]]]:
    """Last N 1m klines for short-window vol-surge / price-move scoring.

    Binance weight = 1 per call (limit <= 100). At ~115 LMC candidates
    per 30s tick that's ~230 calls/min — well under the 6000/min budget.
    """
    try:
        r = requests.get(
            f"{BINANCE_BASE}/api/v3/klines",
            params={"symbol": symbol, "interval": "1m", "limit": limit},
            timeout=4,
        )
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


def compute_short_vol_surge(klines_1m: Optional[List[List[Any]]],
                             short_win: int = 5,
                             base_win: int = 60) -> Tuple[float, float, float, float]:
    """Compare avg per-minute quote volume in the most recent `short_win`
    minutes to the avg in the preceding `base_win` minutes.

    Returns (short_vol_usd, base_avg_per_min, surge_ratio, chg_pct).
      short_vol_usd : sum quoteVolume over last short_win 1m bars
      base_avg_per_min : avg quoteVolume per minute over preceding base_win bars
      surge_ratio : short_avg_per_min / base_avg_per_min (0 if no baseline)
      chg_pct : % price change from short_win minutes ago to last close
    """
    if not klines_1m or len(klines_1m) < short_win + 1:
        return 0.0, 0.0, 0.0, 0.0
    # Binance kline shape: [openTime, open, high, low, close, volume,
    # closeTime, quoteAssetVolume, ...]. Use quoteAssetVolume (index 7)
    # so we stay in USDT terms.
    try:
        vols = [float(k[7] or 0) for k in klines_1m]
        closes = [float(k[4] or 0) for k in klines_1m]
    except (TypeError, ValueError, IndexError):
        return 0.0, 0.0, 0.0, 0.0

    short_vols = vols[-short_win:]
    base_vols  = vols[:-short_win][-base_win:]   # exclude the short window
    if not short_vols or not base_vols:
        return 0.0, 0.0, 0.0, 0.0

    short_total = sum(short_vols)
    short_avg   = short_total / len(short_vols)
    base_avg    = sum(base_vols) / len(base_vols)
    surge       = (short_avg / base_avg) if base_avg > 0 else 0.0

    p_now  = closes[-1] if closes else 0.0
    p_then = closes[-short_win - 1] if len(closes) > short_win else 0.0
    chg = ((p_now - p_then) / p_then * 100.0) if p_then > 0 else 0.0

    return short_total, base_avg, surge, chg


# ── Universe filter ─────────────────────────────────────────────────────

def universe_from_tickers(tickers: List[Dict[str, Any]],
                           min_vol: float, max_vol: float) -> List[Dict[str, Any]]:
    """Filter to USDT pairs with 24h vol in [min_vol, 5*max_vol] (5× slop
    so we don't pre-filter the maybe-LMC coins too aggressively here —
    real LMC filter uses 7-day avg).
    """
    out = []
    upper = max_vol * 5
    for t in tickers:
        sym = t.get("symbol", "")
        if not sym.endswith("USDT"):
            continue
        # Skip leveraged tokens and stablecoins
        if any(sym.startswith(p) for p in ("USDC", "USDP", "TUSD", "BUSD", "FDUSD", "DAI")):
            continue
        if any(suf in sym for suf in ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")):
            continue
        try:
            qv = float(t.get("quoteVolume") or 0)
        except (TypeError, ValueError):
            continue
        if qv < min_vol or qv > upper:
            continue
        out.append(t)
    return out


# ── Scoring ─────────────────────────────────────────────────────────────

def compute_mcap_score(avg_vol_7d: float, cfg: Dict[str, Any]) -> Tuple[float, Dict[str, Any]]:
    w = float(cfg.get("lmcWeightMcap", 25))
    if avg_vol_7d < 500_000:
        pts = w
        bucket = "MICRO"
    elif avg_vol_7d < 1_000_000:
        pts = w * 0.8
        bucket = "TINY"
    elif avg_vol_7d < 3_000_000:
        pts = w * 0.6
        bucket = "SMALL"
    elif avg_vol_7d < 5_000_000:
        pts = w * 0.4
        bucket = "SMALLISH"
    elif avg_vol_7d < 10_000_000:
        pts = w * 0.2
        bucket = "SMALL-MID"
    else:
        pts = 0
        bucket = "MID+"
    return pts, {"bucket": bucket, "avg_7d_usd": round(avg_vol_7d, 0), "pts": round(pts, 1)}


def compute_vol_surge_score(today_vol: float, avg_vol_7d: float,
                             cfg: Dict[str, Any]) -> Tuple[float, Dict[str, Any]]:
    w = float(cfg.get("lmcWeightVolSurge", 25))
    surge = _safe_div(today_vol, avg_vol_7d)
    if surge >= 10:
        pts = w
    elif surge >= 5:
        pts = w * 0.8
    elif surge >= 3:
        pts = w * 0.6
    elif surge >= 2:
        pts = w * 0.4
    else:
        pts = 0
    return pts, {"surge_x": round(surge, 2), "pts": round(pts, 1)}


def compute_trade_surge_score(today_trades: int, avg_trades_7d: float,
                                cfg: Dict[str, Any]) -> Tuple[float, Dict[str, Any]]:
    w = float(cfg.get("lmcWeightTradeSurge", 15))
    surge = _safe_div(today_trades, avg_trades_7d)
    if surge >= 10:
        pts = w
    elif surge >= 5:
        pts = w * 0.7
    elif surge >= 3:
        pts = w * 0.5
    elif surge >= 2:
        pts = w * 0.25
    else:
        pts = 0
    return pts, {"surge_x": round(surge, 2), "pts": round(pts, 1)}


def compute_today_move_score(chg_24h: float, cfg: Dict[str, Any]) -> Tuple[float, Dict[str, Any]]:
    """Symmetric: rewards big moves in EITHER direction (direction
    handled separately).  Caller uses the sign to label PUMP/DUMP."""
    w = float(cfg.get("lmcWeightTodayMove", 20))
    abs_chg = abs(chg_24h)
    if abs_chg >= 20:
        pts = w
    elif abs_chg >= 10:
        pts = w * 0.75
    elif abs_chg >= 5:
        pts = w * 0.5
    elif abs_chg >= 2:
        pts = w * 0.25
    else:
        pts = 0
    return pts, {"chg_24h_pct": round(chg_24h, 2), "pts": round(pts, 1)}


def compute_pre_pump_quiet_score(daily_klines: List[List[Any]],
                                   cfg: Dict[str, Any]) -> Tuple[float, Dict[str, Any]]:
    """Was the coin SLEEPING for the last 7 days?  A coiled spring is
    more likely to explode than something that's been ranging wildly."""
    w = float(cfg.get("lmcWeightPrePumpQuiet", 10))
    closed_7d = daily_klines[:-1] if len(daily_klines) > 1 else daily_klines
    if len(closed_7d) < 6:
        return 0, {"reason": "insufficient history", "pts": 0}
    highs = [float(k[2]) for k in closed_7d]
    lows  = [float(k[3]) for k in closed_7d]
    hi = max(highs); lo = min(lows)
    mid = (hi + lo) / 2
    range_pct = _safe_div(hi - lo, mid) * 100 if mid > 0 else 0
    if range_pct < 20:
        pts = w
    elif range_pct < 30:
        pts = w * 0.6
    elif range_pct < 50:
        pts = w * 0.3
    else:
        pts = 0
    return pts, {"range_7d_pct": round(range_pct, 1), "pts": round(pts, 1)}


def compute_liquidity_sanity_score(today_vol: float, cfg: Dict[str, Any]) -> Tuple[float, Dict[str, Any]]:
    """Need at least some liquidity TODAY or we can't trade it."""
    w = float(cfg.get("lmcWeightLiquiditySanity", 5))
    if today_vol >= 100_000:
        return w, {"vol_today_usd": round(today_vol, 0), "pts": w}
    return 0, {"vol_today_usd": round(today_vol, 0), "pts": 0}


# ── Direction classification ────────────────────────────────────────────

def classify_direction(hourly_klines: List[List[Any]]) -> Tuple[str, Dict[str, Any]]:
    """Look at last 24h hourly action to call PUMP / DUMP / NEUTRAL."""
    if not hourly_klines or len(hourly_klines) < 6:
        return "NEUTRAL", {"reason": "insufficient hourly bars"}
    opens  = [float(k[1]) for k in hourly_klines]
    highs  = [float(k[2]) for k in hourly_klines]
    lows   = [float(k[3]) for k in hourly_klines]
    closes = [float(k[4]) for k in hourly_klines]
    qvols  = [float(k[7]) for k in hourly_klines]
    tb_quote = [float(k[10] or 0) for k in hourly_klines]

    score = 0  # +ve = pump, -ve = dump

    # 1. Green/red ratio (last 6h)
    last6 = list(zip(opens[-6:], closes[-6:]))
    greens = sum(1 for o, c in last6 if c > o)
    reds   = sum(1 for o, c in last6 if c < o)
    score += (greens - reds) * 8

    # 2. Position vs simple 24h VWAP
    typ = [(highs[i] + lows[i] + closes[i]) / 3 for i in range(len(closes))]
    vwap_num = sum(typ[i] * qvols[i] for i in range(len(closes)))
    vwap_den = sum(qvols)
    vwap = _safe_div(vwap_num, vwap_den)
    if vwap > 0:
        if closes[-1] > vwap * 1.01:
            score += 12
        elif closes[-1] > vwap:
            score += 5
        elif closes[-1] < vwap * 0.99:
            score -= 12
        else:
            score -= 5

    # 3. Distance from 24h high (breakout if close to high)
    hi_24h = max(highs)
    dist_from_high_pct = _safe_div(hi_24h - closes[-1], closes[-1]) * 100
    if dist_from_high_pct < 1:
        score += 10   # breaking out
    elif dist_from_high_pct > 10:
        score -= 8    # well below high

    # 4. Taker buy ratio last 6h
    tb6 = sum(tb_quote[-6:])
    q6  = sum(qvols[-6:])
    tbr = _safe_div(tb6, q6)
    if tbr >= 0.6:
        score += 10
    elif tbr >= 0.55:
        score += 5
    elif tbr <= 0.4:
        score -= 10
    elif tbr <= 0.45:
        score -= 5

    # Classify
    if score >= 15:
        label = "PUMP"
    elif score <= -15:
        label = "DUMP"
    else:
        label = "NEUTRAL"
    return label, {
        "dir_score": score,
        "greens_6h": greens,
        "reds_6h": reds,
        "vwap": round(vwap, 8),
        "close": round(closes[-1], 8),
        "dist_from_24h_high_pct": round(dist_from_high_pct, 2),
        "taker_buy_ratio_6h": round(tbr, 3),
    }


# ── Main evaluator ──────────────────────────────────────────────────────

def evaluate_symbol(symbol: str, ticker: Dict[str, Any],
                     daily_klines_cache: Dict[str, Tuple[float, List[List[Any]]]],
                     cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return a detection event or None if doesn't qualify."""
    # Pull 7-day daily klines (cached)
    cached = daily_klines_cache.get(symbol)
    now = time.time()
    if cached and (now - cached[0]) < 3600:
        daily = cached[1]
    else:
        daily = fetch_daily_klines_7d(symbol)
        if daily is None:
            return None
        daily_klines_cache[symbol] = (now, daily)

    if len(daily) < 7:
        return None

    closed_7d = daily[:-1]  # exclude today partial
    if len(closed_7d) < 6:
        return None

    # 7-day average quote volume + trade count (excluding today)
    quote_vols = [float(k[7]) for k in closed_7d]
    trade_counts = [int(k[8] or 0) for k in closed_7d]
    avg_vol_7d = statistics.mean(quote_vols)
    avg_trades_7d = statistics.mean(trade_counts) if trade_counts else 0

    # LMC bracket
    max_avg = float(cfg.get("lmcMaxAvgVol7d", 10_000_000))
    min_avg = float(cfg.get("lmcMinVol24h", 2_000_000))  # iter 75
    if avg_vol_7d > max_avg or avg_vol_7d < min_avg:
        return None

    # Today's totals (from ticker)
    try:
        today_vol     = float(ticker.get("quoteVolume") or 0)
        today_trades  = int(ticker.get("count") or 0)
        chg_24h_pct   = float(ticker.get("priceChangePercent") or 0)
        last_price    = float(ticker.get("lastPrice") or 0)
    except (TypeError, ValueError):
        return None

    if last_price <= 0:
        return None

    # Entry trigger
    vol_surge = _safe_div(today_vol, avg_vol_7d)
    trade_surge = _safe_div(today_trades, avg_trades_7d)

    min_vol_surge   = float(cfg.get("lmcMinVolSurge", 3.0))
    min_trade_surge = float(cfg.get("lmcMinTradeSurge", 3.0))
    min_abs_chg     = float(cfg.get("lmcMinAbsChg24h", 2.0))

    # ── iter 92 — Slow path: rolling 24h gates (existing behavior) ─
    slow_pass = (
        vol_surge   >= min_vol_surge
        and trade_surge >= min_trade_surge
        and abs(chg_24h_pct) >= min_abs_chg
    )

    # ── iter 92 — Fast path: 5-minute vol-surge gate ───────────────
    # Catches BCH-style sharp dumps 2-3 minutes earlier than the
    # rolling 24h ratio can. Cheap pre-filter (chg_24h>=1%) avoids
    # fetching 1m klines for boring/flat coins.
    fast_min_surge = float(cfg.get("lmcMin5mVolSurge", 3.0))
    fast_min_chg   = float(cfg.get("lmcMin5mAbsChg",   0.8))
    fast_prefilter_chg = float(cfg.get("lmcFastPrefilterChg24h", 1.0))

    short_vol_5m  = 0.0
    short_base_pm = 0.0
    surge_5m      = 0.0
    chg_5m_pct    = 0.0
    fast_pass     = False
    if abs(chg_24h_pct) >= fast_prefilter_chg:
        klines_1m = fetch_minute_klines(symbol, limit=65)
        short_vol_5m, short_base_pm, surge_5m, chg_5m_pct = compute_short_vol_surge(
            klines_1m, short_win=5, base_win=60
        )
        fast_pass = (surge_5m >= fast_min_surge and abs(chg_5m_pct) >= fast_min_chg)

    if not (slow_pass or fast_pass):
        return None

    trigger_source = (
        "24h+5m" if (slow_pass and fast_pass) else
        ("5m"   if fast_pass else "24h")
    )

    # Score
    s_mcap, b_mcap   = compute_mcap_score(avg_vol_7d, cfg)
    s_vol, b_vol     = compute_vol_surge_score(today_vol, avg_vol_7d, cfg)
    s_trd, b_trd     = compute_trade_surge_score(today_trades, avg_trades_7d, cfg)
    s_mv, b_mv       = compute_today_move_score(chg_24h_pct, cfg)
    s_qt, b_qt       = compute_pre_pump_quiet_score(daily, cfg)
    s_lq, b_lq       = compute_liquidity_sanity_score(today_vol, cfg)

    # iter 92 — bonus from short-window surge so a fast-path-only
    # candidate (where 24h ratios haven't caught up) can still clear
    # the watch threshold and surface in the dashboard.
    fast_bonus_w = float(cfg.get("lmcWeight5mSurge", 20.0))
    if surge_5m <= 0:
        s_fast = 0.0
    elif surge_5m >= 8.0:
        s_fast = fast_bonus_w
    elif surge_5m >= 5.0:
        s_fast = fast_bonus_w * 0.8
    elif surge_5m >= 3.0:
        s_fast = fast_bonus_w * 0.6
    elif surge_5m >= 2.0:
        s_fast = fast_bonus_w * 0.3
    else:
        s_fast = 0.0
    b_fast = {
        "trigger_source": trigger_source,
        "surge_5m": round(surge_5m, 2),
        "chg_5m_pct": round(chg_5m_pct, 2),
        "short_vol_5m_usd": round(short_vol_5m, 0),
        "base_per_min_usd": round(short_base_pm, 0),
        "pts": round(s_fast, 1),
    }

    score = round(s_mcap + s_vol + s_trd + s_mv + s_qt + s_lq + s_fast, 1)

    # Direction (requires extra API call — only do for candidates above watch)
    watch_min = float(cfg.get("lmcWatchScore", 50))
    if score < watch_min:
        return None

    hourly = fetch_hourly_klines(symbol, limit=24)
    direction, dir_breakdown = classify_direction(hourly or [])

    # Build label
    explosive_min = float(cfg.get("lmcExplosiveScore", 75))
    if score >= explosive_min:
        label = f"EXPLOSIVE_{direction}"
    else:
        label = f"WATCH_{direction}"

    return {
        "symbol": symbol,
        "ts": int(time.time() * 1000),
        "label": label,
        "score": score,
        "direction": direction,
        "trigger_price": last_price,
        "chg_24h_pct": round(chg_24h_pct, 2),
        "today_vol_usd": round(today_vol, 0),
        "avg_7d_vol_usd": round(avg_vol_7d, 0),
        "vol_surge_x": round(vol_surge, 2),
        "trade_surge_x": round(trade_surge, 2),
        # iter 92 — fast-path metrics so the dashboard + post-mortem can
        # tell whether the slow 24h gate or the 5m gate fired this event.
        "trigger_source": trigger_source,
        "surge_5m": round(surge_5m, 2),
        "chg_5m_pct": round(chg_5m_pct, 2),
        "short_vol_5m_usd": round(short_vol_5m, 0),
        "breakdown": {
            "mcap": b_mcap, "vol_surge": b_vol, "trade_surge": b_trd,
            "today_move": b_mv, "pre_pump_quiet": b_qt,
            "liquidity_sanity": b_lq,
            "fast_5m": b_fast,
        },
        "direction_breakdown": dir_breakdown,
    }


# ── Publish + trade ─────────────────────────────────────────────────────

def cooldown_active(r: redis.Redis, symbol: str) -> bool:
    try:
        return bool(r.get(f"LMC:COOLDOWN:{symbol}"))
    except Exception:
        return False


def set_cooldown(r: redis.Redis, symbol: str, ttl_s: int) -> None:
    try:
        r.setex(f"LMC:COOLDOWN:{symbol}", ttl_s, "1")
    except Exception:
        pass


# iter 93 — separate cooldown channel for the flash-dump fast path.
# Shorter than the regular 30-min cooldown (default 5 min) so a
# continuing flash dump can re-fire while still avoiding 10s-tick spam.
def flash_cooldown_active(r: redis.Redis, symbol: str) -> bool:
    try:
        return bool(r.get(f"LMC:FLASH_COOLDOWN:{symbol}"))
    except Exception:
        return False


def set_flash_cooldown(r: redis.Redis, symbol: str, ttl_s: int) -> None:
    try:
        r.setex(f"LMC:FLASH_COOLDOWN:{symbol}", ttl_s, "1")
    except Exception:
        pass


def publish_detection(r: redis.Redis, event: Dict[str, Any]) -> None:
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        r.rpush(f"LMC:DETECTIONS:{date}", json.dumps(event))
        r.expire(f"LMC:DETECTIONS:{date}", 14 * 24 * 3600)
        r.hset("LMC:LATEST", event["symbol"], json.dumps(event))
        r.rpush("LMC:OUTCOME_PENDING", json.dumps({
            "symbol": event["symbol"],
            "ts": event["ts"],
            "entry_price": event["trigger_price"],
            "label": event["label"],
            "score": event["score"],
        }))
    except Exception as exc:
        log.warning(f"publish_detection failed: {exc}")


def record_paper_trade(r: redis.Redis, event: Dict[str, Any], cfg: Dict[str, Any]) -> None:
    if not event["label"].startswith("EXPLOSIVE_PUMP"):
        return
    if event["score"] < float(cfg.get("lmcLiveScore", 75)):
        return
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    paper = {
        "symbol": event["symbol"],
        "ts": event["ts"],
        "entry_price": event["trigger_price"],
        "score": event["score"],
        "direction": event["direction"],
        "vol_surge_x": event["vol_surge_x"],
        "avg_7d_vol_usd": event["avg_7d_vol_usd"],
        "mode": "PAPER",
    }
    try:
        r.rpush(f"LMC:PAPER_TRADES:{date}", json.dumps(paper))
        r.expire(f"LMC:PAPER_TRADES:{date}", 30 * 24 * 3600)
    except Exception as exc:
        log.warning(f"paper trade record failed: {exc}")


def delegate_buy(symbol: str, event: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    url = (
        f"{BACKEND_BASE}/api/v1/order/pattern-buy/{symbol}"
        f"?sell_pct={cfg.get('lmcSellPctLabel', 5.0)}&rule_label=LMC"
    )
    try:
        r = requests.post(url, timeout=4)
        if r.status_code != 200:
            log.warning(f"[LMC] delegate {symbol} HTTP {r.status_code}: {r.text[:200]}")
            return False
        log.info(
            f"[LMC] 🚀 LIVE delegate {symbol} score={event.get('score')} "
            f"label={event.get('label')} entry={event.get('trigger_price')}"
        )
        return True
    except Exception as exc:
        log.error(f"[LMC] delegate {symbol} error: {exc}")
        return False


def is_paper_mode(cfg: Dict[str, Any]) -> bool:
    if not bool(cfg.get("lmcPaperMode", True)):
        return False
    end_str = cfg.get("lmcPaperModeEndDate")
    if not end_str:
        return True
    try:
        end = datetime.strptime(end_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) < end
    except Exception:
        return True


# ── iter 93 ─────────────────────────────────────────────────────────────
# Flash-dump fast path. Runs BEFORE evaluate_symbol() on every tick.
# Compares the current 24h ticker against the snapshot taken at the
# previous tick (10s ago by default). If price dropped >= flash_min_drop
# in that window AND at least flash_min_vol_usd of fresh volume traded,
# we publish a FLASH_DUMP event immediately. This gives 10-30s detection
# latency from the start of a real dump — vs 60-90s for the 5m gate.

def detect_flash_dump(ticker: Dict[str, Any],
                       avg_vol_7d: float,
                       cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Returns an event dict if this symbol just flash-dumped, else None.
    Mutates _PREV_TICKERS with the current snapshot.

    `ticker` is one element of the /api/v3/ticker/24hr response.
    `avg_vol_7d` is the 7-day daily avg quote volume (for scoring).
    """
    sym = ticker.get("symbol")
    if not sym:
        return None
    try:
        last_price = float(ticker.get("lastPrice") or 0)
        cur_vol    = float(ticker.get("quoteVolume") or 0)
        chg_24h    = float(ticker.get("priceChangePercent") or 0)
    except (TypeError, ValueError):
        return None
    if last_price <= 0:
        return None

    now_s = time.time()
    prev = _PREV_TICKERS.get(sym)
    _PREV_TICKERS[sym] = (now_s, last_price, cur_vol)

    if prev is None:
        return None
    prev_ts, prev_price, prev_vol = prev
    dt = now_s - prev_ts
    # Skip if snapshot too old (process restart, long stall) or zero.
    if dt <= 0 or dt > 60:
        return None
    if prev_price <= 0:
        return None

    price_chg_pct = (last_price - prev_price) / prev_price * 100.0
    vol_delta = cur_vol - prev_vol  # USDT volume added in the window

    flash_min_drop = float(cfg.get("lmcFlashMinDropPct", 0.5))
    flash_min_vol  = float(cfg.get("lmcFlashMinVolDeltaUsd", 50_000.0))

    # iter 93 — only dump direction for now (matches operator request).
    if price_chg_pct > -flash_min_drop:
        return None
    if vol_delta < flash_min_vol:
        return None

    # Simple flash score: 60 + (price-drop-magnitude × 10), capped 80.
    score = round(min(80.0, 60.0 + abs(price_chg_pct) * 10.0), 1)

    return {
        "symbol": sym,
        "ts": int(now_s * 1000),
        "label": "FLASH_DUMP",
        "score": score,
        "direction": "DUMP",
        "trigger_price": last_price,
        "trigger_source": "flash",
        # Window-level details so the dashboard can show "X% in Ys"
        "flash_window_s":      round(dt, 1),
        "flash_price_chg_pct": round(price_chg_pct, 3),
        "flash_vol_delta_usd": round(vol_delta, 0),
        # Echo 24h context for parity with the slow path's event shape
        "chg_24h_pct":   round(chg_24h, 2),
        "today_vol_usd": round(cur_vol, 0),
        "avg_7d_vol_usd": round(avg_vol_7d, 0),
        "vol_surge_x":    round(_safe_div(cur_vol, avg_vol_7d), 2),
        "trade_surge_x":  None,  # not computed in flash path
        "surge_5m":       None,
        "chg_5m_pct":     None,
        "short_vol_5m_usd": None,
        "breakdown": {
            "flash": {
                "window_s": round(dt, 1),
                "price_chg_pct": round(price_chg_pct, 3),
                "vol_delta_usd": round(vol_delta, 0),
                "pts": score,
            }
        },
    }


# ── Outcome tracker ─────────────────────────────────────────────────────

def fetch_last_price(r: redis.Redis, symbol: str) -> Optional[float]:
    try:
        raw = r.hget("CURRENT_PRICE", symbol)
        if not raw:
            return None
        obj = json.loads(raw)
        p = float(obj.get("price") or 0)
        return p if p > 0 else None
    except Exception:
        return None


def run_outcome_tracker(r: redis.Redis, cfg: Dict[str, Any]) -> None:
    checkpoints = list(cfg.get("lmcOutcomeCheckMinutes", [15, 60, 240, 1440]))
    try:
        n = r.llen("LMC:OUTCOME_PENDING")
    except Exception:
        return
    if not n:
        return
    now_ms = int(time.time() * 1000)
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    processed = 0
    requeue: List[str] = []
    for _ in range(min(n, 500)):
        try:
            raw = r.lpop("LMC:OUTCOME_PENDING")
        except Exception:
            break
        if not raw:
            break
        try:
            rec = json.loads(raw)
        except Exception:
            continue
        age_min = (now_ms - rec.get("ts", now_ms)) / 60000
        last_done = rec.get("last_cp_done", 0)
        for cp in checkpoints:
            if cp <= last_done:
                continue
            if age_min >= cp:
                price = fetch_last_price(r, rec["symbol"])
                if price and rec.get("entry_price"):
                    chg_pct = _safe_div(price - rec["entry_price"], rec["entry_price"]) * 100
                    outcome = {
                        "symbol": rec["symbol"],
                        "ts_detection": rec["ts"],
                        "checkpoint_min": cp,
                        "entry_price": rec["entry_price"],
                        "price_now": price,
                        "chg_pct": round(chg_pct, 3),
                        "label": rec.get("label"),
                        "score": rec.get("score"),
                        "ts_outcome": now_ms,
                    }
                    try:
                        r.rpush(f"LMC:OUTCOMES:{date}", json.dumps(outcome))
                        r.expire(f"LMC:OUTCOMES:{date}", 30 * 24 * 3600)
                        processed += 1
                    except Exception:
                        pass
                    rec["last_cp_done"] = cp
        if any(cp > rec.get("last_cp_done", 0) for cp in checkpoints):
            requeue.append(json.dumps(rec))
    if requeue:
        try:
            r.rpush("LMC:OUTCOME_PENDING", *requeue)
        except Exception:
            pass
    if processed:
        log.info(f"[LMC] outcome tracker recorded {processed} checkpoint(s)")


# ── Main loop ───────────────────────────────────────────────────────────

def main() -> None:
    r = get_redis()
    log.info("[LMC] started — backend=%s, redis=%s:%s",
             BACKEND_BASE, REDIS_HOST, REDIS_PORT)
    daily_cache: Dict[str, Tuple[float, List[List[Any]]]] = {}

    while True:
        try:
            cfg = load_config(r)
            if not bool(cfg.get("lmcEnabled", True)):
                time.sleep(POLL_INTERVAL_S)
                continue
            paper = is_paper_mode(cfg)
            log.info(f"[LMC] tick — mode={'PAPER' if paper else 'LIVE'}")

            tickers = fetch_all_24h_tickers()
            if not tickers:
                log.warning("[LMC] no tickers — skipping tick")
                time.sleep(POLL_INTERVAL_S)
                continue

            min_v = float(cfg.get("lmcMinVol24h", 2_000_000))  # iter 75
            max_v = float(cfg.get("lmcMaxAvgVol7d", 10_000_000))
            candidates = universe_from_tickers(tickers, min_v, max_v)
            log.info(f"[LMC] universe = {len(candidates)} candidates "
                     f"(from {len(tickers)} all USDT pairs)")

            # Prune old cache
            now = time.time()
            for sym in list(daily_cache.keys()):
                if now - daily_cache[sym][0] > 3600:
                    daily_cache.pop(sym, None)

            fires = 0
            flash_fires = 0
            flash_cd_s = int(cfg.get("lmcFlashCooldownSec", 300))   # iter93 — 5 min
            for t in candidates:
                sym = t.get("symbol", "")

                # ── iter 93 — FLASH dump fast-fast-path ────────────
                # Runs on every 10s tick, BEFORE the heavier slow/5m
                # evaluation. Detects sudden drops by comparing the
                # current 24h ticker to the snapshot from the prior
                # tick. Has its own (shorter) cooldown so it doesn't
                # spam during an extended dump.
                if not flash_cooldown_active(r, sym):
                    try:
                        # Cheap avg_7d_vol estimate from cache if we
                        # have it; else use today's 24h vol as an
                        # approximation (the field only feeds the
                        # event payload, not the gate).
                        avg7d = 0.0
                        cached = daily_cache.get(sym)
                        if cached:
                            # daily_cache value shape from evaluate_symbol
                            # is (ts, klines) — recompute the simple avg.
                            try:
                                _ts, _klines = cached
                                if _klines:
                                    avg7d = sum(float(k[7] or 0) for k in _klines[:-1]) / max(1, len(_klines) - 1)
                            except Exception:
                                avg7d = 0.0
                        if avg7d <= 0:
                            avg7d = float(t.get("quoteVolume") or 0)
                        flash_evt = detect_flash_dump(t, avg7d, cfg)
                    except Exception as exc:
                        log.debug(f"[LMC] flash check {sym} error: {exc}")
                        flash_evt = None
                    if flash_evt is not None:
                        publish_detection(r, flash_evt)
                        flash_fires += 1
                        set_flash_cooldown(r, sym, flash_cd_s)
                        log.info(
                            f"[LMC] ⚡ FLASH_DUMP {sym} "
                            f"drop={flash_evt['flash_price_chg_pct']}% in "
                            f"{flash_evt['flash_window_s']}s "
                            f"vol_delta=${flash_evt['flash_vol_delta_usd']:,.0f} "
                            f"score={flash_evt['score']}"
                        )

                if cooldown_active(r, sym):
                    continue
                try:
                    event = evaluate_symbol(sym, t, daily_cache, cfg)
                except Exception as exc:
                    log.warning(f"[LMC] evaluate {sym} error: {exc}")
                    continue
                if event is None:
                    continue

                publish_detection(r, event)
                fires += 1

                log.info(
                    f"[LMC] {event['label']} {sym} score={event['score']} "
                    f"vol_x={event['vol_surge_x']} chg_24h={event['chg_24h_pct']}% "
                    f"7d_avg=${event['avg_7d_vol_usd']:,.0f}"
                )

                live_min = float(cfg.get("lmcLiveScore", 75))
                if event["label"] == "EXPLOSIVE_PUMP" and event["score"] >= live_min:
                    if paper:
                        record_paper_trade(r, event, cfg)
                        log.info(
                            f"[LMC] 📝 PAPER BUY {sym} score={event['score']} "
                            f"entry={event['trigger_price']}"
                        )
                    else:
                        delegate_buy(sym, event, cfg)
                set_cooldown(r, sym, int(cfg.get("lmcCooldownSec", 1800)))

            log.info(f"[LMC] tick complete — {fires} slow + {flash_fires} flash "
                     f"(cache={len(daily_cache)} symbols)")

            try:
                run_outcome_tracker(r, cfg)
            except Exception as exc:
                log.warning(f"[LMC] outcome tracker error: {exc}")

        except KeyboardInterrupt:
            log.info("[LMC] shutting down")
            break
        except Exception as exc:
            log.error(f"[LMC] main loop error: {exc}")

        time.sleep(POLL_INTERVAL_S)


if __name__ == "__main__":
    main()
