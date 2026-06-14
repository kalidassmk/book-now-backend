#!/usr/bin/env python3
"""
volume_spike_pattern.py — iter 69 (2026-05-24)
─────────────────────────────────────────────────────────────────────────────
Volume-Spike Pattern (VSP) detector — predicts BIG PUMP vs BIG DUMP.

A volume spike alone is symmetric: it happens on both pumps AND dumps.
The KEY signal that disambiguates is the *taker buy ratio* from Binance
1m klines — quoteVolume that came from aggressive market BUYs vs the
total quoteVolume.  >0.6 means buyers were aggressive; <0.4 means
sellers were aggressive.  Combine that with candle color, body/wick
structure, VWAP position, and trade-count surge to classify direction;
combine with z-score, surge sustainment, and range expansion to
classify magnitude.

Pipeline per symbol per poll:
  1. Entry — any of:
       vol_1m >= entry_vol_1m_mult × prior 30m avg
       vol_5m >= entry_vol_5m_mult × prior 25m avg
       trades_1m >= entry_trade_mult × prior 30m avg
  2. Direction score D ∈ [-100, +100] (see compute_direction).
  3. Magnitude score M ∈ [0, 100]   (see compute_magnitude).
  4. Classify:
       |D| ≥ 40 + M ≥ 60  → BIG_PUMP / BIG_DUMP   (confidence ≥ 75)
       |D| ≥ 60 + M ≥ 40  → MODERATE
       else               → UNCERTAIN  (alert only)
  5. Paper mode (default 7 days): record the would-be buy, don't trade.
     Live mode: BIG_PUMP + confidence ≥ vspLiveConfidence → delegate to
     /api/v1/order/pattern-buy (inherits iter65/66 gates + HARD-SL).
  6. Outcome tracking: each detection is revisited at +5/+15/+30/+60m
     to record actual price change → VSP:OUTCOMES:<date>.

Redis output:
  VSP:DETECTIONS:<YYYY-MM-DD>   — RPUSH JSON per fire (newest at tail)
  VSP:LATEST                    — HSET symbol → latest detection JSON
  VSP:OUTCOMES:<YYYY-MM-DD>     — RPUSH JSON per outcome record
  VSP:PAPER_TRADES:<YYYY-MM-DD> — RPUSH JSON per paper trade (BIG_PUMP only)
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
log = logging.getLogger("VSP")

CONFIG_KEY = "TRADING_CONFIG"
DEFAULTS: Dict[str, Any] = {
    "vspEnabled": True,                       # iter170 — ON (VSP-only auto-buy)
    "vspPaperMode": True,                    # first 7 days paper-only
    "vspPaperModeEndDate": "2026-05-31",     # auto-flip to live after this date
    "vspTopSymbols": 100,
    "vspPollIntervalSec": 15,
    "vspCooldownSec": 600,                   # per-symbol cooldown
    "vspMin24hVolUsd": 2_000_000,            # iter 75 — bumped 1M→2M (false-signal noise)

    # Entry triggers (any of these wakes up the scorer)
    "vspEntryVol1mMult": 5.0,
    "vspEntryVol5mMult": 3.0,
    "vspEntryTradeMult": 4.0,

    # Direction scoring weights (sum to 100 if pure pump/dump)
    "vspWeightCandleColor": 20,
    "vspWeightTakerRatio": 20,
    "vspWeightBodyWick": 15,
    "vspWeightVwap": 10,
    "vspWeightChg5m": 15,
    "vspWeightHhLl": 10,
    "vspWeightTradeSize": 10,

    # Magnitude scoring weights (sum to 100)
    "vspMagWeightZScore": 25,
    "vspMagWeightSustained": 20,
    "vspMagWeightRangeExp": 15,
    "vspMagWeightAccel": 10,
    "vspMagWeightTradeSurge": 10,
    "vspMagWeightWasQuiet": 10,
    "vspMagWeightRoomToRun": 10,

    # Classification thresholds
    "vspBigDirThreshold": 40,                # |D| >= this for BIG
    "vspBigMagThreshold": 60,                # M >= this for BIG
    "vspModerateDirThreshold": 60,
    "vspModerateMagThreshold": 40,

    # Live-mode buy threshold
    "vspLiveConfidence": 75,                 # min confidence to delegate buy
    "vspSellPctLabel": 5.0,                  # passed to pattern-buy

    # iter170 — VSP-only REAL-MONEY auto-buy (mirrors trading_config.py).
    # The subprocess just forwards BIG_PUMP signals to the backend's
    # /api/v1/vsp/auto-buy/{symbol} endpoint, which owns all order logic.
    "vspAutoBuyLiveEnabled": True,
    "vspAutoBuyMinConfidence": 75,
    "vspAutoBuyCrossCheckEnabled": True,     # skip if EarlyPump/CCP/LMC/PumpRider hit it today

    # Outcome tracking
    "vspOutcomeCheckMinutes": [5, 15, 30, 60],
}

POLL_INTERVAL_S = float(os.getenv("VSP_POLL_S", "15"))
BACKEND_BASE = os.getenv("BOOKNOW_BACKEND_BASE", "http://backend:8083")
BINANCE_BASE = "https://api.binance.com"

REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_ANALYSE_HOST = os.getenv("REDIS_ANALYSE_HOST", REDIS_HOST)
REDIS_ANALYSE_PORT = int(os.getenv("REDIS_ANALYSE_PORT", "6379"))


def get_redis() -> redis.Redis:
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def get_redis_analyse() -> redis.Redis:
    return redis.Redis(host=REDIS_ANALYSE_HOST, port=REDIS_ANALYSE_PORT,
                       decode_responses=True)


def load_config(r: redis.Redis) -> Dict[str, Any]:
    """Merge defaults with TRADING_CONFIG from Redis."""
    cfg = dict(DEFAULTS)
    try:
        raw = r.get(CONFIG_KEY)
        if raw:
            cfg.update(json.loads(raw))
    except Exception as exc:
        log.warning(f"config load failed: {exc} — using defaults")
    return cfg


# ── Universe ─────────────────────────────────────────────────────────────

def pick_universe(r: redis.Redis, top_n: int) -> List[str]:
    """Pick scan universe — FAST_MOVE top-ranked first, then CURRENT_PRICE
    keys to top up.  Matches the pump_rider.py strategy so VSP scans the
    same coins that other detectors cover.

    CURRENT_PRICE entries don't carry quoteVolume (only price/percentage),
    so we don't sort by 24h vol here — instead we use FAST_MOVE rank
    (already volume-aware) for the first slice, and let the per-symbol
    24h-vol floor (vspMin24hVolUsd) gate the rest at evaluate time.
    """
    out: List[str] = []
    seen: set = set()
    # 1. FAST_MOVE top-ranked
    try:
        raw = r.hgetall("FAST_MOVE") or {}
        scored: List[Tuple[float, str]] = []
        for sym, val in raw.items():
            try:
                d = json.loads(val) if isinstance(val, str) else val
                score = float(d.get("overAllCount") or d.get("score") or 0)
                scored.append((score, sym))
            except Exception:
                continue
        scored.sort(reverse=True)
        for _, sym in scored:
            if sym not in seen and sym.endswith("USDT"):
                seen.add(sym); out.append(sym)
            if len(out) >= top_n:
                return out
    except Exception as exc:
        log.warning(f"FAST_MOVE read failed: {exc}")
    # 2. Top up from CURRENT_PRICE
    try:
        keys = list(r.hkeys("CURRENT_PRICE") or [])
        for k in keys:
            if k.endswith("USDT") and k not in seen:
                seen.add(k); out.append(k)
                if len(out) >= top_n:
                    return out
    except Exception as exc:
        log.warning(f"CURRENT_PRICE read failed: {exc}")
    return out


# ── Binance klines ───────────────────────────────────────────────────────

def fetch_klines(symbol: str, limit: int = 90) -> Optional[List[List[Any]]]:
    """Fetch 1m klines.  90 bars = 1.5h — enough for 30m baseline + ATR-14
    + 1d z-score window (we'll fall back to 1440 if needed)."""
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


def fetch_klines_long(symbol: str) -> Optional[List[List[Any]]]:
    """Fetch 1440 1m bars (= 24h) once per symbol per analysis — used for
    z-score baseline + 24h high/low context."""
    try:
        r = requests.get(
            f"{BINANCE_BASE}/api/v3/klines",
            params={"symbol": symbol, "interval": "1m", "limit": 1440},
            timeout=6,
        )
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


# ── Scoring ──────────────────────────────────────────────────────────────

def _safe_div(a: float, b: float) -> float:
    return a / b if b > 0 else 0.0


def compute_direction(closed: List[List[float]], cfg: Dict[str, Any]) -> Tuple[float, Dict[str, Any]]:
    """Direction score D ∈ [-100, +100].  Positive = pump, negative = dump."""
    opens  = [float(k[1]) for k in closed]
    highs  = [float(k[2]) for k in closed]
    lows   = [float(k[3]) for k in closed]
    closes = [float(k[4]) for k in closed]
    qvols  = [float(k[7]) for k in closed]
    tb_quote = [float(k[10] or 0) for k in closed]

    if len(closes) < 10:
        return 0.0, {"reason": "insufficient bars"}

    c, o, h, l = closes[-1], opens[-1], highs[-1], lows[-1]

    score = 0.0
    breakdown: Dict[str, Any] = {}

    # 1. Last 3 candle colors
    w = float(cfg.get("vspWeightCandleColor", 20))
    last3_open = opens[-3:]
    last3_close = closes[-3:]
    greens = sum(1 for i in range(3) if last3_close[i] > last3_open[i])
    reds   = sum(1 for i in range(3) if last3_close[i] < last3_open[i])
    color_pts = w * (greens - reds) / 3.0   # -w..+w
    score += color_pts
    breakdown["candle_color"] = {"greens": greens, "reds": reds, "pts": round(color_pts, 1)}

    # 2. Taker buy ratio (last 5m)
    w = float(cfg.get("vspWeightTakerRatio", 20))
    q5  = sum(qvols[-5:])
    tb5 = sum(tb_quote[-5:])
    tbr = _safe_div(tb5, q5)
    if tbr >= 0.6:
        tbr_pts = w
    elif tbr >= 0.55:
        tbr_pts = w * 0.5
    elif tbr <= 0.4:
        tbr_pts = -w
    elif tbr <= 0.45:
        tbr_pts = -w * 0.5
    else:
        tbr_pts = 0.0
    score += tbr_pts
    breakdown["taker_buy_ratio"] = {"ratio_5m": round(tbr, 3), "pts": round(tbr_pts, 1)}

    # 3. Body vs wick on the trigger bar
    w = float(cfg.get("vspWeightBodyWick", 15))
    rng = max(h - l, 1e-12)
    body = abs(c - o)
    upper_wick = h - max(c, o)
    lower_wick = min(c, o) - l
    body_frac = body / rng
    if c > o:  # green
        if body_frac > 0.7 and upper_wick / rng < 0.1:
            bw_pts = w
        elif body_frac > 0.5:
            bw_pts = w * 0.5
        elif upper_wick / rng > 0.5:
            bw_pts = -w * 0.5   # green doji with upper wick = exhaustion
        else:
            bw_pts = 0.0
    else:      # red
        if body_frac > 0.7 and lower_wick / rng < 0.1:
            bw_pts = -w
        elif body_frac > 0.5:
            bw_pts = -w * 0.5
        elif lower_wick / rng > 0.5:
            bw_pts = w * 0.3    # red with long lower wick = absorption
        else:
            bw_pts = 0.0
    score += bw_pts
    breakdown["body_wick"] = {
        "body_frac": round(body_frac, 2),
        "upper_wick_frac": round(upper_wick / rng, 2),
        "lower_wick_frac": round(lower_wick / rng, 2),
        "pts": round(bw_pts, 1),
    }

    # 4. VWAP position (rolling 30 bars as proxy)
    w = float(cfg.get("vspWeightVwap", 10))
    n_vwap = min(30, len(closes))
    typ = [(highs[i] + lows[i] + closes[i]) / 3 for i in range(-n_vwap, 0)]
    vwap_num = sum(typ[i] * qvols[-n_vwap + i] for i in range(n_vwap))
    vwap_den = sum(qvols[-n_vwap:])
    vwap = _safe_div(vwap_num, vwap_den)
    if vwap > 0:
        if c > vwap * 1.002:
            v_pts = w
        elif c > vwap:
            v_pts = w * 0.4
        elif c < vwap * 0.998:
            v_pts = -w
        else:
            v_pts = -w * 0.4
    else:
        v_pts = 0.0
    score += v_pts
    breakdown["vwap"] = {"vwap": round(vwap, 8), "close": round(c, 8), "pts": round(v_pts, 1)}

    # 5. Change last 5m
    w = float(cfg.get("vspWeightChg5m", 15))
    chg_5m = _safe_div(c - closes[-6], closes[-6]) * 100 if len(closes) >= 6 else 0
    if chg_5m >= 1.5:
        chg_pts = w
    elif chg_5m >= 0.5:
        chg_pts = w * 0.5
    elif chg_5m <= -1.5:
        chg_pts = -w
    elif chg_5m <= -0.5:
        chg_pts = -w * 0.5
    else:
        chg_pts = 0.0
    score += chg_pts
    breakdown["chg_5m"] = {"pct": round(chg_5m, 3), "pts": round(chg_pts, 1)}

    # 6. Higher highs / Lower lows last 5 bars
    w = float(cfg.get("vspWeightHhLl", 10))
    hh = sum(1 for i in range(-4, 0) if highs[i] > highs[i - 1])
    ll = sum(1 for i in range(-4, 0) if lows[i] < lows[i - 1])
    hl_pts = w * (hh - ll) / 4.0
    score += hl_pts
    breakdown["hh_ll"] = {"hh": hh, "ll": ll, "pts": round(hl_pts, 1)}

    # 7. Avg trade size trend (proxy: qvol / trades for last 5 vs prior 25)
    w = float(cfg.get("vspWeightTradeSize", 10))
    trades = [int(k[8] or 0) for k in closed]
    last5_avg_size = _safe_div(sum(qvols[-5:]), max(1, sum(trades[-5:])))
    prior_avg_size = _safe_div(sum(qvols[-30:-5]), max(1, sum(trades[-30:-5])))
    size_growth = _safe_div(last5_avg_size, prior_avg_size) if prior_avg_size > 0 else 1.0
    if size_growth >= 1.5:
        ts_pts = w if c > o else -w * 0.7
    elif size_growth >= 1.2:
        ts_pts = w * 0.5 if c > o else -w * 0.3
    elif size_growth <= 0.8:
        ts_pts = -w * 0.3
    else:
        ts_pts = 0.0
    score += ts_pts
    breakdown["trade_size"] = {"growth": round(size_growth, 2), "pts": round(ts_pts, 1)}

    score = max(-100.0, min(100.0, score))
    return score, breakdown


def compute_magnitude(closed: List[List[float]], long_klines: Optional[List[List[float]]],
                       cfg: Dict[str, Any]) -> Tuple[float, Dict[str, Any]]:
    """Magnitude score M ∈ [0, 100]."""
    opens  = [float(k[1]) for k in closed]
    highs  = [float(k[2]) for k in closed]
    lows   = [float(k[3]) for k in closed]
    closes = [float(k[4]) for k in closed]
    qvols  = [float(k[7]) for k in closed]
    trades = [int(k[8] or 0) for k in closed]

    score = 0.0
    breakdown: Dict[str, Any] = {}

    # 1. Vol z-score vs 1d mean
    w = float(cfg.get("vspMagWeightZScore", 25))
    if long_klines and len(long_klines) >= 60:
        long_qvols = [float(k[7]) for k in long_klines[:-1]]
        mean_qv = statistics.mean(long_qvols)
        try:
            std_qv = statistics.stdev(long_qvols)
        except statistics.StatisticsError:
            std_qv = 0.0
        z = _safe_div(qvols[-1] - mean_qv, std_qv) if std_qv > 0 else 0.0
    else:
        # Fallback: use what we have
        baseline = statistics.mean(qvols[:-1]) if len(qvols) > 1 else 0
        try:
            std_qv = statistics.stdev(qvols[:-1])
        except statistics.StatisticsError:
            std_qv = 0.0
        z = _safe_div(qvols[-1] - baseline, std_qv) if std_qv > 0 else 0.0
    if z >= 6:
        z_pts = w
    elif z >= 4:
        z_pts = w * 0.75
    elif z >= 2.5:
        z_pts = w * 0.4
    else:
        z_pts = 0.0
    score += z_pts
    breakdown["vol_z_score"] = {"z": round(z, 2), "pts": round(z_pts, 1)}

    # 2. Surge sustained (last 3 bars all >= 2× prior 25m avg)
    w = float(cfg.get("vspMagWeightSustained", 20))
    base_qv = _safe_div(sum(qvols[-30:-5]), 25) if len(qvols) >= 30 else 0
    if base_qv > 0:
        last3 = qvols[-3:]
        ratios = [q / base_qv for q in last3]
        sustained = all(r >= 2.0 for r in ratios)
    else:
        sustained = False
        ratios = []
    if sustained:
        sus_pts = w
    elif ratios and ratios[-1] >= 3.0:
        sus_pts = w * 0.5
    else:
        sus_pts = 0.0
    score += sus_pts
    breakdown["surge_sustained"] = {
        "ratios": [round(r, 2) for r in ratios],
        "sustained": sustained,
        "pts": round(sus_pts, 1),
    }

    # 3. Range expansion (5m range >= 3× ATR-14)
    w = float(cfg.get("vspMagWeightRangeExp", 15))
    trs = [highs[i] - lows[i] for i in range(-14, 0)] if len(highs) >= 14 else []
    atr = statistics.mean(trs) if trs else 0
    rng5 = max(highs[-5:]) - min(lows[-5:]) if len(highs) >= 5 else 0
    rng_ratio = _safe_div(rng5, atr * 5) if atr > 0 else 0
    if rng_ratio >= 3.0:
        rng_pts = w
    elif rng_ratio >= 2.0:
        rng_pts = w * 0.5
    elif rng_ratio >= 1.5:
        rng_pts = w * 0.25
    else:
        rng_pts = 0.0
    score += rng_pts
    breakdown["range_expansion"] = {"rng5_vs_atr": round(rng_ratio, 2), "pts": round(rng_pts, 1)}

    # 4. Price acceleration (chg_1m > chg_5m / 4)
    w = float(cfg.get("vspMagWeightAccel", 10))
    chg_1m = _safe_div(closes[-1] - opens[-1], opens[-1]) * 100
    chg_5m = _safe_div(closes[-1] - closes[-6], closes[-6]) * 100 if len(closes) >= 6 else 0
    if abs(chg_5m) > 0.1 and abs(chg_1m) > abs(chg_5m) / 4:
        acc_pts = w
    elif abs(chg_5m) > 0.1 and abs(chg_1m) > abs(chg_5m) / 6:
        acc_pts = w * 0.5
    else:
        acc_pts = 0.0
    score += acc_pts
    breakdown["acceleration"] = {
        "chg_1m": round(chg_1m, 3), "chg_5m": round(chg_5m, 3), "pts": round(acc_pts, 1),
    }

    # 5. Trade count surge
    w = float(cfg.get("vspMagWeightTradeSurge", 10))
    base_tc = _safe_div(sum(trades[-30:-5]), 25) if len(trades) >= 30 else 0
    tc_ratio = _safe_div(trades[-1], base_tc) if base_tc > 0 else 0
    if tc_ratio >= 6:
        tc_pts = w
    elif tc_ratio >= 4:
        tc_pts = w * 0.6
    elif tc_ratio >= 2.5:
        tc_pts = w * 0.3
    else:
        tc_pts = 0.0
    score += tc_pts
    breakdown["trade_surge"] = {"ratio": round(tc_ratio, 2), "pts": round(tc_pts, 1)}

    # 6. Was quiet 1h before (range last hour, excluding last 5m, < 0.5%)
    w = float(cfg.get("vspMagWeightWasQuiet", 10))
    if len(highs) >= 60:
        prior_h = max(highs[-60:-5])
        prior_l = min(lows[-60:-5])
        prior_mid = (prior_h + prior_l) / 2
        prior_range_pct = _safe_div(prior_h - prior_l, prior_mid) * 100
        if prior_range_pct < 0.5:
            q_pts = w
        elif prior_range_pct < 1.0:
            q_pts = w * 0.5
        else:
            q_pts = 0.0
    else:
        q_pts = 0.0
        prior_range_pct = 0
    score += q_pts
    breakdown["was_quiet"] = {"prior_range_pct": round(prior_range_pct, 3), "pts": round(q_pts, 1)}

    # 7. Room to run (distance from 24h high for pump, 24h low for dump)
    # This depends on direction — caller stamps `direction` later.  Use
    # closes[-1] vs 24h high/low from long_klines.
    w = float(cfg.get("vspMagWeightRoomToRun", 10))
    room_pts = 0.0
    high_24h = low_24h = c = closes[-1]
    if long_klines and len(long_klines) >= 60:
        high_24h = max(float(k[2]) for k in long_klines[:-1])
        low_24h  = min(float(k[3]) for k in long_klines[:-1])
        # Pure magnitude check: average distance from extremes — penalise
        # when price is already at either extreme.
        dist_high_pct = _safe_div(high_24h - c, c) * 100
        dist_low_pct  = _safe_div(c - low_24h, c) * 100
        # If we're between extremes (room either way), full points.
        if min(dist_high_pct, dist_low_pct) >= 1.0:
            room_pts = w
        elif min(dist_high_pct, dist_low_pct) >= 0.3:
            room_pts = w * 0.5
        else:
            room_pts = 0.0
    score += room_pts
    breakdown["room_to_run"] = {
        "dist_high_pct": round(_safe_div(high_24h - c, c) * 100, 2),
        "dist_low_pct":  round(_safe_div(c - low_24h, c) * 100, 2),
        "pts": round(room_pts, 1),
    }

    score = max(0.0, min(100.0, score))
    return score, breakdown


def evaluate_symbol(symbol: str, cfg: Dict[str, Any],
                     long_cache: Dict[str, List[List[float]]]) -> Optional[Dict[str, Any]]:
    """Run the full pipeline; return event dict or None if no entry."""
    ks = fetch_klines(symbol, limit=62)
    if not ks or len(ks) < 32:
        return None
    closed = ks[:-1]   # exclude in-progress

    # ── Entry check ──
    try:
        qvols  = [float(k[7]) for k in closed]
        trades = [int(k[8] or 0) for k in closed]
        closes = [float(k[4]) for k in closed]
    except (ValueError, IndexError, TypeError):
        return None
    if len(qvols) < 30:
        return None

    base_qv_30m = _safe_div(sum(qvols[-30:]), 30)
    base_qv_25m = _safe_div(sum(qvols[-30:-5]), 25)
    base_tc_30m = _safe_div(sum(trades[-30:]), 30)

    vol_1m_mult = _safe_div(qvols[-1], base_qv_30m)
    qv_5m_avg = _safe_div(sum(qvols[-5:]), 5)
    vol_5m_mult = _safe_div(qv_5m_avg, base_qv_25m)
    trade_1m_mult = _safe_div(trades[-1], base_tc_30m)

    entry_v1 = float(cfg.get("vspEntryVol1mMult", 5.0))
    entry_v5 = float(cfg.get("vspEntryVol5mMult", 3.0))
    entry_tc = float(cfg.get("vspEntryTradeMult", 4.0))

    entry_triggered = (
        vol_1m_mult >= entry_v1
        or vol_5m_mult >= entry_v5
        or trade_1m_mult >= entry_tc
    )
    if not entry_triggered:
        return None

    # ── 24h vol floor ──
    qv_24h_est = sum(qvols)  # last 60m sum scaled up ~24
    # Use long klines for true 24h vol
    long_ks = long_cache.get(symbol)
    if long_ks is None:
        long_ks = fetch_klines_long(symbol)
        if long_ks:
            long_cache[symbol] = long_ks
    if long_ks and len(long_ks) >= 60:
        qv_24h = sum(float(k[7]) for k in long_ks[:-1])
    else:
        qv_24h = qv_24h_est * 24

    min_24h = float(cfg.get("vspMin24hVolUsd", 2_000_000))  # iter 75
    if qv_24h < min_24h:
        return None

    # ── Direction + Magnitude ──
    d_score, d_breakdown = compute_direction(closed, cfg)
    m_score, m_breakdown = compute_magnitude(closed, long_ks, cfg)

    # ── Classify ──
    big_dir = float(cfg.get("vspBigDirThreshold", 40))
    big_mag = float(cfg.get("vspBigMagThreshold", 60))
    mod_dir = float(cfg.get("vspModerateDirThreshold", 60))
    mod_mag = float(cfg.get("vspModerateMagThreshold", 40))

    confidence = min(100.0, abs(d_score) * 0.6 + m_score * 0.4)

    label = "UNCERTAIN"
    if abs(d_score) >= big_dir and m_score >= big_mag:
        label = "BIG_PUMP" if d_score > 0 else "BIG_DUMP"
    elif abs(d_score) >= mod_dir and m_score >= mod_mag:
        label = "MODERATE_PUMP" if d_score > 0 else "MODERATE_DUMP"

    # ── iter164 — BIG_PUMP buy-grade tightening (data-driven, 25d backtest) ──
    # BIG_PUMP is VSP's strongest buy label, but its raw forward return faded
    # (chasing). Requiring (a) a strongly buy-dominant 5m taker ratio and
    # (b) a non-blow-off 1m volume spike lifted avg 15m return +0.24% → +0.45%
    # with a lower loss-rate. When BIG_PUMP fails either, downgrade it to
    # MODERATE_PUMP (still publishes, lower conviction). cfg-gated + reversible.
    if label == "BIG_PUMP" and bool(cfg.get("vspBigPumpTightenEnabled", True)):
        tbr5 = float((d_breakdown.get("taker_buy_ratio") or {}).get("ratio_5m", 0.0) or 0.0)
        min_tbr = float(cfg.get("vspBigPumpMinTaker5m", 0.60))
        max_v1m = float(cfg.get("vspBigPumpMaxVol1mMult", 10.0))
        if tbr5 < min_tbr or vol_1m_mult >= max_v1m:
            label = "MODERATE_PUMP"

    # ── Buy/Sell volume on trigger candle (UI-only; additive) ──
    buy_vol = sell_vol = None
    try:
        tk = closed[-1]
        total_qv = float(tk[7])          # quote asset volume (USDT)
        taker_buy = float(tk[10])        # taker buy quote volume (USDT)
        buy_vol = round(taker_buy, 2)
        sell_vol = round(max(0.0, total_qv - taker_buy), 2)
    except (ValueError, IndexError, TypeError):
        buy_vol = sell_vol = None

    return {
        "symbol": symbol,
        "ts": int(time.time() * 1000),
        "label": label,
        "direction_score": round(d_score, 1),
        "magnitude_score": round(m_score, 1),
        "confidence": round(confidence, 1),
        "trigger_close": closes[-1],
        "buy_vol": buy_vol,
        "sell_vol": sell_vol,
        "entry": {
            "vol_1m_mult": round(vol_1m_mult, 2),
            "vol_5m_mult": round(vol_5m_mult, 2),
            "trade_1m_mult": round(trade_1m_mult, 2),
        },
        "direction": d_breakdown,
        "magnitude": m_breakdown,
        "qv_24h_usdt": round(qv_24h, 0),
    }


# ── Publish + Trade ──────────────────────────────────────────────────────

def cooldown_active(r: redis.Redis, symbol: str) -> bool:
    try:
        return bool(r.get(f"VSP:COOLDOWN:{symbol}"))
    except Exception:
        return False


def set_cooldown(r: redis.Redis, symbol: str, ttl_s: int) -> None:
    try:
        r.setex(f"VSP:COOLDOWN:{symbol}", ttl_s, "1")
    except Exception:
        pass


def publish_detection(r: redis.Redis, event: Dict[str, Any]) -> None:
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        r.rpush(f"VSP:DETECTIONS:{date}", json.dumps(event))
        r.ltrim(f"VSP:DETECTIONS:{date}", -2000, -1)        # cap day-key (t3.micro RAM)
        r.expire(f"VSP:DETECTIONS:{date}", 90 * 24 * 3600)  # iter157 90d retention (history page)
        r.hset("VSP:LATEST", event["symbol"], json.dumps(event))
        # Outcome-pending queue for the tracker pass
        r.rpush("VSP:OUTCOME_PENDING", json.dumps({
            "symbol": event["symbol"],
            "ts": event["ts"],
            "entry_price": event["trigger_close"],
            "label": event["label"],
            "confidence": event["confidence"],
        }))
    except Exception as exc:
        log.warning(f"publish_detection failed: {exc}")


def record_paper_trade(r: redis.Redis, event: Dict[str, Any], cfg: Dict[str, Any]) -> None:
    """Record what we WOULD have bought in paper mode."""
    if event["label"] != "BIG_PUMP":
        return
    if event["confidence"] < float(cfg.get("vspLiveConfidence", 75)):
        return
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    paper = {
        "symbol": event["symbol"],
        "ts": event["ts"],
        "entry_price": event["trigger_close"],
        "confidence": event["confidence"],
        "direction_score": event["direction_score"],
        "magnitude_score": event["magnitude_score"],
        "mode": "PAPER",
    }
    try:
        r.rpush(f"VSP:PAPER_TRADES:{date}", json.dumps(paper))
        r.expire(f"VSP:PAPER_TRADES:{date}", 30 * 24 * 3600)
    except Exception as exc:
        log.warning(f"paper trade record failed: {exc}")


def delegate_buy(symbol: str, event: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    """POST a BIG_PUMP signal to the backend's VSP-only auto-buy endpoint.

    iter170 — replaces the old capped pattern-buy delegate.  We forward
    the SIGNAL price (event['trigger_close']); the backend's
    VspAutoBuyManager owns ALL order logic: buy at signal price (skip if
    current price already above), 20 USDT, +30% TP / -6% SL bracket, and
    a 5-position cap.  Gated server-side by vspAutoBuyLiveEnabled — this
    path is isolated from every other bot/ladder.
    """
    signal_price = event.get("trigger_close")
    confidence = event.get("confidence", 0)
    label = event.get("label", "BIG_PUMP")
    url = (
        f"{BACKEND_BASE}/api/v1/vsp/auto-buy/{symbol}"
        f"?signal_price={signal_price}&label={label}&confidence={confidence}"
    )
    try:
        r = requests.post(url, timeout=4)
        if r.status_code != 200:
            log.warning(
                f"[VSP] auto-buy {symbol} HTTP {r.status_code}: {r.text[:200]}"
            )
            return False
        try:
            body = r.json()
        except Exception:
            body = {}
        log.info(
            f"[VSP] 🚀 auto-buy {symbol} → {body.get('status')} "
            f"({body.get('reason', '')}) conf={confidence} "
            f"label={label} signal={signal_price}"
        )
        return True
    except Exception as exc:
        log.error(f"[VSP] auto-buy {symbol} error: {exc}")
        return False


def other_algo_signaled_today(
    r: redis.Redis,
    r_an: Optional[redis.Redis],
    symbol: str,
) -> Optional[str]:
    """iter171 — VSP-only exclusivity gate.

    Return the NAME of another detector that already fired ``symbol``
    TODAY (UTC), or ``None`` if VSP is the only one.  We check:

      • EarlyPump → EARLY_PUMP:LATEST hash on the analyse-DB (per-symbol,
        carries ``ts`` ms — counted only if today).
      • CCP       → CCP:DETECTIONS:<date>        (today-only by key)
      • LMC       → LMC:DETECTIONS:<date> +
                    INSTANT_PUMP:DETECTIONS:<date> (same algo family)
      • PumpRider → PUMP_RIDER:DETECTIONS:<date>

    The DETECTIONS:<date> lists are already scoped to today by their key,
    so for those we just look for the symbol; for EarlyPump's LATEST hash
    we verify the timestamp is from today.
    """
    symbol = symbol.upper()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # 1) EarlyPump — analyse-DB LATEST hash (timestamped).
    if r_an is not None:
        try:
            raw = r_an.hget("EARLY_PUMP:LATEST", symbol)
            if raw:
                ev = json.loads(raw)
                ts = float(ev.get("ts") or 0)
                if ts > 0:
                    d = datetime.fromtimestamp(
                        ts / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")
                    if d == today:
                        return "EarlyPump"
        except Exception as exc:
            log.debug(f"[VSP] EarlyPump cross-check {symbol}: {exc}")

    # 2-4) Today's DETECTIONS lists on the main DB (key already = today).
    checks = (
        ("CCP", f"CCP:DETECTIONS:{today}"),
        ("LMC", f"LMC:DETECTIONS:{today}"),
        ("LMC", f"INSTANT_PUMP:DETECTIONS:{today}"),
        ("PumpRider", f"PUMP_RIDER:DETECTIONS:{today}"),
    )
    for name, key in checks:
        try:
            items = r.lrange(key, 0, 999) or []
        except Exception as exc:
            log.debug(f"[VSP] {name} cross-check {symbol}: {exc}")
            continue
        for it in items:
            try:
                ev = json.loads(it)
            except Exception:
                continue
            if str(ev.get("symbol", "")).upper() == symbol:
                return name
    return None


def is_paper_mode(cfg: Dict[str, Any]) -> bool:
    if not bool(cfg.get("vspPaperMode", True)):
        return False
    # Auto-flip after end date.
    end_str = cfg.get("vspPaperModeEndDate")
    if not end_str:
        return True
    try:
        end = datetime.strptime(end_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) < end
    except Exception:
        return True


# ── Outcome tracking ─────────────────────────────────────────────────────

def fetch_last_price(r: redis.Redis, symbol: str) -> Optional[float]:
    """Read latest price from CURRENT_PRICE hash (maintained by Market
    Scanner WS).  Schema: {symbol, percentage, price, timestamp, ...}.
    """
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
    """Walk VSP:OUTCOME_PENDING; for each entry whose age has reached one
    of the checkpoints, fetch current price, record outcome, and either
    requeue (for next checkpoint) or drop."""
    checkpoints = list(cfg.get("vspOutcomeCheckMinutes", [5, 15, 30, 60]))
    if not checkpoints:
        return
    try:
        n = r.llen("VSP:OUTCOME_PENDING")
    except Exception:
        return
    if not n:
        return
    now_ms = int(time.time() * 1000)
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    processed = 0
    requeue: List[str] = []
    for _ in range(min(n, 500)):  # cap per pass
        try:
            raw = r.lpop("VSP:OUTCOME_PENDING")
        except Exception:
            break
        if not raw:
            break
        try:
            rec = json.loads(raw)
        except Exception:
            continue
        age_min = (now_ms - rec.get("ts", now_ms)) / 60000
        next_cp = None
        for cp in checkpoints:
            if age_min < cp:
                next_cp = cp
                break
        if next_cp is None:
            # All checkpoints passed → finalise + drop.
            continue
        # Has this entry already crossed its next checkpoint?
        last_done = rec.get("last_cp_done", 0)
        # Find checkpoints we haven't recorded yet and that have passed.
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
                        "confidence": rec.get("confidence"),
                        "ts_outcome": now_ms,
                    }
                    try:
                        r.rpush(f"VSP:OUTCOMES:{date}", json.dumps(outcome))
                        r.expire(f"VSP:OUTCOMES:{date}", 30 * 24 * 3600)
                        processed += 1
                    except Exception:
                        pass
                    rec["last_cp_done"] = cp
        # If more checkpoints remain, requeue.
        last_done = rec.get("last_cp_done", 0)
        if any(cp > last_done for cp in checkpoints):
            requeue.append(json.dumps(rec))
    # Restore requeued items.
    if requeue:
        try:
            r.rpush("VSP:OUTCOME_PENDING", *requeue)
        except Exception:
            pass
    if processed:
        log.info(f"[VSP] outcome tracker recorded {processed} checkpoint(s)")


# ── Main loop ────────────────────────────────────────────────────────────

def main() -> None:
    r = get_redis()
    try:
        r_an = get_redis_analyse()   # EarlyPump detections live here
    except Exception as exc:
        log.warning(f"[VSP] analyse-DB unavailable ({exc}); EarlyPump cross-check off")
        r_an = None
    log.info("[VSP] started — backend=%s, redis=%s:%s",
             BACKEND_BASE, REDIS_HOST, REDIS_PORT)

    long_cache: Dict[str, List[List[float]]] = {}
    long_cache_ts: Dict[str, float] = {}
    LONG_CACHE_TTL = 600  # refresh per-symbol 24h klines every 10min

    while True:
        try:
            cfg = load_config(r)
            if not bool(cfg.get("vspEnabled", True)):
                time.sleep(POLL_INTERVAL_S)
                continue
            paper = is_paper_mode(cfg)
            log.info(f"[VSP] tick — mode={'PAPER' if paper else 'LIVE'}")

            symbols = pick_universe(r, int(cfg.get("vspTopSymbols", 100)))
            log.info(f"[VSP] universe = {len(symbols)} symbols")

            # Prune expired long_cache.
            now = time.time()
            for sym in list(long_cache_ts.keys()):
                if now - long_cache_ts[sym] > LONG_CACHE_TTL:
                    long_cache.pop(sym, None)
                    long_cache_ts.pop(sym, None)

            fires = 0
            for sym in symbols:
                if cooldown_active(r, sym):
                    continue
                try:
                    event = evaluate_symbol(sym, cfg, long_cache)
                except Exception as exc:
                    log.warning(f"[VSP] evaluate {sym} error: {exc}")
                    continue
                if event is None:
                    continue
                # Refresh cache stamp on any long-klines pull.
                if sym in long_cache:
                    long_cache_ts[sym] = now

                publish_detection(r, event)
                fires += 1

                if event["label"] in ("BIG_PUMP", "MODERATE_PUMP", "BIG_DUMP", "MODERATE_DUMP"):
                    log.info(
                        f"[VSP] {event['label']} {sym} "
                        f"D={event['direction_score']} M={event['magnitude_score']} "
                        f"conf={event['confidence']} entry={event['trigger_close']}"
                    )

                # Decide on action
                if event["label"] == "BIG_PUMP" and \
                        event["confidence"] >= float(cfg.get("vspLiveConfidence", 75)):
                    if paper:
                        record_paper_trade(r, event, cfg)
                        log.info(
                            f"[VSP] 📝 PAPER BUY {sym} conf={event['confidence']} "
                            f"entry={event['trigger_close']}"
                        )
                    else:
                        # iter171 — VSP-only exclusivity: skip the buy if
                        # another algo already flagged this coin today.
                        blocker = None
                        if bool(cfg.get("vspAutoBuyCrossCheckEnabled", True)):
                            blocker = other_algo_signaled_today(r, r_an, sym)
                        if blocker:
                            log.info(
                                f"[VSP] ⛔ skip auto-buy {sym} — already signalled "
                                f"today by {blocker} (VSP-only exclusivity)"
                            )
                        else:
                            delegate_buy(sym, event, cfg)
                # BIG_DUMP — emit dashboard banner; never short.
                set_cooldown(r, sym, int(cfg.get("vspCooldownSec", 600)))

            log.info(f"[VSP] tick complete — {fires} signals")

            # Outcome tracker
            try:
                run_outcome_tracker(r, cfg)
            except Exception as exc:
                log.warning(f"[VSP] outcome tracker error: {exc}")

        except KeyboardInterrupt:
            log.info("[VSP] shutting down")
            break
        except Exception as exc:
            log.error(f"[VSP] main loop error: {exc}")

        time.sleep(POLL_INTERVAL_S)


if __name__ == "__main__":
    main()
