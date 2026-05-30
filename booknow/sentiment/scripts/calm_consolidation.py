#!/usr/bin/env python3
"""
calm_consolidation.py — iter 72 (2026-05-24)
─────────────────────────────────────────────────────────────────────────────
Calm Consolidation Pattern (CCP) detector.

Thesis:
  Price drifting flat + volume drying up = sellers exhausted,
  accumulation phase.  This is the OPPOSITE of panic (price down +
  vol UP) and the precursor to many sharp moves.  When selling
  pressure fades, the next move is often a breakout — usually UP if
  we're near recent lows ("spring loaded").

Direction bias (where in the range are we consolidating?):
  • Near 24h low + 7d low → CALM_REVERSAL_UP   (best setup)
  • Middle of range       → CALM_NEUTRAL
  • Near 24h high         → CALM_BREAKDOWN_RISK (avoid)

Pipeline per poll (every 60s):
  1. Fetch /api/v3/ticker/24hr ONCE (returns all symbols)
  2. Filter to USDT pairs with 24h vol >= lmcMinVol24h
     (re-use the same liquidity floor)
  3. For each candidate:
       a. Fetch 12 × 5m + 24 × 1h klines
       b. Run consolidation gate (price flat + vol declining + range tight)
       c. If passes, score 0-100 across 7 factors
       d. Classify direction from position vs 24h/7d range
  4. Paper mode: record would-be CALM_REVERSAL_UP buys
  5. Live mode: delegate via /api/v1/order/pattern-buy

Redis output:
  CCP:DETECTIONS:<date>     RPUSH per fire
  CCP:LATEST                symbol → latest event
  CCP:PAPER_TRADES:<date>   would-be buys
  CCP:OUTCOMES:<date>       actual price chg at +30/+120/+360/+1440 min
  CCP:COOLDOWN:<sym>        per-symbol re-fire cooldown
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
log = logging.getLogger("CCP")

CONFIG_KEY = "TRADING_CONFIG"
DEFAULTS: Dict[str, Any] = {
    "ccpEnabled": False,
    "ccpPaperMode": True,
    "ccpPaperModeEndDate": "2026-05-31",
    "ccpPollIntervalSec": 60,
    "ccpCooldownSec": 1800,
    "ccpMin24hVolUsd": 2_000_000,  # iter 75 — bumped 500K→2M (false-signal noise)
    "ccpTopSymbols": 200,

    # Gate thresholds
    "ccpMaxAbsChg1hPct": 2.0,
    "ccpMaxAbsChg4hPct": 3.0,
    "ccpMaxVolRatio": 0.7,
    "ccpMaxRangeRatio": 0.5,
    "ccpMaxSpikeCount": 2,
    "ccpSpikePct": 0.5,

    # Scoring weights
    "ccpWeightVolDrying": 25,
    "ccpWeightRangeContraction": 20,
    "ccpWeightSellersExhausted": 15,
    "ccpWeightDipDepth": 15,
    "ccpWeightTimeAtLow": 10,
    "ccpWeightSupportProximity": 10,
    "ccpWeightLowVolatility": 5,

    # Classification thresholds
    "ccpMinScore": 50,
    "ccpQualityScore": 75,
    "ccpLiveScore": 75,
    "ccpSellPctLabel": 5.0,
    "ccpOutcomeCheckMinutes": [30, 120, 360, 1440],
}

POLL_INTERVAL_S = float(os.getenv("CCP_POLL_S", "60"))
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


# ── Binance ─────────────────────────────────────────────────────────────

def fetch_all_24h_tickers() -> List[Dict[str, Any]]:
    try:
        r = requests.get(f"{BINANCE_BASE}/api/v3/ticker/24hr", timeout=8)
        if r.status_code != 200:
            return []
        return r.json() or []
    except Exception as exc:
        log.warning(f"tickers fetch failed: {exc}")
        return []


def fetch_5m_klines(symbol: str, limit: int = 12) -> Optional[List[List[Any]]]:
    try:
        r = requests.get(
            f"{BINANCE_BASE}/api/v3/klines",
            params={"symbol": symbol, "interval": "5m", "limit": limit + 1},
            timeout=4,
        )
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


def fetch_hourly_klines(symbol: str, limit: int = 24) -> Optional[List[List[Any]]]:
    try:
        r = requests.get(
            f"{BINANCE_BASE}/api/v3/klines",
            params={"symbol": symbol, "interval": "1h", "limit": limit + 1},
            timeout=4,
        )
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


def fetch_daily_klines(symbol: str, limit: int = 7) -> Optional[List[List[Any]]]:
    try:
        r = requests.get(
            f"{BINANCE_BASE}/api/v3/klines",
            params={"symbol": symbol, "interval": "1d", "limit": limit + 1},
            timeout=4,
        )
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


# ── Gate (entry conditions) ─────────────────────────────────────────────

def passes_gate(klines_5m: List[List[Any]], klines_1h: List[List[Any]],
                  cfg: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """All conditions must pass for the symbol to be scored."""
    closed_5m = klines_5m[:-1] if len(klines_5m) > 1 else klines_5m
    closed_1h = klines_1h[:-1] if len(klines_1h) > 1 else klines_1h
    if len(closed_5m) < 12 or len(closed_1h) < 4:
        return False, {"reason": "insufficient bars"}

    opens  = [float(k[1]) for k in closed_5m]
    highs  = [float(k[2]) for k in closed_5m]
    lows   = [float(k[3]) for k in closed_5m]
    closes = [float(k[4]) for k in closed_5m]
    qvols  = [float(k[7]) for k in closed_5m]

    # 1. Price drift small over 1h
    c_now, c_1h = closes[-1], closes[0]
    chg_1h_pct = _safe_div(c_now - c_1h, c_1h) * 100
    if abs(chg_1h_pct) > float(cfg.get("ccpMaxAbsChg1hPct", 2.0)):
        return False, {"reason": f"chg_1h {chg_1h_pct:.2f}% too large"}

    # 2. Price drift small over 4h (using hourly bars)
    closes_1h = [float(k[4]) for k in closed_1h]
    if len(closes_1h) >= 4:
        c_4h_ago = closes_1h[-4]
        chg_4h_pct = _safe_div(c_now - c_4h_ago, c_4h_ago) * 100
        if abs(chg_4h_pct) > float(cfg.get("ccpMaxAbsChg4hPct", 3.0)):
            return False, {"reason": f"chg_4h {chg_4h_pct:.2f}% too large"}
    else:
        chg_4h_pct = chg_1h_pct

    # 3. Volume declining
    vol_recent = sum(qvols[-6:]) / 6
    vol_prior  = sum(qvols[:6]) / 6
    vol_ratio = _safe_div(vol_recent, vol_prior)
    if vol_ratio > float(cfg.get("ccpMaxVolRatio", 0.7)):
        return False, {"reason": f"vol_ratio {vol_ratio:.2f} not declining"}

    # 4. Range contracting
    range_recent = max(highs[-6:]) - min(lows[-6:])
    range_prior  = max(highs[:6])  - min(lows[:6])
    range_ratio = _safe_div(range_recent, range_prior)
    if range_ratio > float(cfg.get("ccpMaxRangeRatio", 0.5)):
        return False, {"reason": f"range_ratio {range_ratio:.2f} not contracting"}

    # 5. No big spikes
    spike_pct = float(cfg.get("ccpSpikePct", 0.5))
    spike_count = 0
    for i in range(1, len(closes)):
        delta_pct = abs(_safe_div(closes[i] - closes[i - 1], closes[i - 1])) * 100
        if delta_pct > spike_pct:
            spike_count += 1
    if spike_count > int(cfg.get("ccpMaxSpikeCount", 2)):
        return False, {"reason": f"too many spikes ({spike_count})"}

    return True, {
        "chg_1h_pct": round(chg_1h_pct, 3),
        "chg_4h_pct": round(chg_4h_pct, 3),
        "vol_ratio": round(vol_ratio, 3),
        "range_ratio": round(range_ratio, 3),
        "spike_count": spike_count,
    }


# ── Scoring ─────────────────────────────────────────────────────────────

def compute_score(klines_5m: List[List[Any]], klines_1h: List[List[Any]],
                    klines_1d: Optional[List[List[Any]]],
                    cfg: Dict[str, Any]) -> Tuple[float, Dict[str, Any]]:
    closed_5m = klines_5m[:-1] if len(klines_5m) > 1 else klines_5m
    closed_1h = klines_1h[:-1] if len(klines_1h) > 1 else klines_1h
    if len(closed_5m) < 12 or len(closed_1h) < 6:
        return 0, {"reason": "insufficient bars"}

    opens5  = [float(k[1]) for k in closed_5m]
    highs5  = [float(k[2]) for k in closed_5m]
    lows5   = [float(k[3]) for k in closed_5m]
    closes5 = [float(k[4]) for k in closed_5m]
    qvols5  = [float(k[7]) for k in closed_5m]

    highs1h  = [float(k[2]) for k in closed_1h]
    lows1h   = [float(k[3]) for k in closed_1h]
    closes1h = [float(k[4]) for k in closed_1h]
    qvols1h  = [float(k[7]) for k in closed_1h]

    breakdown: Dict[str, Any] = {}
    score = 0.0

    # 1. Volume drying (25 pts) — recent 1h vol vs 24h avg
    w = float(cfg.get("ccpWeightVolDrying", 25))
    vol_1h_now = sum(qvols5)  # 12 × 5m = 1h
    vol_24h_avg = statistics.mean(qvols1h) if qvols1h else 0
    dryness = _safe_div(vol_1h_now, vol_24h_avg)
    if dryness < 0.3:
        pts = w
    elif dryness < 0.5:
        pts = w * 0.8
    elif dryness < 0.7:
        pts = w * 0.6
    elif dryness < 1.0:
        pts = w * 0.3
    else:
        pts = 0
    score += pts
    breakdown["vol_drying"] = {"ratio": round(dryness, 3), "pts": round(pts, 1)}

    # 2. Range contraction (20 pts) — recent 1h range vs hourly ATR
    w = float(cfg.get("ccpWeightRangeContraction", 20))
    trs = [highs1h[i] - lows1h[i] for i in range(len(closed_1h))]
    atr_h = statistics.mean(trs) if trs else 0
    range_1h_now = max(highs5) - min(lows5)
    contraction = _safe_div(range_1h_now, atr_h)
    if contraction < 0.3:
        pts = w
    elif contraction < 0.5:
        pts = w * 0.7
    elif contraction < 0.8:
        pts = w * 0.4
    else:
        pts = 0
    score += pts
    breakdown["range_contraction"] = {"ratio_vs_atr": round(contraction, 3), "pts": round(pts, 1)}

    # 3. Sellers exhausted (15 pts) — last 10 5m bars: red bars with declining vol
    w = float(cfg.get("ccpWeightSellersExhausted", 15))
    last10 = list(zip(opens5[-10:], closes5[-10:], qvols5[-10:]))
    red_count = sum(1 for o, c, _ in last10 if c < o)
    # Vol on red bars (sellers giving up = falling vol on reds)
    red_vols = [v for o, c, v in last10 if c < o]
    avg_red_vol = statistics.mean(red_vols) if red_vols else 0
    avg_all_vol = statistics.mean(qvols5[-10:])
    if red_count >= 4 and avg_red_vol < avg_all_vol * 0.7:
        pts = w
    elif red_count >= 3 and avg_red_vol < avg_all_vol:
        pts = w * 0.6
    elif red_count >= 2:
        pts = w * 0.3
    else:
        pts = 0
    score += pts
    breakdown["sellers_exhausted"] = {
        "red_count": red_count,
        "avg_red_vol_ratio": round(_safe_div(avg_red_vol, avg_all_vol), 2),
        "pts": round(pts, 1),
    }

    # 4. Prior dip depth (15 pts) — was the coin recently down BEFORE consolidating?
    w = float(cfg.get("ccpWeightDipDepth", 15))
    # Look 6h back: was there a meaningful dip?
    if len(closes1h) >= 6:
        h_6h_ago = max(highs1h[-6:])  # peak in last 6h
        l_recent = min(lows1h[-3:])   # recent low (last 3h)
        dip_pct = _safe_div(h_6h_ago - l_recent, h_6h_ago) * 100
        if dip_pct >= 5:
            pts = w
        elif dip_pct >= 3:
            pts = w * 0.7
        elif dip_pct >= 1.5:
            pts = w * 0.4
        elif dip_pct >= 0.5:
            pts = w * 0.2
        else:
            pts = 0
    else:
        dip_pct = 0
        pts = 0
    score += pts
    breakdown["dip_depth"] = {"dip_6h_pct": round(dip_pct, 2), "pts": round(pts, 1)}

    # 5. Time at low (10 pts) — how many of last 12 5m bars are near the local low
    w = float(cfg.get("ccpWeightTimeAtLow", 10))
    local_low = min(lows5)
    local_high = max(highs5)
    near_low_threshold = local_low + (local_high - local_low) * 0.3  # bottom 30%
    bars_near_low = sum(1 for c in closes5 if c <= near_low_threshold)
    if bars_near_low >= 8:
        pts = w
    elif bars_near_low >= 5:
        pts = w * 0.6
    elif bars_near_low >= 3:
        pts = w * 0.3
    else:
        pts = 0
    score += pts
    breakdown["time_at_low"] = {"bars_in_bottom_30pct": bars_near_low, "pts": round(pts, 1)}

    # 6. Support proximity (10 pts) — close to 24h low
    w = float(cfg.get("ccpWeightSupportProximity", 10))
    low_24h = min(lows1h)
    high_24h = max(highs1h)
    range_24h = high_24h - low_24h
    if range_24h > 0:
        pos_in_range_pct = _safe_div(closes5[-1] - low_24h, range_24h) * 100
        if pos_in_range_pct <= 20:
            pts = w
        elif pos_in_range_pct <= 35:
            pts = w * 0.6
        elif pos_in_range_pct <= 50:
            pts = w * 0.3
        else:
            pts = 0
    else:
        pos_in_range_pct = 50
        pts = 0
    score += pts
    breakdown["support_proximity"] = {"pos_in_24h_range_pct": round(pos_in_range_pct, 1), "pts": round(pts, 1)}

    # 7. Low volatility (5 pts) — no big wicks
    w = float(cfg.get("ccpWeightLowVolatility", 5))
    max_wick_frac = 0
    for i in range(len(closed_5m)):
        rng = max(highs5[i] - lows5[i], 1e-12)
        upper = highs5[i] - max(opens5[i], closes5[i])
        lower = min(opens5[i], closes5[i]) - lows5[i]
        wick_frac = (upper + lower) / rng
        max_wick_frac = max(max_wick_frac, wick_frac)
    if max_wick_frac < 0.5:
        pts = w
    elif max_wick_frac < 0.7:
        pts = w * 0.5
    else:
        pts = 0
    score += pts
    breakdown["low_volatility"] = {"max_wick_frac": round(max_wick_frac, 2), "pts": round(pts, 1)}

    return round(score, 1), breakdown


# ── Direction bias ──────────────────────────────────────────────────────

def classify_direction(klines_1h: List[List[Any]],
                         klines_1d: Optional[List[List[Any]]]) -> Tuple[str, Dict[str, Any]]:
    """Position in 24h + 7d range determines bias."""
    closed_1h = klines_1h[:-1] if len(klines_1h) > 1 else klines_1h
    if len(closed_1h) < 6:
        return "NEUTRAL", {"reason": "insufficient hourly bars"}
    highs1h = [float(k[2]) for k in closed_1h]
    lows1h  = [float(k[3]) for k in closed_1h]
    close_now = float(closed_1h[-1][4])
    low_24h  = min(lows1h)
    high_24h = max(highs1h)
    range_24h = high_24h - low_24h
    pos_24h_pct = _safe_div(close_now - low_24h, range_24h) * 100 if range_24h > 0 else 50

    # 7d range
    pos_7d_pct = 50.0
    if klines_1d:
        closed_1d = klines_1d[:-1] if len(klines_1d) > 1 else klines_1d
        if len(closed_1d) >= 3:
            highs_7d = [float(k[2]) for k in closed_1d]
            lows_7d  = [float(k[3]) for k in closed_1d]
            l7 = min(lows_7d); h7 = max(highs_7d)
            r7 = h7 - l7
            pos_7d_pct = _safe_div(close_now - l7, r7) * 100 if r7 > 0 else 50

    # Bias rules
    if pos_24h_pct <= 25 and pos_7d_pct <= 35:
        bias = "REVERSAL_UP"
    elif pos_24h_pct >= 75 or pos_7d_pct >= 80:
        bias = "BREAKDOWN_RISK"
    else:
        bias = "NEUTRAL"

    return bias, {
        "close": close_now,
        "low_24h": low_24h,
        "high_24h": high_24h,
        "pos_24h_pct": round(pos_24h_pct, 1),
        "pos_7d_pct": round(pos_7d_pct, 1),
    }


# ── Per-symbol evaluator ────────────────────────────────────────────────

def evaluate_symbol(symbol: str, ticker: Dict[str, Any],
                     daily_cache: Dict[str, Tuple[float, List[List[Any]]]],
                     cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # 24h vol floor
    try:
        qv_24h = float(ticker.get("quoteVolume") or 0)
        last_price = float(ticker.get("lastPrice") or 0)
        chg_24h = float(ticker.get("priceChangePercent") or 0)
    except (TypeError, ValueError):
        return None
    if qv_24h < float(cfg.get("ccpMin24hVolUsd", 2_000_000)):  # iter 75
        return None
    if last_price <= 0:
        return None
    # Skip coins already moving big today (active pump/dump, not consolidating)
    if abs(chg_24h) > 10:
        return None

    klines_5m = fetch_5m_klines(symbol, limit=12)
    if not klines_5m:
        return None
    klines_1h = fetch_hourly_klines(symbol, limit=24)
    if not klines_1h:
        return None

    gate_ok, gate_meta = passes_gate(klines_5m, klines_1h, cfg)
    if not gate_ok:
        return None

    # Daily klines (cached 1h)
    cached = daily_cache.get(symbol)
    now = time.time()
    if cached and (now - cached[0]) < 3600:
        klines_1d = cached[1]
    else:
        klines_1d = fetch_daily_klines(symbol, limit=7)
        if klines_1d:
            daily_cache[symbol] = (now, klines_1d)

    score, score_breakdown = compute_score(klines_5m, klines_1h, klines_1d, cfg)
    if score < float(cfg.get("ccpMinScore", 50)):
        return None

    direction, dir_meta = classify_direction(klines_1h, klines_1d)
    quality_min = float(cfg.get("ccpQualityScore", 75))
    if score >= quality_min:
        label = f"CALM_{direction}"
    else:
        label = f"WATCH_{direction}"

    return {
        "symbol": symbol,
        "ts": int(time.time() * 1000),
        "label": label,
        "score": score,
        "direction": direction,
        "trigger_price": last_price,
        "chg_24h_pct": round(chg_24h, 2),
        "today_vol_usd": round(qv_24h, 0),
        "gate": gate_meta,
        "breakdown": score_breakdown,
        "direction_breakdown": dir_meta,
    }


# ── Publish + trade ─────────────────────────────────────────────────────

def cooldown_active(r: redis.Redis, symbol: str) -> bool:
    try:
        return bool(r.get(f"CCP:COOLDOWN:{symbol}"))
    except Exception:
        return False


def set_cooldown(r: redis.Redis, symbol: str, ttl_s: int) -> None:
    try:
        r.setex(f"CCP:COOLDOWN:{symbol}", ttl_s, "1")
    except Exception:
        pass


def publish_detection(r: redis.Redis, event: Dict[str, Any]) -> None:
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        r.rpush(f"CCP:DETECTIONS:{date}", json.dumps(event))
        r.expire(f"CCP:DETECTIONS:{date}", 14 * 24 * 3600)
        r.hset("CCP:LATEST", event["symbol"], json.dumps(event))
        r.rpush("CCP:OUTCOME_PENDING", json.dumps({
            "symbol": event["symbol"],
            "ts": event["ts"],
            "entry_price": event["trigger_price"],
            "label": event["label"],
            "score": event["score"],
        }))
    except Exception as exc:
        log.warning(f"publish failed: {exc}")


def record_paper_trade(r: redis.Redis, event: Dict[str, Any], cfg: Dict[str, Any]) -> None:
    if event["label"] != "CALM_REVERSAL_UP":
        return
    if event["score"] < float(cfg.get("ccpLiveScore", 75)):
        return
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    paper = {
        "symbol": event["symbol"],
        "ts": event["ts"],
        "entry_price": event["trigger_price"],
        "score": event["score"],
        "direction": event["direction"],
        "pos_in_24h_pct": event.get("direction_breakdown", {}).get("pos_24h_pct"),
        "mode": "PAPER",
    }
    try:
        r.rpush(f"CCP:PAPER_TRADES:{date}", json.dumps(paper))
        r.expire(f"CCP:PAPER_TRADES:{date}", 30 * 24 * 3600)
    except Exception as exc:
        log.warning(f"paper trade failed: {exc}")


def delegate_buy(symbol: str, event: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    url = (
        f"{BACKEND_BASE}/api/v1/order/pattern-buy/{symbol}"
        f"?sell_pct={cfg.get('ccpSellPctLabel', 5.0)}&rule_label=CCP"
    )
    try:
        r = requests.post(url, timeout=4)
        if r.status_code != 200:
            log.warning(f"[CCP] delegate {symbol} HTTP {r.status_code}: {r.text[:200]}")
            return False
        log.info(
            f"[CCP] 🚀 LIVE delegate {symbol} score={event.get('score')} "
            f"label={event.get('label')} entry={event.get('trigger_price')}"
        )
        return True
    except Exception as exc:
        log.error(f"[CCP] delegate {symbol} error: {exc}")
        return False


def is_paper_mode(cfg: Dict[str, Any]) -> bool:
    if not bool(cfg.get("ccpPaperMode", True)):
        return False
    end_str = cfg.get("ccpPaperModeEndDate")
    if not end_str:
        return True
    try:
        end = datetime.strptime(end_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) < end
    except Exception:
        return True


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
    checkpoints = list(cfg.get("ccpOutcomeCheckMinutes", [30, 120, 360, 1440]))
    try:
        n = r.llen("CCP:OUTCOME_PENDING")
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
            raw = r.lpop("CCP:OUTCOME_PENDING")
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
                        r.rpush(f"CCP:OUTCOMES:{date}", json.dumps(outcome))
                        r.expire(f"CCP:OUTCOMES:{date}", 30 * 24 * 3600)
                        processed += 1
                    except Exception:
                        pass
                    rec["last_cp_done"] = cp
        if any(cp > rec.get("last_cp_done", 0) for cp in checkpoints):
            requeue.append(json.dumps(rec))
    if requeue:
        try:
            r.rpush("CCP:OUTCOME_PENDING", *requeue)
        except Exception:
            pass
    if processed:
        log.info(f"[CCP] outcome tracker recorded {processed} checkpoint(s)")


# ── Main loop ───────────────────────────────────────────────────────────

def main() -> None:
    r = get_redis()
    log.info("[CCP] started — backend=%s, redis=%s:%s",
             BACKEND_BASE, REDIS_HOST, REDIS_PORT)
    daily_cache: Dict[str, Tuple[float, List[List[Any]]]] = {}

    while True:
        try:
            cfg = load_config(r)
            if not bool(cfg.get("ccpEnabled", True)):
                time.sleep(POLL_INTERVAL_S)
                continue
            paper = is_paper_mode(cfg)
            log.info(f"[CCP] tick — mode={'PAPER' if paper else 'LIVE'}")

            tickers = fetch_all_24h_tickers()
            if not tickers:
                log.warning("[CCP] no tickers — skipping")
                time.sleep(POLL_INTERVAL_S)
                continue

            # Pre-filter universe by 24h vol + chg
            min_vol = float(cfg.get("ccpMin24hVolUsd", 2_000_000))  # iter 75
            top_n = int(cfg.get("ccpTopSymbols", 200))
            candidates = []
            for t in tickers:
                sym = t.get("symbol", "")
                if not sym.endswith("USDT"):
                    continue
                if any(sym.startswith(p) for p in ("USDC", "USDP", "TUSD", "BUSD", "FDUSD", "DAI")):
                    continue
                if any(suf in sym for suf in ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")):
                    continue
                try:
                    qv = float(t.get("quoteVolume") or 0)
                    chg = float(t.get("priceChangePercent") or 0)
                except (TypeError, ValueError):
                    continue
                if qv < min_vol or abs(chg) > 10:
                    continue
                candidates.append(t)
            # Sort by vol descending, cap at top_n
            candidates.sort(key=lambda t: float(t.get("quoteVolume") or 0), reverse=True)
            candidates = candidates[:top_n]
            log.info(f"[CCP] candidates = {len(candidates)} (after vol+chg filter)")

            # Prune cache
            now = time.time()
            for sym in list(daily_cache.keys()):
                if now - daily_cache[sym][0] > 3600:
                    daily_cache.pop(sym, None)

            fires = 0
            for t in candidates:
                sym = t.get("symbol", "")
                if cooldown_active(r, sym):
                    continue
                try:
                    event = evaluate_symbol(sym, t, daily_cache, cfg)
                except Exception as exc:
                    log.warning(f"[CCP] evaluate {sym} error: {exc}")
                    continue
                if event is None:
                    continue

                publish_detection(r, event)
                fires += 1

                log.info(
                    f"[CCP] {event['label']} {sym} score={event['score']} "
                    f"pos_24h={event['direction_breakdown'].get('pos_24h_pct')}% "
                    f"chg_24h={event['chg_24h_pct']}% vol={event['today_vol_usd']:,.0f}"
                )

                live_min = float(cfg.get("ccpLiveScore", 75))
                if event["label"] == "CALM_REVERSAL_UP" and event["score"] >= live_min:
                    if paper:
                        record_paper_trade(r, event, cfg)
                        log.info(
                            f"[CCP] 📝 PAPER BUY {sym} score={event['score']} "
                            f"entry={event['trigger_price']}"
                        )
                    else:
                        delegate_buy(sym, event, cfg)
                set_cooldown(r, sym, int(cfg.get("ccpCooldownSec", 1800)))

            log.info(f"[CCP] tick complete — {fires} signals "
                     f"(cache={len(daily_cache)} symbols)")

            try:
                run_outcome_tracker(r, cfg)
            except Exception as exc:
                log.warning(f"[CCP] outcome tracker error: {exc}")

        except KeyboardInterrupt:
            log.info("[CCP] shutting down")
            break
        except Exception as exc:
            log.error(f"[CCP] main loop error: {exc}")

        time.sleep(POLL_INTERVAL_S)


if __name__ == "__main__":
    main()
