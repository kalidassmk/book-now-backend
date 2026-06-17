#!/usr/bin/env python3
"""
stealth_accumulation.py — iter176 (2026-06-17)
─────────────────────────────────────────────────────────────────────────────
"Buy the quiet coin BEFORE it pumps" detector.

Motivation — the operator does not want to chase aggressive pump tops (high
risk, late entry, frequent stop-outs).  Instead: find a coin that is in SLOW
movement while someone is quietly accumulating it, and buy the moment that
accumulation resolves UP — before the explosion.

The signature was derived + backtested on REDUSDT and 19 other coins (15m
candles, 5 days).  REDUSDT's +7.7% pump on 2026-06-16 started exactly here:

  15:15–17:30  tight 0.094x band, low volume, but taker-buy% 61–81%  ← STEALTH
  17:45        vol 148k (≈4.5× the coil avg), close 0.0960 breaks the band,
               buy% 67%, GREEN candle                                  ← IGNITION
  → ran to 0.1034 (+7.7%).

Backtest (20 coins / 5 days): 8 signals, 75% win, no big losses — because the
detector fires on the spring-release of a quiet coil, not on a hot top.

The signature (all on CLOSED 15m bars):
  COIL  (last ACCUM_COIL_BARS bars before the ignition bar)
    • high/low spread ≤ ACCUM_RANGE_MAX_PCT          (quiet, tight)
    • avg taker-buy% ≥ ACCUM_BUY_AVG_MIN             (hidden demand)
    • ≥ ACCUM_BUY_HI_BARS bars with taker-buy% ≥ 60  (persistent buying)
  IGNITION  (the most-recently-CLOSED bar)
    • volume ≥ ACCUM_IG_VOL_MULT × coil-avg volume   (spring release)
    • close > coil high                              (breakout)
    • taker-buy% ≥ ACCUM_IG_BUY_MIN                  (buyers in control)
    • close > open                                   (green)
  ANTI-CHASE
    • prior 12-bar (3h) run-up ≤ ACCUM_ANTI_CHASE_PCT (don't enter mid-move)

On a fire it publishes a radar-shaped SIGNAL to STEALTH_ACCUM:SIGNALS:<date>
in MAIN Redis.  SignalAutoBuyManager consumes it as the "accumulation" source
(paper-first, gated by signalAutoBuyLiveEnabled).  A per-symbol cooldown key
suppresses re-fires.  This script BUYS NOTHING itself — it only detects.

All state lives in Redis so the supervisor can restart this freely.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import redis
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("StealthAccum")

# ── Config (Redis-backed; falls back to defaults if missing) ────────────
CONFIG_KEY = "TRADING_CONFIG"
DEFAULTS: Dict[str, Any] = {
    # single switch — when off the detector idles (saves API budget) and the
    # auto-buy manager ignores any stale signals anyway.
    "signalAutoBuySourceAccumulation": True,
    "accumScanTopN": 120,
}

# Detector thresholds — the validated backtest values (kept as constants so a
# bad dashboard edit can't silently degrade the signature; tune here + redeploy).
ACCUM_COIL_BARS = 8           # coil look-back window (closed 15m bars)
ACCUM_RANGE_MAX_PCT = 1.4     # coil high/low spread ceiling, %
ACCUM_BUY_AVG_MIN = 55.0      # avg taker-buy% across the coil
ACCUM_BUY_HI_BARS = 3         # ≥ this many coil bars with taker-buy% ≥ 60
ACCUM_IG_VOL_MULT = 3.0       # ignition vol ≥ this × coil-avg vol
ACCUM_IG_BUY_MIN = 60.0       # ignition taker-buy% floor
ACCUM_ANTI_CHASE_PCT = 6.0    # skip if prior 12-bar (3h) run-up exceeds this
ACCUM_SIGNAL_TTL_S = 600      # per-symbol cooldown (suppress re-fires)
KLINE_INTERVAL = "15m"
KLINE_LIMIT = 22              # enough for anti-chase(12) + coil(8) + ignition + forming

# 15m bars close every 15 min; a 60s poll catches the ignition bar within a
# minute (well inside signalAutoBuyMaxAgeSec=120) while keeping REST weight low
# (~120 kline calls/min for the default top-120 scan).
POLL_INTERVAL_S = float(os.getenv("STEALTH_ACCUM_POLL_S", "60"))
BINANCE_BASE = "https://api.binance.com"

REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# Binance kline field indices
K_OPEN, K_HIGH, K_LOW, K_CLOSE = 1, 2, 3, 4
K_VOL, K_TAKER_BUY_BASE = 5, 9


def get_redis() -> redis.Redis:
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def load_config(r: redis.Redis) -> Dict[str, Any]:
    raw = r.get(CONFIG_KEY)
    if not raw:
        return dict(DEFAULTS)
    try:
        c = json.loads(raw)
    except Exception:
        return dict(DEFAULTS)
    out = dict(DEFAULTS)
    for k in DEFAULTS:
        if k in c and c[k] is not None:
            out[k] = c[k]
    return out


def pick_symbols(r: redis.Redis, top_n: int) -> List[str]:
    """Top-N USDT symbols by FAST_MOVE score, topped up with CURRENT_PRICE."""
    out: List[str] = []
    seen: set = set()
    try:
        raw = r.hgetall("FAST_MOVE") or {}
        scored: List[tuple] = []
        for sym, val in raw.items():
            try:
                d = json.loads(val)
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
    except Exception as e:
        log.warning(f"FAST_MOVE read failed: {e}")
    try:
        keys = list(r.hkeys("CURRENT_PRICE") or [])
        for k in keys:
            if k.endswith("USDT") and k not in seen:
                seen.add(k); out.append(k)
                if len(out) >= top_n:
                    return out
    except Exception:
        pass
    return out


def fetch_klines(symbol: str, limit: int = KLINE_LIMIT) -> Optional[List[List[Any]]]:
    try:
        r = requests.get(
            f"{BINANCE_BASE}/api/v3/klines",
            params={"symbol": symbol, "interval": KLINE_INTERVAL, "limit": limit},
            timeout=5,
        )
        if r.status_code != 200:
            return None
        return r.json()
    except Exception as exc:
        log.debug(f"klines fetch failed for {symbol}: {exc}")
        return None


def _buy_pct(bar: List[Any]) -> float:
    vol = float(bar[K_VOL] or 0)
    if vol <= 0:
        return 0.0
    return float(bar[K_TAKER_BUY_BASE] or 0) / vol * 100.0


def evaluate(klines: List[List[Any]]) -> Optional[Dict[str, Any]]:
    """Return a signal dict if the most-recently-closed bar is a stealth
    ignition, else None.  klines[-1] is the in-progress (forming) bar."""
    # need: 12 anti-chase + 8 coil + 1 ignition + 1 forming
    if not klines or len(klines) < ACCUM_COIL_BARS + 14:
        return None
    closed = klines[:-1]              # drop the forming bar
    ig = closed[-1]                   # ignition candidate (last CLOSED bar)
    coil = closed[-1 - ACCUM_COIL_BARS:-1]
    if len(coil) < ACCUM_COIL_BARS:
        return None

    hi = max(float(b[K_HIGH]) for b in coil)
    lo = min(float(b[K_LOW]) for b in coil)
    if lo <= 0:
        return None
    rng_pct = (hi - lo) / lo * 100.0
    avg_buy = sum(_buy_pct(b) for b in coil) / len(coil)
    hi_bars = sum(1 for b in coil if _buy_pct(b) >= 60.0)
    coil_avg_vol = sum(float(b[K_VOL]) for b in coil) / len(coil)
    if coil_avg_vol <= 0:
        return None

    is_coil = (
        rng_pct <= ACCUM_RANGE_MAX_PCT
        and avg_buy >= ACCUM_BUY_AVG_MIN
        and hi_bars >= ACCUM_BUY_HI_BARS
    )
    if not is_coil:
        return None

    # anti-chase: prior 12-bar (3h) run-up using the bar BEFORE ignition.
    prev_close = float(closed[-2][K_CLOSE])
    base_close = float(closed[-14][K_CLOSE])
    if base_close > 0:
        run = (prev_close - base_close) / base_close * 100.0
        if run > ACCUM_ANTI_CHASE_PCT:
            return None

    ig_close = float(ig[K_CLOSE])
    ig_open = float(ig[K_OPEN])
    ig_vol = float(ig[K_VOL])
    ig_buy = _buy_pct(ig)
    ig_vol_mult = ig_vol / coil_avg_vol

    ignite = (
        ig_vol_mult >= ACCUM_IG_VOL_MULT
        and ig_close > hi
        and ig_buy >= ACCUM_IG_BUY_MIN
        and ig_close > ig_open
    )
    if not ignite:
        return None

    chg_pct = (ig_close - ig_open) / ig_open * 100.0 if ig_open > 0 else 0.0
    return {
        "price": ig_close,
        "vol_surge": round(ig_vol_mult, 2),
        "chg_pct": round(chg_pct, 2),
        "coil_range_pct": round(rng_pct, 2),
        "coil_buy_avg": round(avg_buy, 1),
        "coil_hi_bars": hi_bars,
        "ig_buy_pct": round(ig_buy, 1),
        "coil_hi": hi,
        "coil_lo": lo,
    }


def under_cooldown(r: redis.Redis, symbol: str) -> bool:
    try:
        return bool(r.exists(f"STEALTH_ACCUM:COOLDOWN:{symbol}"))
    except Exception:
        return False


def set_cooldown(r: redis.Redis, symbol: str, ttl_s: int) -> None:
    try:
        r.setex(f"STEALTH_ACCUM:COOLDOWN:{symbol}", int(ttl_s), "1")
    except Exception:
        pass


def publish_signal(r: redis.Redis, symbol: str, sig: Dict[str, Any]) -> None:
    """Append a radar-shaped SIGNAL that SignalAutoBuyManager._scan_radar reads."""
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    event = {
        "symbol": symbol,
        "ts": int(time.time() * 1000),
        "label": "SIGNAL",
        "price": sig["price"],
        "vol_surge": sig["vol_surge"],
        "chg_pct": sig["chg_pct"],
        "coil_range_pct": sig["coil_range_pct"],
        "coil_buy_avg": sig["coil_buy_avg"],
        "coil_hi_bars": sig["coil_hi_bars"],
        "ig_buy_pct": sig["ig_buy_pct"],
        "source": "stealth_accumulation",
    }
    payload = json.dumps(event)
    try:
        key = f"STEALTH_ACCUM:SIGNALS:{date}"
        r.rpush(key, payload)
        r.ltrim(key, -2000, -1)
        r.expire(key, 90 * 24 * 3600)
        r.hset("STEALTH_ACCUM:LATEST", symbol, payload)
    except Exception as exc:
        log.warning(f"publish_signal failed: {exc}")


def update_status(r: redis.Redis, fields: Dict[str, Any]) -> None:
    try:
        r.hset("STEALTH_ACCUM:STATUS", mapping={k: str(v) for k, v in fields.items()})
    except Exception:
        pass


def cycle(r: redis.Redis) -> None:
    cfg = load_config(r)
    if not bool(cfg.get("signalAutoBuySourceAccumulation", True)):
        update_status(r, {"last_poll_ts": int(time.time() * 1000), "enabled": 0})
        return

    top_n = int(cfg.get("accumScanTopN", 120) or 120)
    symbols = pick_symbols(r, top_n)
    if not symbols:
        update_status(r, {"last_poll_ts": int(time.time() * 1000), "scanned": 0})
        return

    scanned = fired = skipped_cd = 0
    for sym in symbols:
        if under_cooldown(r, sym):
            skipped_cd += 1
            continue
        kl = fetch_klines(sym)
        if not kl:
            continue
        scanned += 1
        try:
            sig = evaluate(kl)
        except Exception as exc:
            log.debug(f"evaluate {sym} error: {exc}")
            continue
        if not sig:
            continue
        publish_signal(r, sym, sig)
        set_cooldown(r, sym, ACCUM_SIGNAL_TTL_S)
        fired += 1
        log.info(
            "[STEALTH_ACCUM] 🟢 %s SIGNAL @ %s  coil(range %.2f%% buy%% %.0f hi%d) "
            "ignite(vol %.1fx buy%% %.0f chg %+.2f%%)",
            sym, sig["price"], sig["coil_range_pct"], sig["coil_buy_avg"],
            sig["coil_hi_bars"], sig["vol_surge"], sig["ig_buy_pct"], sig["chg_pct"],
        )

    update_status(r, {
        "last_poll_ts": int(time.time() * 1000),
        "scanned": scanned,
        "fired": fired,
        "skipped_cooldown": skipped_cd,
        "enabled": 1,
    })


def main() -> None:
    r = get_redis()
    try:
        r.ping()
    except Exception as exc:
        log.error(f"redis ping failed: {exc}")
        sys.exit(1)
    log.info(
        "[STEALTH_ACCUM] starting — poll=%ss interval=%s coilBars=%d range≤%.1f%% "
        "buyAvg≥%.0f hiBars≥%d igVol≥%.1fx igBuy≥%.0f antiChase≤%.0f%%",
        POLL_INTERVAL_S, KLINE_INTERVAL, ACCUM_COIL_BARS, ACCUM_RANGE_MAX_PCT,
        ACCUM_BUY_AVG_MIN, ACCUM_BUY_HI_BARS, ACCUM_IG_VOL_MULT, ACCUM_IG_BUY_MIN,
        ACCUM_ANTI_CHASE_PCT,
    )
    while True:
        t0 = time.time()
        try:
            cycle(r)
        except Exception as exc:
            log.error(f"cycle error: {exc}", exc_info=True)
        elapsed = time.time() - t0
        time.sleep(max(1.0, POLL_INTERVAL_S - elapsed))


if __name__ == "__main__":
    main()
