#!/usr/bin/env python3
"""
dip_hunter.py — iter177 (2026-06-18)
─────────────────────────────────────────────────────────────────────────────
"Buy the DEEP dip — but only AFTER it stops falling and turns UP" detector.

Motivation — the operator was repeatedly losing money because the old
orderflow/buysignals auto-buy sources bought BREAKOUTS and HIGHS (e.g. ROSE was
bought at 5.05 USDT on its 02:00 spike top, then bled out).  The operator's
request, verbatim: "i want deep down price buy ... we should confirm the price
wont down, only will up, that deep down need to buy."

So this is a MEAN-REVERSION detector, the exact opposite of a breakout buyer:
  1. find a coin that has dropped DEEP from its recent high  (≥ DIP_MIN_DROP_PCT)
  2. make sure it is not a falling knife / delisting crash    (≥ DIP_MAX_DROP_PCT)
  3. confirm it is OVERSOLD                                    (window RSI min ≤ DIP_RSI_OVERSOLD)
  4. confirm the BOTTOM is in — a higher-low has formed        (curr_low > prior swing low)
  5. confirm momentum is TURNING UP — RSI rising off the low
  6. confirm the FIRST green reversal candle                   (close > open AND close > prev high)

Only when ALL six gates pass do we publish a SIGNAL.  We do NOT buy on the way
down; we buy the confirmed turn.  This kills the "caught a falling knife" loss
pattern.

On a fire it publishes a radar-shaped SIGNAL to DIP_RADAR:SIGNALS:<date> in MAIN
Redis.  SignalAutoBuyManager consumes it as the "dip" source (gated by
signalAutoBuyLiveEnabled + signalAutoBuySourceDip).  A per-symbol cooldown key
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
log = logging.getLogger("DipHunter")

# ── Config (Redis-backed; falls back to defaults if missing) ────────────
CONFIG_KEY = "TRADING_CONFIG"
DEFAULTS: Dict[str, Any] = {
    # single switch — when off the detector idles (saves API budget) and the
    # auto-buy manager ignores any stale signals anyway.
    "signalAutoBuySourceDip": True,
    "dipScanTopN": 120,
    "dipMinDropPct": 8.0,     # must be at least this far below the recent high
}

# Detector thresholds — tuned for the "deep dip, confirmed turn" signature.
# Kept as module constants (except the two that the operator explicitly tunes
# from the dashboard above) so a bad dashboard edit can't silently degrade the
# core safety gates; tune the rest here + redeploy.
DIP_LOOKBACK_BARS = 48        # recent-high window (closed 5m bars = 4h)
DIP_MIN_DROP_PCT = 8.0        # deep-down floor: drop from window high ≥ this %
DIP_MAX_DROP_PCT = 35.0       # falling-knife ceiling: skip if drop worse than this %
DIP_RSI_PERIOD = 14           # Wilder RSI period
DIP_RSI_WINDOW = 6            # bars to look back for the oversold extreme
DIP_RSI_OVERSOLD = 35.0       # window RSI minimum must have reached ≤ this
DIP_RSI_TURN_MIN = 35.0       # current RSI must have recovered back to ≥ this
DIP_SWING_LOOKBACK = 12       # bars (excl. current) to find the prior swing low
DIP_SIGNAL_TTL_S = 900        # per-symbol cooldown (suppress re-fires), 15m
KLINE_INTERVAL = "5m"
# need: lookback(48) + swing(12) buffer + RSI warm-up + forming bar
KLINE_LIMIT = 100

# 5m bars close every 5 min; a 60s poll catches the reversal bar within a
# minute (well inside signalAutoBuyMaxAgeSec=120) while keeping REST weight low.
POLL_INTERVAL_S = float(os.getenv("DIP_HUNTER_POLL_S", "60"))
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


def wilder_rsi(closes: List[float], period: int = DIP_RSI_PERIOD) -> List[float]:
    """Wilder's RSI over a list of closes.  Returns a list aligned to `closes`;
    the first `period` entries are filler 50.0 (not enough data yet)."""
    n = len(closes)
    if n <= period:
        return [50.0] * n
    rsis: List[float] = [50.0] * n
    gains = 0.0
    losses = 0.0
    for i in range(1, period + 1):
        ch = closes[i] - closes[i - 1]
        if ch >= 0:
            gains += ch
        else:
            losses -= ch
    avg_gain = gains / period
    avg_loss = losses / period
    rs = avg_gain / avg_loss if avg_loss > 0 else float("inf")
    rsis[period] = 100.0 - (100.0 / (1.0 + rs)) if avg_loss > 0 else 100.0
    for i in range(period + 1, n):
        ch = closes[i] - closes[i - 1]
        gain = ch if ch > 0 else 0.0
        loss = -ch if ch < 0 else 0.0
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        rs = avg_gain / avg_loss if avg_loss > 0 else float("inf")
        rsis[i] = 100.0 - (100.0 / (1.0 + rs)) if avg_loss > 0 else 100.0
    return rsis


def evaluate(klines: List[List[Any]], min_drop_pct: float) -> Optional[Dict[str, Any]]:
    """Return a signal dict if the most-recently-closed bar is a confirmed deep-dip
    reversal, else None.  klines[-1] is the in-progress (forming) bar."""
    if not klines or len(klines) < DIP_LOOKBACK_BARS + DIP_RSI_PERIOD + 3:
        return None
    closed = klines[:-1]                 # drop the forming bar
    curr = closed[-1]                    # reversal candidate (last CLOSED bar)
    prev = closed[-2]

    closes = [float(b[K_CLOSE]) for b in closed]
    highs = [float(b[K_HIGH]) for b in closed]
    lows = [float(b[K_LOW]) for b in closed]

    curr_close = closes[-1]
    curr_open = float(curr[K_OPEN])
    curr_high = highs[-1]
    curr_low = lows[-1]
    prev_high = highs[-2]
    if curr_close <= 0 or curr_open <= 0:
        return None

    # ── Gate 1: DEEP DOWN — how far below the recent-window high are we? ──
    window_high = max(highs[-DIP_LOOKBACK_BARS:])
    if window_high <= 0:
        return None
    drop_from_high = (curr_close - window_high) / window_high * 100.0  # negative
    if drop_from_high > -min_drop_pct:
        return None                      # not deep enough

    # ── Gate 2: NOT A FALLING KNIFE — reject delisting / crash dumps ──
    if drop_from_high < -DIP_MAX_DROP_PCT:
        return None

    # ── Gate 3: OVERSOLD — RSI reached an extreme low in the recent window ──
    rsis = wilder_rsi(closes, DIP_RSI_PERIOD)
    rsi_now = rsis[-1]
    rsi_prev = rsis[-2]
    rsi_window = rsis[-DIP_RSI_WINDOW:]
    rsi_min = min(rsi_window)
    if rsi_min > DIP_RSI_OVERSOLD:
        return None                      # never got oversold → not a real dip

    # ── Gate 4: BOTTOM IS IN — a higher-low has formed (price stopped falling) ─
    # prior swing low over the lookback window, EXCLUDING the current bar.
    swing_lows = lows[-(DIP_SWING_LOOKBACK + 1):-1]
    if not swing_lows:
        return None
    prior_swing_low = min(swing_lows)
    if curr_low <= prior_swing_low:
        return None                      # still making lower lows → knife still falling

    # ── Gate 5: MOMENTUM TURNING UP — RSI rising off the oversold low ──
    if not (rsi_now > rsi_prev and rsi_now >= DIP_RSI_TURN_MIN):
        return None

    # ── Gate 6: FIRST GREEN REVERSAL CANDLE — close>open AND close>prev high ──
    if not (curr_close > curr_open and curr_close > prev_high):
        return None

    chg_pct = (curr_close - curr_open) / curr_open * 100.0
    return {
        "price": curr_close,
        "drop_from_high": round(drop_from_high, 2),
        "window_high": window_high,
        "rsi_now": round(rsi_now, 1),
        "rsi_min": round(rsi_min, 1),
        "prior_swing_low": prior_swing_low,
        "curr_low": curr_low,
        "chg_pct": round(chg_pct, 2),
    }


def under_cooldown(r: redis.Redis, symbol: str) -> bool:
    try:
        return bool(r.exists(f"DIP_RADAR:COOLDOWN:{symbol}"))
    except Exception:
        return False


def set_cooldown(r: redis.Redis, symbol: str, ttl_s: int) -> None:
    try:
        r.setex(f"DIP_RADAR:COOLDOWN:{symbol}", int(ttl_s), "1")
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
        "drop_from_high": sig["drop_from_high"],
        "rsi_now": sig["rsi_now"],
        "rsi_min": sig["rsi_min"],
        "chg_pct": sig["chg_pct"],
        "source": "dip",
    }
    payload = json.dumps(event)
    try:
        key = f"DIP_RADAR:SIGNALS:{date}"
        r.rpush(key, payload)
        r.ltrim(key, -2000, -1)
        r.expire(key, 90 * 24 * 3600)
        r.hset("DIP_RADAR:LATEST", symbol, payload)
    except Exception as exc:
        log.warning(f"publish_signal failed: {exc}")


def update_status(r: redis.Redis, fields: Dict[str, Any]) -> None:
    try:
        r.hset("DIP_RADAR:STATUS", mapping={k: str(v) for k, v in fields.items()})
    except Exception:
        pass


def cycle(r: redis.Redis) -> None:
    cfg = load_config(r)
    if not bool(cfg.get("signalAutoBuySourceDip", True)):
        update_status(r, {"last_poll_ts": int(time.time() * 1000), "enabled": 0})
        return

    top_n = int(cfg.get("dipScanTopN", 120) or 120)
    min_drop = float(cfg.get("dipMinDropPct", DIP_MIN_DROP_PCT) or DIP_MIN_DROP_PCT)
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
            sig = evaluate(kl, min_drop)
        except Exception as exc:
            log.debug(f"evaluate {sym} error: {exc}")
            continue
        if not sig:
            continue
        publish_signal(r, sym, sig)
        set_cooldown(r, sym, DIP_SIGNAL_TTL_S)
        fired += 1
        log.info(
            "[DIP_RADAR] 🔵 %s DIP-REVERSAL @ %s  drop %.2f%% from high  "
            "rsi(min %.0f→now %.0f)  higherLow(%.8f>%.8f)  green %+.2f%%",
            sym, sig["price"], sig["drop_from_high"], sig["rsi_min"], sig["rsi_now"],
            sig["curr_low"], sig["prior_swing_low"], sig["chg_pct"],
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
        "[DIP_RADAR] starting — poll=%ss interval=%s lookback=%d minDrop≥%.0f%% "
        "maxDrop≤%.0f%% rsiOversold≤%.0f rsiTurn≥%.0f swingLook=%d",
        POLL_INTERVAL_S, KLINE_INTERVAL, DIP_LOOKBACK_BARS, DIP_MIN_DROP_PCT,
        DIP_MAX_DROP_PCT, DIP_RSI_OVERSOLD, DIP_RSI_TURN_MIN, DIP_SWING_LOOKBACK,
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
