#!/usr/bin/env python3
"""
pump_rider.py — iter 55 (2026-05-23)
─────────────────────────────────────────────────────────────────────────────
"Volume-leads-price" pump detector.

Motivation — the MEUSDT pump on 2026-05-23:
  07:46  $0.0997 → $0.1000   vol 1.4x baseline   (build-up)
  07:47  $0.1000 → $0.1020   vol 2.9x baseline   ← LEAD-IN candle
  07:48  $0.1020 → $0.1042   vol 4.9x baseline   ← CONFIRMATION
  07:49  $0.1042 → $0.1075   vol 4.7x baseline   ← +3.17%
  07:50  $0.1075 → $0.1096   vol 5.6x baseline   ← peak $0.1101
  07:51  $0.1099 → $0.1052   vol 6.5x baseline   ← reversal (RED)
  07:52  $0.1050 → $0.0990   vol 8.6x baseline   ← crash

The R1/R2/R3 path bought at 07:52 — *during the crash* — because its
ST timing data lagged the price action by ~15 min.  A direct
volume-spike detector running on 1m closes would have caught the
07:47 lead-in and ridden two candles for a +0.40 net win.

This subprocess:
  1. Every POLL_INTERVAL_S, refreshes the top-N most-active USDT pairs
     from CURRENT_PRICE in Redis (avoids scanning all 600+).
  2. For each, fetches the last 25 1m klines from Binance.
  3. Applies entry rules on the most-recently-CLOSED candle:
       - vol_qty_usdt >= cfg.pumpRiderVolMultipleThreshold * baseline_qv
       - price change %     >= cfg.pumpRiderMinPriceChangePct
       - prior candle vol  >= cfg.pumpRiderMinPriorVolMultiple * baseline_qv
       - 24h vol           >= cfg.pumpRiderMinVol24hUsd
       - cumulative gain from `pumpRiderMaxLookbackCandles` ago
                            <= cfg.pumpRiderMaxCumulativeGainPct
       - the candle is GREEN  (close > open) — guards against red-candle
         spikes (distribution)
  4. On a fire, publishes to PUMP_RIDER:DETECTIONS:<date> AND calls the
     python backend's /api/v1/order/pattern-buy/{symbol} endpoint so
     the buy lands in TradeState and inherits HARD-SL / dynamic TP /
     filter pipeline.
  5. Per-symbol cooldown PUMP_RIDER:COOLDOWN:<sym> (default 10 min)
     prevents re-fires.  Also defers to the existing RULES_COOLDOWN:<sym>
     key set by iter52.

All state lives in Redis so the supervisor can restart this freely.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from statistics import mean
from typing import Any, Dict, List, Optional

import redis
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("PumpRider")

# ── Config (Redis-backed; falls back to defaults if missing) ────────────
CONFIG_KEY = "TRADING_CONFIG"
DEFAULTS: Dict[str, Any] = {
    "pumpRiderEnabled": True,
    "pumpRiderVolMultipleThreshold": 2.5,    # vol > 2.5x baseline
    "pumpRiderMinPriceChangePct": 0.8,       # candle must close +0.8%
    "pumpRiderMinPriorVolMultiple": 1.5,     # prior candle warm-up
    "pumpRiderMinVol24hUsd": 1_000_000,      # liquidity floor
    "pumpRiderMaxCumulativeGainPct": 8.0,    # skip if already pumped > +8%
    "pumpRiderMaxLookbackCandles": 10,       # look-back window for cumulative %
    "pumpRiderTopSymbols": 50,               # scan top-N by FAST_MOVE score
    "pumpRiderCooldownSec": 600,             # 10 min per-symbol cooldown
    "pumpRiderSellPctLabel": 5.0,            # passed to try_buy (cfg.profitAmountUsdt wins downstream)
    # iter 56 (2026-05-23) — Early Pump watchlist intersection
    "pumpRiderWatchlistMode": "prefer",      # "off" | "prefer" | "require"
    "pumpRiderWatchlistScoreMin": 60,        # Early Pump score floor
    "pumpRiderWatchlistMaxAgeSec": 1800,     # ignore EP detections older than 30 min
}
POLL_INTERVAL_S = float(os.getenv("PUMP_RIDER_POLL_S", "10"))
BACKEND_BASE = os.getenv("BOOKNOW_BACKEND_BASE", "http://backend:8083")
BINANCE_BASE = "https://api.binance.com"

REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_ANALYSE_HOST = os.getenv("REDIS_ANALYSE_HOST", REDIS_HOST)
REDIS_ANALYSE_PORT = int(os.getenv("REDIS_ANALYSE_PORT", "6379"))


def get_redis() -> redis.Redis:
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def get_redis_analyse() -> redis.Redis:
    """Separate client for the analyse-DB (where Early Pump writes its
    detections).  In Docker Compose this is the `redis-analyse` service."""
    return redis.Redis(host=REDIS_ANALYSE_HOST, port=REDIS_ANALYSE_PORT, decode_responses=True)


def get_early_pump_watchlist(r_an: redis.Redis, max_age_sec: int,
                              min_score: int) -> Dict[str, int]:
    """Pull EARLY_PUMP:LATEST (per-symbol latest detection) and filter.

    Returns ``{symbol: score}`` for detections that are ≥ ``min_score`` AND
    no older than ``max_age_sec``.  Used to gate / boost PumpRider buys.
    """
    out: Dict[str, int] = {}
    try:
        raw = r_an.hgetall("EARLY_PUMP:LATEST") or {}
    except Exception as exc:
        log.warning(f"EARLY_PUMP:LATEST read failed: {exc}")
        return out
    now_ms = time.time() * 1000
    cutoff_ms = now_ms - max_age_sec * 1000
    for sym, raw_val in raw.items():
        try:
            ev = json.loads(raw_val)
            score = int(ev.get("score") or 0)
            ts = int(ev.get("ts") or 0)
        except Exception:
            continue
        if score < min_score or ts < cutoff_ms:
            continue
        out[sym] = score
    return out


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
    """Pick the N most-active USDT symbols.

    Uses the FAST_MOVE hash (written by Fast Move Analyzer) — already
    ranked by momentum.  Falls back to FAST_MOVE_TOP5 + CURRENT_PRICE
    keys if FAST_MOVE is empty.
    """
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
        if scored:
            return [s for _, s in scored[:top_n]]
    except Exception as e:
        log.warning(f"FAST_MOVE read failed: {e}")
    # Fallback — use any CURRENT_PRICE keys (less targeted, but works).
    try:
        keys = list(r.hkeys("CURRENT_PRICE") or [])
        return [k for k in keys if k.endswith("USDT")][:top_n]
    except Exception:
        return []


def fetch_klines(symbol: str, limit: int = 25) -> Optional[List[List]]:
    """Last `limit` 1m klines for symbol.  Returns the raw Binance shape."""
    try:
        r = requests.get(
            f"{BINANCE_BASE}/api/v3/klines",
            params={"symbol": symbol, "interval": "1m", "limit": limit},
            timeout=5,
        )
        if r.status_code != 200:
            return None
        return r.json()
    except Exception as exc:
        log.debug(f"klines fetch failed for {symbol}: {exc}")
        return None


def fetch_24h_ticker(symbol: str) -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(
            f"{BINANCE_BASE}/api/v3/ticker/24hr",
            params={"symbol": symbol},
            timeout=5,
        )
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


def evaluate_symbol(symbol: str, cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return a detection event dict if the entry rules fire, else None."""
    ks = fetch_klines(symbol, limit=25)
    if not ks or len(ks) < 22:
        return None

    # klines[-1] is the in-progress candle.  Use [-2] (most-recently-closed)
    # as the trigger candle, and [-3] as the prior warm-up candle.
    trigger = ks[-2]
    prior = ks[-3]

    try:
        o = float(trigger[1]); h = float(trigger[2]); l = float(trigger[3])
        c = float(trigger[4]); qv = float(trigger[7])
        po = float(prior[1]); pc = float(prior[4]); pqv = float(prior[7])
    except (ValueError, IndexError, TypeError):
        return None

    if o <= 0 or c <= 0 or qv <= 0:
        return None

    # Baseline = 20 candles before the prior one (don't include the warm-up
    # in the baseline or it falsely raises the bar).
    baseline_window = ks[-23:-3]
    if len(baseline_window) < 15:
        return None
    try:
        baseline_qv = mean(float(k[7]) for k in baseline_window)
    except Exception:
        return None
    if baseline_qv <= 0:
        return None

    vol_mult = qv / baseline_qv
    prior_vol_mult = pqv / baseline_qv if baseline_qv > 0 else 0
    price_change_pct = (c - o) / o * 100

    # ── Entry rules ────────────────────────────────────────────────
    reasons = []
    if vol_mult < cfg["pumpRiderVolMultipleThreshold"]:
        reasons.append(f"vol_mult={vol_mult:.2f}<{cfg['pumpRiderVolMultipleThreshold']}")
    if price_change_pct < cfg["pumpRiderMinPriceChangePct"]:
        reasons.append(f"chg_pct={price_change_pct:.2f}<{cfg['pumpRiderMinPriceChangePct']}")
    if prior_vol_mult < cfg["pumpRiderMinPriorVolMultiple"]:
        reasons.append(f"prior_vol={prior_vol_mult:.2f}<{cfg['pumpRiderMinPriorVolMultiple']}")
    if c <= o:
        reasons.append("not_green_candle")

    # Cumulative gain check — if we're already +N% from `lookback` candles
    # ago, the move is mature and likely about to exhaust.
    look = int(cfg["pumpRiderMaxLookbackCandles"])
    if len(ks) >= look + 2:
        lookback_open = float(ks[-(look + 2)][1])
        if lookback_open > 0:
            cum_gain = (c - lookback_open) / lookback_open * 100
            if cum_gain > cfg["pumpRiderMaxCumulativeGainPct"]:
                reasons.append(f"already_pumped_{cum_gain:.1f}%")

    if reasons:
        return None  # didn't fire

    return {
        "symbol": symbol,
        "ts": int(time.time() * 1000),
        "trigger_open": o,
        "trigger_close": c,
        "trigger_high": h,
        "price_change_pct": round(price_change_pct, 3),
        "vol_mult": round(vol_mult, 2),
        "prior_vol_mult": round(prior_vol_mult, 2),
        "baseline_qv_usdt": round(baseline_qv, 0),
        "trigger_qv_usdt": round(qv, 0),
    }


def already_in_position(r: redis.Redis, symbol: str) -> bool:
    """Skip if the bot already holds (or is filling) a buy on this symbol."""
    try:
        raw = r.hget("BUY", symbol)
        if not raw:
            return False
        row = json.loads(raw)
        if not row:
            return False
        # NEW = pending buy, FILLED = active position
        if row.get("status") in ("NEW", "FILLED") and float(row.get("executedQty") or 0) >= 0:
            return True
    except Exception:
        pass
    return False


def under_cooldown(r: redis.Redis, symbol: str) -> Optional[int]:
    """Returns seconds remaining if any cooldown is active for this symbol."""
    keys = [
        f"PUMP_RIDER:COOLDOWN:{symbol}",
        f"RULES_COOLDOWN:{symbol}",  # iter52 — set after any R1/R2/R3 sell
    ]
    for k in keys:
        try:
            ttl = r.ttl(k)
            if ttl and ttl > 0:
                return ttl
        except Exception:
            continue
    return None


def delegate_buy(symbol: str, event: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    """POST to the python backend's pattern-buy endpoint."""
    url = (
        f"{BACKEND_BASE}/api/v1/order/pattern-buy/{symbol}"
        f"?sell_pct={cfg['pumpRiderSellPctLabel']}&rule_label=PUMP_RIDER"
    )
    try:
        r = requests.post(url, timeout=4)
        if r.status_code != 200:
            log.warning(
                f"[PUMP_RIDER] delegate {symbol} HTTP {r.status_code}: "
                f"{r.text[:200]}"
            )
            return False
        log.info(
            f"[PUMP_RIDER] 🚀 LIVE delegate {symbol} "
            f"vol={event['vol_mult']}x chg={event['price_change_pct']}% "
            f"trigger_close={event['trigger_close']}"
        )
        return True
    except Exception as exc:
        log.error(f"[PUMP_RIDER] delegate {symbol} error: {exc}")
        return False


def publish_detection(r: redis.Redis, event: Dict[str, Any]) -> None:
    """Append to PUMP_RIDER:DETECTIONS:<date> for visibility/analytics."""
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"PUMP_RIDER:DETECTIONS:{date}"
    try:
        r.lpush(key, json.dumps(event))
        r.ltrim(key, 0, 999)
        r.expire(key, 14 * 24 * 60 * 60)  # 14-day retention
    except Exception as exc:
        log.warning(f"publish_detection failed: {exc}")


def set_cooldown(r: redis.Redis, symbol: str, ttl_s: int) -> None:
    try:
        r.setex(f"PUMP_RIDER:COOLDOWN:{symbol}", int(ttl_s), "1")
    except Exception:
        pass


def update_status(r: redis.Redis, fields: Dict[str, Any]) -> None:
    try:
        r.hset("PUMP_RIDER:STATUS", mapping={k: str(v) for k, v in fields.items()})
    except Exception:
        pass


def cycle(r: redis.Redis, r_an: redis.Redis) -> None:
    cfg = load_config(r)
    if not cfg["pumpRiderEnabled"]:
        update_status(r, {"last_poll_ts": int(time.time() * 1000), "enabled": 0})
        return

    symbols = pick_symbols(r, int(cfg["pumpRiderTopSymbols"]))
    if not symbols:
        log.warning("[PUMP_RIDER] no symbols available from FAST_MOVE/CURRENT_PRICE")
        update_status(r, {"last_poll_ts": int(time.time() * 1000), "scanned": 0})
        return

    # iter 56 — Early-Pump watchlist intersection.
    mode = str(cfg.get("pumpRiderWatchlistMode", "off") or "off").lower()
    watchlist: Dict[str, int] = {}
    if mode in ("prefer", "require"):
        watchlist = get_early_pump_watchlist(
            r_an,
            int(cfg["pumpRiderWatchlistMaxAgeSec"]),
            int(cfg["pumpRiderWatchlistScoreMin"]),
        )

    if mode == "require":
        # Hard intersection: only consider coins that are BOTH on PumpRider's
        # scan list AND on the Early-Pump watchlist.
        symbols = [s for s in symbols if s in watchlist]
        # Also add EP-only coins (they won't be in FAST_MOVE if they're
        # still drifting flat).  Cap to keep API budget reasonable.
        for s in watchlist:
            if s not in symbols and len(symbols) < int(cfg["pumpRiderTopSymbols"]) * 2:
                symbols.append(s)
    elif mode == "prefer":
        # Re-rank: watchlisted symbols first, then everything else.
        watch_first = [s for s in symbols if s in watchlist]
        rest = [s for s in symbols if s not in watchlist]
        symbols = watch_first + rest

    fired = 0
    skipped_cooldown = 0
    skipped_inpos = 0
    failed_24h = 0
    watchlist_hits = 0

    for sym in symbols:
        # Fast-path skips (no Binance call needed)
        if under_cooldown(r, sym):
            skipped_cooldown += 1
            continue
        if already_in_position(r, sym):
            skipped_inpos += 1
            continue

        ev = evaluate_symbol(sym, cfg)
        if not ev:
            continue

        # Watchlist enrichment + (in require mode) gating handled above
        if sym in watchlist:
            ev["early_pump_score"] = watchlist[sym]
            watchlist_hits += 1

        # Final 24h-vol liquidity floor (one extra HTTP call only on hits)
        ticker = fetch_24h_ticker(sym)
        if not ticker:
            failed_24h += 1
            continue
        try:
            qv_24h = float(ticker.get("quoteVolume") or 0)
        except Exception:
            qv_24h = 0
        if qv_24h < float(cfg["pumpRiderMinVol24hUsd"]):
            continue
        ev["vol_24h_usd"] = round(qv_24h, 0)
        ev["watchlist_mode"] = mode

        publish_detection(r, ev)
        ok = delegate_buy(sym, ev, cfg)
        if ok:
            set_cooldown(r, sym, int(cfg["pumpRiderCooldownSec"]))
            fired += 1
        # Be polite to Binance.
        time.sleep(0.2)

    update_status(r, {
        "last_poll_ts": int(time.time() * 1000),
        "scanned": len(symbols),
        "watchlist_mode": mode,
        "watchlist_size": len(watchlist),
        "watchlist_hits": watchlist_hits,
        "fired": fired,
        "skipped_cooldown": skipped_cooldown,
        "skipped_inposition": skipped_inpos,
        "failed_24h_lookup": failed_24h,
        "enabled": 1,
    })


def main() -> None:
    r = get_redis()
    r_an = get_redis_analyse()
    try:
        r.ping()
    except Exception as exc:
        log.error(f"redis ping failed: {exc}")
        sys.exit(1)
    try:
        r_an.ping()
        log.info(
            f"[PUMP_RIDER] analyse Redis connected ({REDIS_ANALYSE_HOST}:{REDIS_ANALYSE_PORT})"
        )
    except Exception as exc:
        log.warning(
            f"analyse-Redis ping failed: {exc} — watchlist mode will degrade to 'off'"
        )
    log.info(
        f"[PUMP_RIDER] starting — poll={POLL_INTERVAL_S}s backend={BACKEND_BASE}"
    )
    while True:
        t0 = time.time()
        try:
            cycle(r, r_an)
        except Exception as exc:
            log.error(f"cycle error: {exc}", exc_info=True)
        elapsed = time.time() - t0
        sleep_for = max(1.0, POLL_INTERVAL_S - elapsed)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
