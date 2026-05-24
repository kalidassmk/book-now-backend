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
    """Pick up to N USDT symbols to scan.

    iter 61 (2026-05-23) — COS pumped from $0.001150 at 08:25 UTC.
    Old logic returned only the top 50 FAST_MOVE symbols.  COS was
    quiet before 08:25 so it wasn't in FAST_MOVE → never scanned →
    we missed the pump start by 3.5h.  New logic:
      1. take FAST_MOVE ranking first (highest priority)
      2. UNION with every USDT key in CURRENT_PRICE (~400 symbols)
      3. de-dupe, cap at top_n (default 200)
    """
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
            if len(out) >= top_n: return out
    except Exception as e:
        log.warning(f"FAST_MOVE read failed: {e}")
    # Top-up with CURRENT_PRICE keys (all WS-tracked USDT pairs)
    try:
        keys = list(r.hkeys("CURRENT_PRICE") or [])
        for k in keys:
            if k.endswith("USDT") and k not in seen:
                seen.add(k); out.append(k)
                if len(out) >= top_n: return out
    except Exception:
        pass
    return out


def fetch_klines(symbol: str, limit: int = 32) -> Optional[List[List]]:
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
    """iter 61 — multi-window tiered pump detection.

    Tiers:
      EARLY   — alert only.  Caught early; manual confirmation desired.
      NORMAL  — buy.  Standard pump start (vol surge + meaningful move).
      STRONG  — buy.  High-confidence (big vol surge + big 5m move).
      MEGA    — alert only.  Already pumped too much — chasing the top.

    Replay COS 08:25 UTC:
      chg_1m=+0.61%  chg_5m=+1.14%  vol_surge_5m=4.95x
      → NORMAL (5m vol ≥ 3x AND 5m chg ≥ 1.0%)
      → BUY at $0.001154 ✓
    """
    # iter 62 — fetch 60 1m candles (= 1h) so we can compute chg_1h
    ks = fetch_klines(symbol, limit=62)
    if not ks or len(ks) < 32:
        return None

    # ks[-1] is the in-progress candle; use ks[:-1] for closed-only.
    closed = ks[:-1]
    try:
        opens  = [float(k[1]) for k in closed]
        highs  = [float(k[2]) for k in closed]  # iter 65 — for resistance break
        closes = [float(k[4]) for k in closed]
        qvols  = [float(k[7]) for k in closed]
    except (ValueError, IndexError, TypeError):
        return None
    if len(closes) < 30 or opens[-1] <= 0 or closes[-1] <= 0:
        return None

    c = closes[-1]; o = opens[-1]

    # ── Multi-window % changes ─────────────────────────────────────
    chg_1m  = (c - o) / o * 100 if o > 0 else 0
    chg_5m  = (c - closes[-6])  / closes[-6]  * 100 if closes[-6]  > 0 else 0
    chg_30m = (c - closes[-30]) / closes[-30] * 100 if closes[-30] > 0 else 0
    # iter 62 — chg_1h for slow steady pumps (SUPER/EIGEN/ONDO/WLD style)
    chg_1h  = (c - closes[-60]) / closes[-60] * 100 if len(closes) >= 60 and closes[-60] > 0 else 0

    # ── Volume surges (two windows) ────────────────────────────────
    qv_5m_avg  = sum(qvols[-5:]) / 5
    qv_25m_prior = sum(qvols[-30:-5]) / 25 if len(qvols) >= 30 else 0
    vol_surge_5m = qv_5m_avg / qv_25m_prior if qv_25m_prior > 0 else 0
    qv_1m = qvols[-1]
    qv_25m_base = sum(qvols[-26:-1]) / 25 if len(qvols) >= 26 else 0
    vol_surge_1m = qv_1m / qv_25m_base if qv_25m_base > 0 else 0

    # Cumulative 10m gain (top-chase guard)
    cum_chg_10m = (c - opens[-10]) / opens[-10] * 100 if opens[-10] > 0 else 0

    # ── Tier classification (thresholds from config) ───────────────
    mega_5m   = float(cfg.get("pumpRiderMega5mPct", 5.0))
    mega_1m   = float(cfg.get("pumpRiderMega1mPct", 3.0))
    strong_v  = float(cfg.get("pumpRiderStrongVolMult", 5.0))
    strong_c5 = float(cfg.get("pumpRiderStrongChg5mPct", 1.5))
    normal_v5 = float(cfg.get("pumpRiderNormal5mVolMult", 3.0))
    normal_c5 = float(cfg.get("pumpRiderNormal5mChgPct", 1.0))
    normal_v1 = float(cfg["pumpRiderVolMultipleThreshold"])
    normal_c1 = float(cfg["pumpRiderMinPriceChangePct"])
    early_v5  = float(cfg.get("pumpRiderEarly5mVolMult", 2.0))
    early_c5  = float(cfg.get("pumpRiderEarly5mChgPct", 0.5))
    max_cum   = float(cfg["pumpRiderMaxCumulativeGainPct"])

    tier = None
    if chg_5m >= mega_5m or chg_1m >= mega_1m:
        tier = "MEGA"
    elif (vol_surge_5m >= strong_v and chg_5m >= strong_c5) or \
         (vol_surge_1m >= strong_v and chg_1m >= strong_c5):
        tier = "STRONG"
    elif (vol_surge_5m >= normal_v5 and chg_5m >= normal_c5):
        tier = "NORMAL"
    elif (vol_surge_1m >= normal_v1 and chg_1m >= normal_c1 and c > o):
        tier = "NORMAL"
    # iter 62 — slow steady pump (catches SUPER/EIGEN/ONDO/WLD style)
    # iter 64 — require mild vol confirmation so price-only drift is rejected.
    # Pure 1h price grind without vol behind it is not a real pump; demand at
    # least vol_surge_5m >= pumpRiderSlow1hVolMult (default 1.5x).
    elif (chg_1h >= float(cfg.get("pumpRiderSlow1hChgPct", 2.0))
          and vol_surge_5m >= float(cfg.get("pumpRiderSlow1hVolMult", 1.5))):
        tier = "NORMAL"
    elif (vol_surge_5m >= early_v5 and chg_5m >= early_c5):
        tier = "EARLY"
    elif chg_30m >= 3.0 and vol_surge_5m >= 1.5:
        tier = "EARLY"

    if tier is None:
        return None

    # ── iter 65 — Resistance-break gate for NORMAL/STRONG ──────────────
    # Require the trigger close to break (or be within tolerance of) the
    # prior-N-bar high. Without this, NORMAL fires on any +1% / 3× vol
    # pop inside a tight range that promptly fades. With it, we only buy
    # when the move is actually clearing recent structure.
    #
    # Tolerance: 0.2% — allows for buying just below the high (when the
    # break is imminent but the printed close hasn't quite cleared yet).
    # Lookback: 60 bars = prior 1h.
    # Applies to NORMAL/STRONG only (EARLY/MEGA are alert-only).
    resistance_ok = True
    prior_high = 0.0
    if (tier in ("STRONG", "NORMAL")
            and bool(cfg.get("pumpRiderResistanceBreakEnabled", True))):
        lookback = int(cfg.get("pumpRiderResistanceLookbackBars", 60))
        tol_pct  = float(cfg.get("pumpRiderResistanceTolerancePct", 0.2))
        # Exclude the current (trigger) bar from the high search.
        window = highs[:-1][-lookback:] if len(highs) > 1 else []
        if window:
            prior_high = max(window)
            threshold = prior_high * (1.0 - tol_pct / 100.0)
            resistance_ok = c >= threshold
    if not resistance_ok:
        # Don't fire NORMAL/STRONG; downgrade to EARLY so the signal still
        # publishes for monitoring but no auto-buy is triggered.
        tier = "EARLY"

    # Top-chase guard — already pumped too hard → downgrade buys to MEGA.
    if tier in ("STRONG", "NORMAL") and cum_chg_10m > max_cum:
        tier = "MEGA"

    return {
        "symbol": symbol,
        "ts": int(time.time() * 1000),
        "tier": tier,
        "trigger_close": c,
        "trigger_open": o,
        "chg_1m":  round(chg_1m, 3),
        "chg_5m":  round(chg_5m, 3),
        "chg_30m": round(chg_30m, 3),
        "chg_1h":  round(chg_1h, 3),
        "vol_surge_1m": round(vol_surge_1m, 2),
        "vol_surge_5m": round(vol_surge_5m, 2),
        "cum_chg_10m":  round(cum_chg_10m, 2),
        # iter 65 — resistance-break diagnostics
        "prior_1h_high": round(prior_high, 8) if prior_high else 0,
        "resistance_ok": bool(resistance_ok),
        # Legacy fields for compatibility with existing publish/log code.
        "price_change_pct": round(chg_1m, 3),
        "vol_mult": round(vol_surge_1m, 2),
        "baseline_qv_usdt": round(qv_25m_base, 0),
        "trigger_qv_usdt":  round(qv_1m, 0),
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

    # iter 62 — EARLY_PUMP auto-buy: scan EARLY_PUMP:LATEST for any
    # symbol with a HIGH score detected RECENTLY, and delegate to
    # try_buy.  Acts as a parallel pump-mode trigger for "predictive"
    # signals that PumpRider's chg_5m/chg_30m won't catch.
    ep_autobuy_min_score = int(cfg.get("earlyPumpAutoBuyScore", 85))
    ep_autobuy_max_age_s = int(cfg.get("earlyPumpAutoBuyMaxAgeSec", 300))  # 5min
    ep_autobuys = 0
    if ep_autobuy_min_score > 0:
        try:
            raw = r_an.hgetall("EARLY_PUMP:LATEST") or {}
            now_ms_ = int(time.time() * 1000)
            cutoff_ms = now_ms_ - ep_autobuy_max_age_s * 1000
            ep_candidates = []
            for sym, raw_val in raw.items():
                try:
                    ev = json.loads(raw_val)
                    score = int(ev.get("score") or 0)
                    ts = int(ev.get("ts") or 0)
                    if score < ep_autobuy_min_score or ts < cutoff_ms:
                        continue
                    if under_cooldown(r, sym):
                        continue
                    if already_in_position(r, sym):
                        continue
                    ep_candidates.append((score, sym, ev))
                except Exception:
                    continue
            ep_candidates.sort(reverse=True)
            for score, sym, ev in ep_candidates[:3]:  # cap at 3 per cycle
                ep_event = {
                    "symbol": sym,
                    "ts": int(time.time() * 1000),
                    "tier": "EP_HIGH",  # special tier label for EP auto-buy
                    "trigger_close": ev.get("last_price"),
                    "early_pump_score": score,
                    "ep_change_24h_pct": ev.get("change_24h_pct"),
                    "source": "early_pump_autobuy",
                }
                publish_detection(r, ep_event)
                ok = delegate_buy(sym, ep_event, cfg)
                if ok:
                    set_cooldown(r, sym, int(cfg["pumpRiderCooldownSec"]))
                    ep_autobuys += 1
                log.info(
                    f"[PUMP_RIDER] EP_HIGH {sym} score={score} "
                    f"24h={ev.get('change_24h_pct')}% ok={ok}"
                )
        except Exception as exc:
            log.warning(f"EARLY_PUMP autobuy failed: {exc}")

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

        tier = str(ev.get("tier") or "").upper()
        # iter 61 — publish EVERY tier (EARLY/NORMAL/STRONG/MEGA) so the
        # dashboard alert banner sees it.  Only delegate to try_buy for
        # the actionable tiers.
        publish_detection(r, ev)

        if tier in ("NORMAL", "STRONG"):
            ok = delegate_buy(sym, ev, cfg)
            if ok:
                set_cooldown(r, sym, int(cfg["pumpRiderCooldownSec"]))
                fired += 1
            log.info(
                f"[PUMP_RIDER] {tier} BUY {sym} chg_5m={ev.get('chg_5m')}% "
                f"vol_5m={ev.get('vol_surge_5m')}x ok={ok}"
            )
        elif tier == "EARLY":
            # alert only — no buy.  Short cooldown so we don't spam the
            # banner with the same coin every cycle.
            set_cooldown(r, sym, 60)
            log.info(
                f"[PUMP_RIDER] EARLY {sym} chg_5m={ev.get('chg_5m')}% "
                f"vol_5m={ev.get('vol_surge_5m')}x (alert only)"
            )
        elif tier == "MEGA":
            # already pumped — alert + 5min cooldown so we don't re-alert
            # constantly while the coin sits at the top.
            set_cooldown(r, sym, 300)
            log.info(
                f"[PUMP_RIDER] MEGA {sym} chg_5m={ev.get('chg_5m')}% "
                f"cum_chg_10m={ev.get('cum_chg_10m')}% (too late, alert only)"
            )

        # Be polite to Binance.
        time.sleep(0.2)

    update_status(r, {
        "last_poll_ts": int(time.time() * 1000),
        "scanned": len(symbols),
        "watchlist_mode": mode,
        "watchlist_size": len(watchlist),
        "watchlist_hits": watchlist_hits,
        "fired": fired,
        "ep_autobuys": ep_autobuys,
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
