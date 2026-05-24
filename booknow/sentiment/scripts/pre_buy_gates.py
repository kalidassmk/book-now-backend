"""
pre_buy_gates.py — iter 74 (2026-05-24)
─────────────────────────────────────────────────────────────────────────────
Shared pre-buy filter gates used by Fast Scalper + Virtual Scalper.

Both scalpers historically bypassed the safety pipeline that R1/R2/R3,
Pattern Bot, PumpRider, EP autobuy, VSP, LMC, CCP all use through
try_buy. This module exposes 3 SYNCHRONOUS helpers each scalper can
call right before placing the order:

  1. usdt_cooldown_active(redis_client)
     Checks USDT_INSUFFICIENT_COOLDOWN key (iter 67).  Returns the
     blocker reason string or None.

  2. check_coin_blocked(symbol, cfg, *, dashboard_url=None, timeout_s=2)
     Calls /api/check-coin.  Runs the full filter pipeline:
       • iter71 weak-pump   (Price↑ + Volume↓)
       • iter48 falling-knife
       • iter38 near-top
       • iter44 macro-top
       • iter45 vol-regime
       • post-pump bleed
     Returns blocker reason or None.

  3. orderbook_depth_blocked(symbol, leg_size_usdt, cfg, *, timeout_s=2)
     iter 66 — checks Binance top-20 bids+asks within 0.5% of mid,
     rejects if spread > 0.5% or either side depth < 3× leg.
     Returns blocker reason or None.

All helpers fail-OPEN on network/parse errors (return None) so a
Binance/frontend hiccup doesn't block legitimate trading.  Each can
be disabled individually via TRADING_CONFIG keys:
  useCheckCoinFilterEnabled, orderbookDepthCheckEnabled.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional

import redis as _redis_lib
import requests

log = logging.getLogger(__name__)

# Dashboard URL for /api/check-coin.  In Docker Compose the frontend
# resolves to http://frontend:3000.  Override via env if needed.
_DASHBOARD_URL = os.getenv("BOOKNOW_DASHBOARD_URL", "http://frontend:3000").rstrip("/")
_BINANCE_BASE = "https://api.binance.com"


# ──────────────────────────────────────────────────────────────────────
# 1. USDT cooldown (iter 67)
# ──────────────────────────────────────────────────────────────────────

def usdt_cooldown_active(redis_client: _redis_lib.Redis) -> Optional[str]:
    """Return blocker reason if USDT_INSUFFICIENT_COOLDOWN is active."""
    try:
        ttl = redis_client.ttl("USDT_INSUFFICIENT_COOLDOWN")
        if ttl and int(ttl) > 0:
            return f"usdt_insufficient_cooldown active ({int(ttl)}s left)"
    except Exception:
        pass
    return None


# ──────────────────────────────────────────────────────────────────────
# 2. /api/check-coin pipeline (iter 71 + iter 48 + ...)
# ──────────────────────────────────────────────────────────────────────

def check_coin_blocked(
    symbol: str,
    cfg: Dict[str, Any],
    *,
    dashboard_url: Optional[str] = None,
    timeout_s: float = 2.0,
) -> Optional[str]:
    """Call /api/check-coin and return blocker reason string if any
    filter rejected, else None.

    Disabled via `useCheckCoinFilterEnabled=False`.  Fail-OPEN on
    network errors unless `checkCoinFailClosed=True`.
    """
    if not cfg.get("useCheckCoinFilterEnabled", True):
        return None
    base = (dashboard_url or _DASHBOARD_URL).rstrip("/")
    url = f"{base}/api/check-coin"
    fail_closed = bool(cfg.get("checkCoinFailClosed", False))
    try:
        r = requests.get(url, params={"symbol": symbol}, timeout=timeout_s)
        if r.status_code != 200:
            return f"check-coin HTTP {r.status_code}" if fail_closed else None
        data = r.json()
        verdict = data.get("verdict") or {}
        if verdict.get("blocked"):
            blocker = verdict.get("blocker") or "unknown"
            reason = verdict.get("blocker_reason") or ""
            return f"{blocker}: {reason}" if reason else blocker
        return None
    except requests.Timeout:
        return "check-coin timeout" if fail_closed else None
    except Exception as e:
        return f"check-coin error: {e}" if fail_closed else None


# ──────────────────────────────────────────────────────────────────────
# 3. Orderbook depth check (iter 66)
# ──────────────────────────────────────────────────────────────────────

def orderbook_depth_blocked(
    symbol: str,
    leg_size_usdt: float,
    cfg: Dict[str, Any],
    *,
    timeout_s: float = 2.0,
) -> Optional[str]:
    """Verify top-20 bids+asks have enough depth within X% of mid.

    Rejects if EITHER the spread > pct or bid/ask depth < multiplier × leg.
    Disabled via `orderbookDepthCheckEnabled=False`.  Fail-OPEN on
    network errors (never block on Binance hiccup).
    """
    if not cfg.get("orderbookDepthCheckEnabled", True):
        return None
    if leg_size_usdt <= 0:
        return None

    mult = float(cfg.get("orderbookDepthMultiplier", 3.0))
    pct  = float(cfg.get("orderbookDepthPctOfPrice", 0.5))
    tmo  = float(cfg.get("orderbookDepthTimeoutMs", 2000)) / 1000.0
    timeout_s = min(timeout_s, tmo)
    required = mult * leg_size_usdt

    try:
        r = requests.get(
            f"{_BINANCE_BASE}/api/v3/depth",
            params={"symbol": symbol, "limit": 20},
            timeout=timeout_s,
        )
        if r.status_code != 200:
            return None  # fail-open
        data = r.json()
        bids = data.get("bids") or []
        asks = data.get("asks") or []
        if not bids or not asks:
            return None
        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
        mid = (best_bid + best_ask) / 2.0
        if mid <= 0:
            return None
        bid_floor = mid * (1.0 - pct / 100.0)
        ask_ceil  = mid * (1.0 + pct / 100.0)
        bid_depth = sum(float(p) * float(q) for p, q in bids if float(p) >= bid_floor)
        ask_depth = sum(float(p) * float(q) for p, q in asks if float(p) <= ask_ceil)
        spread_pct = (best_ask - best_bid) / mid * 100.0 if mid > 0 else 0
        if spread_pct > pct:
            return (
                f"thin_orderbook: spread {spread_pct:.2f}% > {pct}% "
                f"(bid={best_bid:.8f} ask={best_ask:.8f})"
            )
        if bid_depth < required:
            return (
                f"thin_orderbook: bid_depth ${bid_depth:.0f} < required "
                f"${required:.0f} ({mult}× leg) within {pct}% of mid"
            )
        if ask_depth < required:
            return (
                f"thin_orderbook: ask_depth ${ask_depth:.0f} < required "
                f"${required:.0f} ({mult}× leg) within {pct}% of mid"
            )
        return None
    except requests.Timeout:
        return None
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────────────
# Combined helper — runs all 3 in order, returns first blocker
# ──────────────────────────────────────────────────────────────────────

def run_all_gates(
    symbol: str,
    leg_size_usdt: float,
    cfg: Dict[str, Any],
    redis_client: _redis_lib.Redis,
    *,
    dashboard_url: Optional[str] = None,
    timeout_s: float = 2.0,
) -> Optional[str]:
    """Convenience wrapper: runs all 3 gates and returns first blocker.

    Order: USDT cooldown → check-coin → orderbook depth.
    USDT first because it's a cheap Redis check.
    """
    block = usdt_cooldown_active(redis_client)
    if block:
        return block
    block = check_coin_blocked(symbol, cfg, dashboard_url=dashboard_url, timeout_s=timeout_s)
    if block:
        return block
    block = orderbook_depth_blocked(symbol, leg_size_usdt, cfg, timeout_s=timeout_s)
    if block:
        return block
    return None
