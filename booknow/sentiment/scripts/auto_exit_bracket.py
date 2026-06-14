#!/usr/bin/env python3
"""
auto_exit_bracket.py — iter168 (2026-06-14)
─────────────────────────────────────────────────────────────────────────────
Auto Exit Bracket ("away-mode protector") — PHASE 1: DRY-RUN ONLY.

Motivation — the operator places a limit BUY and then walks away from the
computer.  When that buy fills they have a coin sitting on Binance with NO
sell order attached, fully unprotected.  They asked for an automatic
"trace algorithm" that, while they're away, attaches a +2% take-profit AND
a -2% stop-loss to every bought coin that has no sell order yet, so a win
is locked at +2% and a loss is capped at -2% without them having to be at
the screen.

What this subprocess does (DRY-RUN):
  1. Every `autoExitPollSec` seconds reads the operator's live holdings from
     Redis (`BINANCE:BALANCES:ALL`, kept fresh by the balance worker).
  2. Reads all open orders (`BINANCE:OPEN_ORDERS:ALL`).  A coin that already
     has an open SELL order is considered protected → skipped (idempotent).
  3. For every non-stablecoin, non-BNB holding worth >= `autoExitMinUsd` that
     has NO open SELL order, it works out the coin's BUY COST BASIS:
       a. the `BUY` Redis hash (dashboard/limit buys land here with their
          `buyPrice`), else
       b. Binance `myTrades` via the engine's authenticated endpoint
          (weighted average of the most-recent BUY fills covering the held
          qty), else
       c. SKIP and flag NO_COST_BASIS — we NEVER guess a buy price, because a
          wrong reference price would mean a real-money loss.
  4. From the cost basis it computes the bracket:
          TP price      = cost * (1 + autoExitTpPct/100)        # +30%
          SL trigger    = cost * (1 - autoExitSlPct/100)        # -4%
          SL limit      = SL trigger * (1 - autoExitSlLimitGap) # just below
     and classifies the situation against the live price:
          NORMAL          live between SL and TP — a standard bracket
          ALREADY_ABOVE_TP live already >= TP (coin already up >2%)
          ALREADY_BELOW_SL live already <= SL trigger (coin already down >2%)
  5. PUBLISHES the intended bracket to `AUTO_EXIT:STATE` (hash, for the UI)
     and `AUTO_EXIT:SIGNALS:<date>` (history list).  It places NO real order.

IMPORTANT — this phase NEVER places, cancels or modifies a real order.
The live switch `autoExitLiveEnabled` is read but Phase-2 execution is
intentionally NOT wired here; when it is on we only log that execution is
pending.  This subprocess is deliberately INDEPENDENT of the engine's
`HARD_DISABLE_AUTOSELL` kill switch — turning this watcher on does not
re-enable any of the bot's own ladder/auto exits.  Phase 2 will place a
Binance OCO (one-cancels-other: TP LIMIT_MAKER + SL STOP_LOSS_LIMIT) so the
two legs can never both fill and oversell.

All state lives in Redis so the supervisor can restart this freely.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("AutoExitBracket")

# ── Config (Redis-backed; falls back to defaults if missing) ────────────
CONFIG_KEY = "TRADING_CONFIG"
DEFAULTS: Dict[str, Any] = {
    # master switches
    "autoExitDryRunEnabled": True,    # run the scanner + publish intended brackets (zero risk)
    "autoExitLiveEnabled": False,     # PHASE 2 — actually place OCO brackets (NOT wired yet)
    # bracket math
    "autoExitTpPct": 30.0,            # take-profit = cost * (1 + 30%)
    "autoExitSlPct": 4.0,             # stop-loss  = cost * (1 - 4%)
    "autoExitSlLimitGap": 0.001,      # SL limit sits this far below the SL trigger (0.1%)
    # eligibility
    "autoExitMinUsd": 5.0,            # ignore dust below this USDT value
    "autoExitFeeBuffer": 0.999,       # sell slightly less than free qty (fee/rounding headroom)
    # loop
    "autoExitPollSec": 15,            # how often the scan runs
    "autoExitCostBasisTtlSec": 300,   # cache a coin's derived cost basis this long
    # engine (for authenticated myTrades fallback)
    "autoExitEngineBase": "http://127.0.0.1:8083",
}

REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

BALANCES_KEY = "BINANCE:BALANCES:ALL"     # JSON array: [{asset, free, locked}, ...]
OPEN_ORDERS_KEY = "BINANCE:OPEN_ORDERS:ALL"  # JSON array of Binance order objects
CURRENT_PRICE_KEY = "CURRENT_PRICE"       # hash: symbol -> {"price": .., ...}
BUY_KEY = "BUY"                           # hash: symbol -> buy record (has buyPrice)
STATE_KEY = "AUTO_EXIT:STATE"             # hash: symbol -> latest intended-bracket JSON
STATUS_KEY = "AUTO_EXIT:STATUS"           # hash: watcher health
SIGNALS_RETENTION_S = 90 * 24 * 60 * 60   # 90-day history (matches history pages)

STABLECOINS = frozenset(
    {"USDT", "USDC", "USDP", "TUSD", "BUSD", "FDUSD", "DAI", "PYUSD"}
)
SKIP_ASSETS = frozenset({"BNB"})  # used for fees, not a trade position


def get_redis() -> redis.Redis:
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def load_config(r: redis.Redis) -> Dict[str, Any]:
    out = dict(DEFAULTS)
    try:
        raw = r.get(CONFIG_KEY)
        if raw:
            c = json.loads(raw)
            for k in DEFAULTS:
                if k in c and c[k] is not None:
                    out[k] = c[k]
    except Exception as exc:
        log.warning(f"config read failed, using defaults: {exc}")
    return out


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def _json_get(r: redis.Redis, key: str) -> Any:
    try:
        raw = r.get(key)
        return json.loads(raw) if raw else None
    except Exception as exc:
        log.debug(f"json get {key} failed: {exc}")
        return None


def read_balances(r: redis.Redis) -> List[Dict[str, Any]]:
    """Live holdings.  Prefer the array key; fall back to per-asset keys."""
    arr = _json_get(r, BALANCES_KEY)
    if isinstance(arr, list) and arr:
        return arr
    out: List[Dict[str, Any]] = []
    try:
        for key in r.keys("BINANCE:BALANCE:*"):
            row = _json_get(r, key)
            if isinstance(row, dict) and row.get("asset"):
                out.append(row)
    except Exception as exc:
        log.debug(f"per-asset balance scan failed: {exc}")
    return out


def read_sell_symbols(r: redis.Redis) -> set[str]:
    """Symbols that already have an open SELL order (any kind)."""
    out: set[str] = set()
    arr = _json_get(r, OPEN_ORDERS_KEY)
    if not isinstance(arr, list):
        return out
    for o in arr:
        try:
            if str(o.get("side", "")).upper() == "SELL":
                sym = str(o.get("symbol", "")).upper()
                if sym:
                    out.add(sym)
        except Exception:
            continue
    return out


def live_price(r: redis.Redis, symbol: str) -> Optional[float]:
    try:
        raw = r.hget(CURRENT_PRICE_KEY, symbol)
        if not raw:
            return None
        px = _safe_float(json.loads(raw).get("price"))
        return px if px > 0 else None
    except Exception:
        return None


# ── Cost-basis resolution ────────────────────────────────────────────────
class CostBasisCache:
    """Per-symbol cost-basis cache to avoid hammering myTrades."""

    def __init__(self) -> None:
        self._cache: Dict[str, Tuple[float, float, str]] = {}  # sym -> (cost, ts, source)

    def get(self, symbol: str, ttl_s: float) -> Optional[Tuple[float, str]]:
        hit = self._cache.get(symbol)
        if not hit:
            return None
        cost, ts, source = hit
        if (time.time() - ts) > ttl_s:
            return None
        return cost, source

    def put(self, symbol: str, cost: float, source: str) -> None:
        self._cache[symbol] = (cost, time.time(), source)


def cost_from_buy_hash(r: redis.Redis, symbol: str) -> Optional[float]:
    """Dashboard / limit buys persist their buy price in the BUY hash."""
    try:
        raw = r.hget(BUY_KEY, symbol)
        if not raw:
            return None
        rec = json.loads(raw)
    except Exception:
        return None
    for field in ("buyPrice", "buyingPoint", "price"):
        v = _safe_float(rec.get(field))
        if v > 0:
            return v
    return None


def cost_from_my_trades(
    engine_base: str, symbol: str, free_qty: float
) -> Optional[float]:
    """Weighted-average BUY price covering the current free qty.

    Calls the engine's authenticated trade-history endpoint (myTrades).
    Walks the most-recent BUY fills until they cover `free_qty`, then takes
    the quote-weighted average price of those fills.  This matches "what did
    I actually pay for the coins I'm still holding".
    """
    url = f"{engine_base.rstrip('/')}/api/v1/trade/trade-history?symbol={urllib.parse.quote(symbol)}"
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, OSError, ValueError) as exc:
        log.debug(f"myTrades fetch {symbol} failed: {exc}")
        return None
    if not isinstance(data, list) or not data:
        return None

    buys = [t for t in data if t.get("isBuyer") is True]
    # Most recent first.
    buys.sort(key=lambda t: _safe_float(t.get("time")), reverse=True)

    target = max(free_qty, 0.0)
    if target <= 0:
        return None
    acc_qty = 0.0
    acc_quote = 0.0
    for t in buys:
        qty = _safe_float(t.get("qty"))
        price = _safe_float(t.get("price"))
        if qty <= 0 or price <= 0:
            continue
        take = min(qty, target - acc_qty)
        if take <= 0:
            break
        acc_qty += take
        acc_quote += take * price
        if acc_qty >= target * 0.999:  # close enough (rounding/dust)
            break
    if acc_qty <= 0 or acc_quote <= 0:
        return None
    return acc_quote / acc_qty


def resolve_cost_basis(
    r: redis.Redis,
    cache: CostBasisCache,
    cfg: Dict[str, Any],
    symbol: str,
    free_qty: float,
) -> Tuple[Optional[float], str]:
    """Return (cost_basis, source). source in BUY_HASH / MY_TRADES / NONE."""
    ttl = _safe_float(cfg.get("autoExitCostBasisTtlSec"), 300)
    cached = cache.get(symbol, ttl)
    if cached:
        return cached[0], cached[1]

    cost = cost_from_buy_hash(r, symbol)
    if cost and cost > 0:
        cache.put(symbol, cost, "BUY_HASH")
        return cost, "BUY_HASH"

    cost = cost_from_my_trades(
        str(cfg.get("autoExitEngineBase") or DEFAULTS["autoExitEngineBase"]),
        symbol,
        free_qty,
    )
    if cost and cost > 0:
        cache.put(symbol, cost, "MY_TRADES")
        return cost, "MY_TRADES"

    return None, "NONE"


# ── Bracket computation ──────────────────────────────────────────────────
def build_bracket(
    cfg: Dict[str, Any],
    symbol: str,
    asset: str,
    free_qty: float,
    cost: float,
    live: float,
) -> Dict[str, Any]:
    tp_pct = _safe_float(cfg.get("autoExitTpPct"), 30.0)
    sl_pct = _safe_float(cfg.get("autoExitSlPct"), 4.0)
    sl_gap = _safe_float(cfg.get("autoExitSlLimitGap"), 0.001)
    fee_buffer = _safe_float(cfg.get("autoExitFeeBuffer"), 0.999)

    tp_price = cost * (1.0 + tp_pct / 100.0)
    sl_trigger = cost * (1.0 - sl_pct / 100.0)
    sl_limit = sl_trigger * (1.0 - sl_gap)
    sell_qty = free_qty * fee_buffer

    if live >= tp_price:
        situation = "ALREADY_ABOVE_TP"   # already up >= +2% from cost
    elif live <= sl_trigger:
        situation = "ALREADY_BELOW_SL"   # already down <= -2% from cost
    else:
        situation = "NORMAL"

    return {
        "symbol": symbol,
        "asset": asset,
        "freeQty": free_qty,
        "sellQty": sell_qty,
        "costBasis": cost,
        "livePrice": live,
        "tpPct": tp_pct,
        "slPct": sl_pct,
        "tpPrice": tp_price,
        "slTrigger": sl_trigger,
        "slLimit": sl_limit,
        "valueUsd": free_qty * live,
        "situation": situation,
    }


# ── Watcher ──────────────────────────────────────────────────────────────
class AutoExitBracket:
    def __init__(self, r: redis.Redis):
        self.r = r
        self.cache = CostBasisCache()

    def _publish(self, ev: Dict[str, Any]) -> None:
        sym = ev["symbol"]
        ev["ts"] = int(time.time() * 1000)
        payload = json.dumps(ev)
        try:
            self.r.hset(STATE_KEY, sym, payload)
        except Exception as exc:
            log.debug(f"state hset {sym} failed: {exc}")

    def _publish_signal(self, ev: Dict[str, Any]) -> None:
        """Append a transition to the dated history list (for the UI)."""
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        key = f"AUTO_EXIT:SIGNALS:{date}"
        try:
            self.r.rpush(key, json.dumps(ev))
            self.r.expire(key, SIGNALS_RETENTION_S)
        except Exception as exc:
            log.debug(f"signal rpush failed: {exc}")

    def scan_once(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        min_usd = _safe_float(cfg.get("autoExitMinUsd"), 5.0)
        live_enabled = bool(cfg.get("autoExitLiveEnabled", False))

        balances = read_balances(self.r)
        sell_syms = read_sell_symbols(self.r)

        seen: set[str] = set()
        n_eligible = 0
        n_no_cost = 0
        n_protected = 0

        for row in balances:
            try:
                asset = str(row.get("asset") or "").upper()
                free = _safe_float(row.get("free"))
                if not asset or asset in STABLECOINS or asset in SKIP_ASSETS:
                    continue
                if free <= 0:
                    continue

                symbol = f"{asset}USDT"
                live = live_price(self.r, symbol)
                if live is None:
                    continue  # no tradeable USDT pair / no live price

                value_usd = free * live
                if value_usd < min_usd:
                    continue  # dust

                seen.add(symbol)

                # Already protected → record state, no bracket needed.
                if symbol in sell_syms:
                    n_protected += 1
                    self._publish({
                        "symbol": symbol,
                        "asset": asset,
                        "freeQty": free,
                        "livePrice": live,
                        "valueUsd": value_usd,
                        "situation": "HAS_SELL_ORDER",
                        "action": "SKIP_PROTECTED",
                        "mode": "DRY_RUN",
                    })
                    continue

                cost, source = resolve_cost_basis(
                    self.r, self.cache, cfg, symbol, free,
                )
                if cost is None:
                    n_no_cost += 1
                    self._publish({
                        "symbol": symbol,
                        "asset": asset,
                        "freeQty": free,
                        "livePrice": live,
                        "valueUsd": value_usd,
                        "situation": "NO_COST_BASIS",
                        "action": "SKIP_NO_COST",
                        "mode": "DRY_RUN",
                        "note": "buy price unknown — never guessed; sell manually or place a buy via the dashboard so cost is recorded",
                    })
                    continue

                n_eligible += 1
                bracket = build_bracket(cfg, symbol, asset, free, cost, live)
                bracket["costSource"] = source
                bracket["mode"] = "LIVE_PENDING" if live_enabled else "DRY_RUN"
                bracket["action"] = (
                    "WOULD_PLACE_OCO" if not live_enabled else "LIVE_NOT_WIRED"
                )
                self._publish(bracket)
                self._publish_signal(bracket)

                if live_enabled:
                    # PHASE 2 is intentionally not wired.  We do NOT place a
                    # real order here — flipping the live flag must not start
                    # trading until Phase 2 (Binance OCO placement via the
                    # engine) is reviewed and shipped.
                    log.warning(
                        "[%s] autoExitLiveEnabled=True but Phase-2 OCO "
                        "placement is NOT wired — would place TP=%.8f / "
                        "SL=%.8f (cost=%.8f via %s). No order sent.",
                        symbol, bracket["tpPrice"], bracket["slTrigger"],
                        cost, source,
                    )
                else:
                    log.info(
                        "[%s] DRY-RUN bracket: cost=%.8f (%s) → TP=%.8f "
                        "(+%.1f%%) SL=%.8f (-%.1f%%) qty=%.6f [%s]",
                        symbol, cost, source, bracket["tpPrice"],
                        bracket["tpPct"], bracket["slTrigger"],
                        bracket["slPct"], bracket["sellQty"],
                        bracket["situation"],
                    )
            except Exception as exc:
                log.warning(f"row error: {exc}")

        # Drop stale STATE entries for coins no longer held / now protected.
        try:
            for sym in list(self.r.hkeys(STATE_KEY)):
                if sym not in seen:
                    self.r.hdel(STATE_KEY, sym)
        except Exception:
            pass

        return {
            "eligible": n_eligible,
            "noCost": n_no_cost,
            "protected": n_protected,
            "scanned": len(seen),
        }

    def run(self) -> None:
        log.info(
            "AutoExitBracket starting — PHASE 1 DRY-RUN (no real orders). "
            "Scans all holdings without a sell order and publishes intended "
            "+TP/-SL brackets to %s.", STATE_KEY,
        )
        while True:
            cfg = load_config(self.r)
            dry = bool(cfg.get("autoExitDryRunEnabled", True))
            live = bool(cfg.get("autoExitLiveEnabled", False))
            poll = max(5, int(_safe_float(cfg.get("autoExitPollSec"), 15)))

            if not dry and not live:
                # Fully disabled — idle but keep the heartbeat fresh.
                self._heartbeat({"enabled": False})
                time.sleep(poll)
                continue

            t0 = time.time()
            try:
                stats = self.scan_once(cfg)
            except Exception as exc:
                log.error(f"scan failed: {exc}")
                stats = {"error": str(exc)}
            stats["enabled"] = True
            stats["live"] = live
            stats["elapsedMs"] = int((time.time() - t0) * 1000)
            self._heartbeat(stats)
            time.sleep(poll)

    def _heartbeat(self, extra: Dict[str, Any]) -> None:
        try:
            hb = {"lastRun": int(time.time() * 1000), "phase": "DRY_RUN", **extra}
            self.r.hset(STATUS_KEY, "health", json.dumps(hb))
        except Exception:
            pass


def main() -> None:
    r = get_redis()
    AutoExitBracket(r).run()


if __name__ == "__main__":
    main()
