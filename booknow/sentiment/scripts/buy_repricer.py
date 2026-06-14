#!/usr/bin/env python3
"""
buy_repricer.py — iter167 (2026-06-14)
─────────────────────────────────────────────────────────────────────────────
Smart Buy Re-pricer ("chase-down") — PHASE 1: DRY-RUN ONLY.

Motivation — the HMSTRUSDT trade on 2026-06-14 20:45:
  A PumpRider signal fired with taker buy 41.1K vs taker SELL 94.9K — i.e. the
  pump was being SOLD into (distribution). A resting limit BUY placed near the
  top would have been better served by being *cancelled and re-placed lower*
  while sell-flow dominated, instead of filling into a fade.

What this subprocess does (DRY-RUN):
  1. Reads an opt-in watchlist `BUY_REPRICER:WATCH` (hash symbol -> JSON with
     the user's resting-buy reference price). Only subscribed coins are watched.
  2. Subscribes to each coin's Binance `aggTrade` stream (WS, zero REST weight)
     and maintains a rolling window of taker BUY vs SELL quote-volume.
  3. When sell-volume dominates buy-volume over the window
     (sell_vol / buy_vol >= ratio AND net delta < 0), it computes a NEW, lower
     limit-buy price (chase-down) and PUBLISHES a dry-run reprice signal to
     `BUY_REPRICER:SIGNALS:<date>`. It tracks the cumulative chase per coin and
     stops at a floor (maxChase% below the original).
  4. Maintains `BUY_REPRICER:STATE` (hash symbol -> latest JSON) for the UI.

IMPORTANT — this phase NEVER places or cancels a real order. The live switch
`buyRepricerLiveEnabled` is honoured but Phase-2 execution is intentionally not
wired yet; when live is on we only log that execution is pending. All
autobuy/autosell remains HARD-disabled engine-wide (iter94).

Trade-side convention (Binance aggTrade):
    m (isBuyerMaker) == True  → aggressor is a SELLER → taker SELL
    m (isBuyerMaker) == False → aggressor is a BUYER  → taker BUY

All state lives in Redis so the supervisor can restart this freely.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional, Tuple

import redis
import ssl

import websockets
import websockets.exceptions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("BuyRepricer")

# ── Config (Redis-backed; falls back to defaults if missing) ────────────
CONFIG_KEY = "TRADING_CONFIG"
DEFAULTS: Dict[str, Any] = {
    # master switches
    "buyRepricerDryRunEnabled": True,    # run the monitor + publish dry-run signals (zero risk)
    "buyRepricerLiveEnabled": False,     # PHASE 2 — actually cancel+replace real orders (NOT wired yet)
    # trigger
    "buyRepricerSellBuyRatio": 1.5,      # sell_vol / buy_vol >= this  → sell-dominant
    "buyRepricerWindowSec": 60,          # rolling taker-flow window
    "buyRepricerMinTrades": 5,           # need at least this many trades in window to act
    "buyRepricerMinFlowUsdt": 500.0,     # need at least this much total quote-vol in window
    # reprice math
    "buyRepricerDiscountPct": 0.3,       # each step: new = ref_anchor * (1 - 0.3%)
    "buyRepricerMinChasePct": 0.15,      # a step must move the order at least this far down
    "buyRepricerMaxChasePct": 3.0,       # never chase more than this below the ORIGINAL ref price (floor)
    "buyRepricerCooldownSec": 30,        # min seconds between two reprices of the same coin
    # loop
    "buyRepricerEvalSec": 2.0,           # how often the decision loop runs
}

REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

WATCH_KEY = "BUY_REPRICER:WATCH"        # hash: symbol -> {"ref_price":.., "added_ts":..}
STATE_KEY = "BUY_REPRICER:STATE"        # hash: symbol -> latest state JSON (for UI)
STATUS_KEY = "BUY_REPRICER:STATUS"      # hash: monitor health
SIGNALS_RETENTION_S = 90 * 24 * 60 * 60  # 90-day history (matches history pages)

WS_BASE = "wss://stream.binance.com:9443/stream"
_SSL_CTX = ssl._create_unverified_context()


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


def load_watchlist(r: redis.Redis) -> Dict[str, Dict[str, Any]]:
    """symbol(upper) -> {ref_price: float, added_ts: int, ...}"""
    out: Dict[str, Dict[str, Any]] = {}
    try:
        raw = r.hgetall(WATCH_KEY) or {}
    except Exception as exc:
        log.warning(f"watchlist read failed: {exc}")
        return out
    for sym, val in raw.items():
        try:
            ev = json.loads(val)
            ref = float(ev.get("ref_price") or 0)
        except Exception:
            continue
        if ref <= 0:
            continue
        out[sym.upper()] = ev
    return out


# ── Per-symbol rolling taker flow ────────────────────────────────────────
class FlowWindow:
    """Rolling taker BUY/SELL quote-volume over a sliding window."""

    def __init__(self, window_sec: float):
        self.window_sec = float(window_sec)
        # (ts_sec, quote_qty, is_buy)
        self._trades: Deque[Tuple[float, float, bool]] = deque()
        self.last_price: Optional[float] = None

    def on_trade(self, data: Dict[str, Any]) -> None:
        try:
            price = float(data["p"])
            qty = float(data["q"])
            ts = float(data.get("T", time.time() * 1000)) / 1000.0
            is_buyer_maker = bool(data.get("m", False))
        except (KeyError, TypeError, ValueError):
            return
        self.last_price = price
        self._trades.append((ts, price * qty, not is_buyer_maker))
        self._evict(ts)

    def _evict(self, now: float) -> None:
        cutoff = now - self.window_sec
        while self._trades and self._trades[0][0] < cutoff:
            self._trades.popleft()

    def stats(self, now: float) -> Dict[str, Any]:
        self._evict(now)
        buy_vol = sell_vol = 0.0
        n = 0
        for _ts, quote, is_buy in self._trades:
            n += 1
            if is_buy:
                buy_vol += quote
            else:
                sell_vol += quote
        return {
            "buy_vol": buy_vol,
            "sell_vol": sell_vol,
            "trades": n,
            "total_vol": buy_vol + sell_vol,
            "delta": buy_vol - sell_vol,
            "last_price": self.last_price,
        }


# ── Per-symbol chase state ───────────────────────────────────────────────
class CoinState:
    def __init__(self, symbol: str, ref_price: float):
        self.symbol = symbol
        self.ref_price = ref_price          # original resting-buy price
        self.effective_price = ref_price    # where the (hypothetical) order now sits
        self.last_action = "WATCH"
        self.last_reprice_ts = 0.0
        self.reprice_count = 0
        self.floor_reached = False


class BuyRepricer:
    def __init__(self, r: redis.Redis):
        self.r = r
        self.cfg = load_config(r)
        self.flows: Dict[str, FlowWindow] = {}
        self.states: Dict[str, CoinState] = {}
        self.subscribed: set[str] = set()
        self._ws: Optional[Any] = None
        self._sub_id = 1

    # ── WS feed ──────────────────────────────────────────────────────────
    async def ws_loop(self) -> None:
        backoff = 2
        while True:
            try:
                async with websockets.connect(
                    WS_BASE, ping_interval=20, ping_timeout=15,
                    ssl=_SSL_CTX, max_size=8 * 1024 * 1024,
                ) as ws:
                    self._ws = ws
                    backoff = 2
                    # (re)subscribe to whatever is currently wanted
                    self.subscribed.clear()
                    await self._sync_subscriptions()
                    log.info("[BuyRepricer] WS connected")
                    async for raw in ws:
                        self._on_frame(raw)
            except (websockets.exceptions.ConnectionClosed, OSError) as e:
                log.warning(f"[BuyRepricer] WS dropped: {e} — reconnect in {backoff}s")
            except Exception as e:
                log.error(f"[BuyRepricer] WS error: {e}", exc_info=True)
            finally:
                self._ws = None
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    def _on_frame(self, raw) -> None:
        try:
            msg = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return
        stream = msg.get("stream", "")
        data = msg.get("data", msg)
        sym_part, _, kind = stream.partition("@")
        if not kind.startswith("aggTrade"):
            return
        sym = sym_part.upper()
        fw = self.flows.get(sym)
        if fw is not None:
            fw.on_trade(data)

    async def _sync_subscriptions(self) -> None:
        """Diff wanted vs subscribed symbols; SUBSCRIBE/UNSUBSCRIBE deltas."""
        if self._ws is None:
            return
        wanted = set(self.states.keys())
        to_add = wanted - self.subscribed
        to_drop = self.subscribed - wanted
        if to_add:
            params = [f"{s.lower()}@aggTrade" for s in to_add]
            await self._ws.send(json.dumps({"method": "SUBSCRIBE", "params": params, "id": self._sub_id}))
            self._sub_id += 1
            self.subscribed |= to_add
        if to_drop:
            params = [f"{s.lower()}@aggTrade" for s in to_drop]
            await self._ws.send(json.dumps({"method": "UNSUBSCRIBE", "params": params, "id": self._sub_id}))
            self._sub_id += 1
            self.subscribed -= to_drop

    # ── Watchlist reconciliation ─────────────────────────────────────────
    def _reconcile_watchlist(self, wl: Dict[str, Dict[str, Any]]) -> None:
        # add new coins
        for sym, ev in wl.items():
            ref = float(ev.get("ref_price") or 0)
            if sym not in self.states:
                self.states[sym] = CoinState(sym, ref)
                self.flows[sym] = FlowWindow(float(self.cfg["buyRepricerWindowSec"]))
                log.info(f"[BuyRepricer] subscribe {sym} ref={ref}")
            else:
                # if the user changed the ref price, reset the chase
                st = self.states[sym]
                if abs(st.ref_price - ref) / max(ref, 1e-12) > 1e-9:
                    st.ref_price = ref
                    st.effective_price = ref
                    st.floor_reached = False
                    st.reprice_count = 0
                    log.info(f"[BuyRepricer] {sym} ref updated → {ref}")
        # drop removed coins
        for sym in list(self.states.keys()):
            if sym not in wl:
                self.states.pop(sym, None)
                self.flows.pop(sym, None)
                self.r.hdel(STATE_KEY, sym)
                log.info(f"[BuyRepricer] unsubscribe {sym}")

    # ── Decision ─────────────────────────────────────────────────────────
    def _evaluate(self, sym: str, now: float) -> None:
        cfg = self.cfg
        st = self.states[sym]
        fw = self.flows[sym]
        s = fw.stats(now)

        ratio_gate = float(cfg["buyRepricerSellBuyRatio"])
        min_trades = int(cfg["buyRepricerMinTrades"])
        min_flow = float(cfg["buyRepricerMinFlowUsdt"])
        discount = float(cfg["buyRepricerDiscountPct"]) / 100.0
        min_chase = float(cfg["buyRepricerMinChasePct"]) / 100.0
        max_chase = float(cfg["buyRepricerMaxChasePct"]) / 100.0
        cooldown = float(cfg["buyRepricerCooldownSec"])

        buy_vol = s["buy_vol"]
        sell_vol = s["sell_vol"]
        ratio = (sell_vol / buy_vol) if buy_vol > 0 else (float("inf") if sell_vol > 0 else 0.0)
        enough = s["trades"] >= min_trades and s["total_vol"] >= min_flow
        sell_dominant = enough and ratio >= ratio_gate and s["delta"] < 0

        floor_price = st.ref_price * (1.0 - max_chase)
        action = "HOLD" if not sell_dominant else "EVAL"
        published = False

        if sell_dominant and not st.floor_reached:
            if (now - st.last_reprice_ts) >= cooldown and st.effective_price > floor_price:
                anchor = st.effective_price
                if s["last_price"] and s["last_price"] < anchor:
                    anchor = s["last_price"]
                target = anchor * (1.0 - discount)
                # ensure a meaningful step below the current order
                step_cap = st.effective_price * (1.0 - min_chase)
                target = min(target, step_cap)
                # clamp to floor
                if target <= floor_price:
                    target = floor_price
                    st.floor_reached = True
                    action = "FLOOR_REACHED"
                else:
                    action = "REPRICE_DOWN"
                self._publish_signal(sym, st, s, ratio, action, st.effective_price, target)
                st.effective_price = target
                st.last_reprice_ts = now
                st.reprice_count += 1
                published = True
            else:
                action = "SELL_DOMINANT_COOLDOWN"
        elif st.floor_reached:
            action = "FLOOR_REACHED"

        # publish a HOLD transition once (sell pressure relented after a chase)
        if not published and action == "HOLD" and st.last_action not in ("HOLD", "WATCH"):
            self._publish_signal(sym, st, s, ratio, "HOLD", st.effective_price, st.effective_price)

        st.last_action = action
        self._write_state(sym, st, s, ratio, floor_price)

    def _publish_signal(self, sym: str, st: CoinState, s: Dict[str, Any],
                        ratio: float, action: str, from_price: float, to_price: float) -> None:
        ev = {
            "symbol": sym,
            "ts": int(time.time() * 1000),
            "action": action,                 # REPRICE_DOWN | FLOOR_REACHED | HOLD
            "ref_price": st.ref_price,
            "from_price": round(from_price, 12),
            "to_price": round(to_price, 12),
            "chase_total_pct": round((1.0 - to_price / st.ref_price) * 100.0, 3) if st.ref_price else 0.0,
            "ratio": round(ratio, 3) if ratio != float("inf") else 999.0,
            "buy_vol": round(s["buy_vol"], 2),
            "sell_vol": round(s["sell_vol"], 2),
            "delta": round(s["delta"], 2),
            "trades": s["trades"],
            "last_price": s["last_price"],
            "reprice_count": st.reprice_count + (1 if action in ("REPRICE_DOWN", "FLOOR_REACHED") else 0),
            "mode": "DRYRUN",
            "live": False,
        }
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        key = f"BUY_REPRICER:SIGNALS:{date}"
        try:
            self.r.lpush(key, json.dumps(ev))
            self.r.ltrim(key, 0, 4999)
            self.r.expire(key, SIGNALS_RETENTION_S)
        except Exception as exc:
            log.warning(f"publish_signal failed: {exc}")
        # Phase-2 placeholder: live execution intentionally not wired yet.
        if bool(self.cfg.get("buyRepricerLiveEnabled", False)) and action in ("REPRICE_DOWN", "FLOOR_REACHED"):
            log.info(f"[BuyRepricer] LIVE flag on but Phase-2 execution not wired — {sym} {action} dry-run only")
        log.info(f"[BuyRepricer] {sym} {action} {from_price:.10f}→{to_price:.10f} "
                 f"ratio={ev['ratio']} buy={ev['buy_vol']} sell={ev['sell_vol']}")

    def _write_state(self, sym: str, st: CoinState, s: Dict[str, Any],
                     ratio: float, floor_price: float) -> None:
        state = {
            "symbol": sym,
            "ref_price": st.ref_price,
            "effective_price": round(st.effective_price, 12),
            "floor_price": round(floor_price, 12),
            "last_action": st.last_action,
            "reprice_count": st.reprice_count,
            "floor_reached": st.floor_reached,
            "ratio": round(ratio, 3) if ratio != float("inf") else 999.0,
            "buy_vol": round(s["buy_vol"], 2),
            "sell_vol": round(s["sell_vol"], 2),
            "last_price": s["last_price"],
            "updated_ts": int(time.time() * 1000),
        }
        try:
            self.r.hset(STATE_KEY, sym, json.dumps(state))
        except Exception as exc:
            log.warning(f"write_state failed: {exc}")

    # ── Main loops ───────────────────────────────────────────────────────
    async def eval_loop(self) -> None:
        while True:
            t0 = time.time()
            try:
                self.cfg = load_config(self.r)
                if not bool(self.cfg.get("buyRepricerDryRunEnabled", True)):
                    self._write_status(enabled=0, watched=len(self.states))
                    await asyncio.sleep(2.0)
                    continue
                wl = load_watchlist(self.r)
                self._reconcile_watchlist(wl)
                await self._sync_subscriptions()
                now = time.time()
                for sym in list(self.states.keys()):
                    try:
                        self._evaluate(sym, now)
                    except Exception as exc:
                        log.warning(f"evaluate {sym} failed: {exc}")
                self._write_status(enabled=1, watched=len(self.states))
            except Exception as exc:
                log.error(f"eval cycle error: {exc}", exc_info=True)
            elapsed = time.time() - t0
            await asyncio.sleep(max(0.5, float(self.cfg.get("buyRepricerEvalSec", 2.0)) - elapsed))

    def _write_status(self, enabled: int, watched: int) -> None:
        try:
            self.r.hset(STATUS_KEY, mapping={
                "enabled": enabled,
                "watched": watched,
                "live_enabled": int(bool(self.cfg.get("buyRepricerLiveEnabled", False))),
                "ws_connected": int(self._ws is not None),
                "last_poll_ts": int(time.time() * 1000),
            })
        except Exception:
            pass

    async def run(self) -> None:
        await asyncio.gather(self.ws_loop(), self.eval_loop())


def main() -> None:
    r = get_redis()
    try:
        r.ping()
    except Exception as exc:
        log.error(f"redis ping failed: {exc}")
        sys.exit(1)
    log.info("[BuyRepricer] starting — PHASE 1 DRY-RUN (no real orders)")
    app = BuyRepricer(r)
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
