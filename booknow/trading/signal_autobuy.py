"""
signal_autobuy.py
─────────────────────────────────────────────────────────────────────────────
iter173 (2026-06-16) — MULTI-SOURCE REAL-MONEY auto-buy manager.

The operator wants to auto-buy every coin that lights up on the three
history pages, with a MARKET buy of a fixed USDT amount:

  • coin-history.html  → "buysignals" strong-buy detectors
        (PumpRider STRONG/NORMAL, InstantPump, VSP BIG_PUMP, CCP
         CALM_REVERSAL_UP, EarlyPump score≥80)  —  *** LMC EXCLUDED ***
  • pump-history.html      → PUMP_RADAR     SIGNAL
  • orderflow-history.html → ORDERFLOW_RADAR BUY

Spec (operator, 2026-06-16):
  • MARKET buy, 5.05 USDT per coin (quoteOrderQty).
  • BUY at/around the signal — SKIP if the live price already ran ABOVE
    the signal price (no chasing).
  • Max 19 concurrent coins (one position per coin).
  • The cap self-heals: when the operator later sells a coin (its base
    balance goes to dust) the slot is freed for the next signal.

This is a BUY-only path.  It does NOT arm a TP/SL bracket — the operator
manages exits manually (or via the other managers).  It is DELIBERATELY
independent of ``autoBuyEnabled`` and the HARD_DISABLE_AUTOBUY kill switch
so turning it on enables these buys and NOTHING else.

Safety: the master gate ``signalAutoBuyLiveEnabled`` defaults to **False**
(paper-first).  In paper mode every "buy" is logged, no order is placed,
and paper slots auto-release after ``PAPER_HOLD_S`` so the simulation keeps
cycling.

Architecture
────────────
A polling AsyncProcessor (no HTTP entry point needed).  Every tick it:
  1. (live) reconciles the position cap against on-exchange balances —
     frees any slot whose coin the operator has sold.
  2. scans the three signal sources across BOTH Redis instances
     (detectors + balances live in MAIN redis; the radar SIGNALS lists
     and EarlyPump live in the ANALYSE redis), keeping only signals
     fresher than ``signalAutoBuyMaxAgeSec`` and not yet processed.
  3. for each fresh signal → ``_buy_one()`` (gates → reserve slot →
     MARKET buy 5.05 USDT).

State lives in MAIN redis:
  • SIGNAL:AUTOBUY:POS          hash  symbol → position json   (the cap)
  • SIGNAL:AUTOBUY:SEEN:<date>  set   "src|symbol|ts|label"    (dedup)
  • SIGNAL:AUTOBUY:LOG:<date>   list  observability events
"""

from __future__ import annotations

import asyncio
import json
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import redis.asyncio as aioredis

from booknow.processors.base import AsyncProcessor
from booknow.repository import redis_keys


POS_KEY = "SIGNAL:AUTOBUY:POS"           # hash symbol -> position json (the cap)
BOUGHT_KEY = "SIGNAL:AUTOBUY:BOUGHT"      # hash symbol -> {ts, source} — NO RE-BUY ledger
SEEN_KEY_PREFIX = "SIGNAL:AUTOBUY:SEEN:"  # set per UTC date (signal dedup)
LOG_KEY_PREFIX = "SIGNAL:AUTOBUY:LOG:"    # list per UTC date (observability)

# Drop a reservation that never produced an order (placement crashed).
RESERVE_TIMEOUT_S = 120
# Reconcile the live cap against balances at most this often (seconds).
RECONCILE_EVERY_S = 30
# Don't reconcile a position younger than this — the balance cache may not
# reflect a just-placed buy yet, and we'd free the slot prematurely.
RECONCILE_MIN_AGE_S = 45
# Paper-mode positions auto-release after this so the sim keeps cycling.
PAPER_HOLD_S = 1800
# A base balance worth less than this (USDT) counts as "sold / dust".
DUST_USDT = 1.0

STABLECOINS = {"USDT", "USDC", "BUSD", "FDUSD", "TUSD", "DAI", "USD"}


def _d(v: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(v))
    except Exception:
        return Decimal(default)


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _base_asset(symbol: str) -> str:
    """Base asset of a USDT pair (BTCUSDT → BTC)."""
    s = symbol.upper()
    return s[:-4] if s.endswith("USDT") else s


class SignalAutoBuyManager(AsyncProcessor):
    """Multi-source, buy-only, real-money auto-buy with a self-healing cap."""

    name = "signal_autobuy"
    sleep_s = 3.0
    error_sleep_s = 3.0

    def __init__(
        self,
        *,
        redis_client: aioredis.Redis,
        redis_analyse: aioredis.Redis,
        ws_api: Any,
        filter_service: Any,
        delist_service: Any,
        config_service: Any,
        guard: Any,
        live_mode: bool,
    ) -> None:
        super().__init__()
        self._redis = redis_client          # MAIN: detectors, balances, POS, prices
        self._ra = redis_analyse            # ANALYSE: radar SIGNALS + EarlyPump
        self._ws_api = ws_api
        self._filters = filter_service
        self._delist = delist_service
        self._config = config_service
        self._guard = guard
        self.live_mode = bool(live_mode)
        # Serialises the read-modify-write of the position hash so two
        # near-simultaneous signals can't both claim the last free slot.
        self._lock = asyncio.Lock()
        self._last_reconcile = 0.0

    # ── observability ───────────────────────────────────────────────
    async def _log_event(self, **kw: Any) -> None:
        kw.setdefault("ts", int(time.time() * 1000))
        date = time.strftime("%Y-%m-%d", time.gmtime())
        try:
            await self._redis.rpush(LOG_KEY_PREFIX + date, json.dumps(kw))
            await self._redis.expire(LOG_KEY_PREFIX + date, 30 * 24 * 3600)
        except Exception:
            pass

    # ── main loop ────────────────────────────────────────────────────
    async def _tick(self) -> None:
        cfg = await self._config.get()
        if not bool(getattr(cfg, "signalAutoBuyLiveEnabled", False)):
            # Master gate off: still housekeep stale reservations so the
            # hash doesn't wedge, but place no orders and read no signals.
            await self._sweep_reservations()
            return

        max_age = int(getattr(cfg, "signalAutoBuyMaxAgeSec", 120) or 120)
        now_ms = int(time.time() * 1000)
        cutoff_ms = now_ms - max_age * 1000

        # 1) self-heal the cap (live only).
        await self._sweep_reservations()
        if self.live_mode and bool(getattr(cfg, "signalAutoBuyReconcileEnabled", True)):
            if time.time() - self._last_reconcile >= RECONCILE_EVERY_S:
                self._last_reconcile = time.time()
                await self._reconcile_cap()
        elif not self.live_mode:
            await self._reconcile_paper()

        # 2) collect fresh signals from the enabled sources.
        signals = await self._collect_signals(cfg, cutoff_ms)
        if not signals:
            return

        # 3) act on each (newest first), deduped via the SEEN set.
        date = time.strftime("%Y-%m-%d", time.gmtime())
        seen_key = SEEN_KEY_PREFIX + date
        signals.sort(key=lambda s: s["ts"], reverse=True)
        for sig in signals:
            sid = f'{sig["source"]}|{sig["symbol"]}|{sig["ts"]}|{sig["label"]}'
            try:
                added = await self._redis.sadd(seen_key, sid)
                await self._redis.expire(seen_key, 2 * 24 * 3600)
            except Exception:
                added = 1
            if not added:
                continue  # already processed this exact signal
            try:
                await self._buy_one(cfg, sig)
            except Exception as e:
                self.log.error("[SIG-AB] buy %s failed: %s",
                               sig.get("symbol"), e, exc_info=True)

    # ── signal collection ────────────────────────────────────────────
    async def _collect_signals(self, cfg: Any, cutoff_ms: int) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        # We scan today + yesterday (UTC) so a signal near midnight isn't missed.
        dates = [
            time.strftime("%Y-%m-%d", time.gmtime()),
            time.strftime("%Y-%m-%d", time.gmtime(time.time() - 86400)),
        ]

        if bool(getattr(cfg, "signalAutoBuySourcePump", True)):
            for d in dates:
                out += await self._scan_radar(
                    self._ra, f"PUMP_RADAR:SIGNALS:{d}", "pump", {"SIGNAL"}, cutoff_ms)
        if bool(getattr(cfg, "signalAutoBuySourceOrderflow", True)):
            for d in dates:
                out += await self._scan_radar(
                    self._ra, f"ORDERFLOW_RADAR:SIGNALS:{d}", "orderflow", {"BUY"}, cutoff_ms)
        if bool(getattr(cfg, "signalAutoBuySourceBuysignals", True)):
            for d in dates:
                out += await self._scan_buysignals(d, cutoff_ms)
        return out

    async def _scan_radar(
        self, client: aioredis.Redis, key: str, source: str,
        keep_labels: set, cutoff_ms: int,
    ) -> List[Dict[str, Any]]:
        """PUMP_RADAR / ORDERFLOW_RADAR lists — already-normalised records."""
        try:
            raw = await client.lrange(key, -400, -1)
        except Exception:
            return []
        res: List[Dict[str, Any]] = []
        for r in raw:
            try:
                ev = json.loads(r)
            except Exception:
                continue
            ts = int(ev.get("ts") or 0)
            if ts < cutoff_ms:
                continue
            label = str(ev.get("label") or "").upper()
            if keep_labels and label not in keep_labels:
                continue
            sym = str(ev.get("symbol") or "").upper()
            price = _f(ev.get("price"))
            if not sym or price <= 0:
                continue
            res.append({"source": source, "symbol": sym, "ts": ts,
                        "label": label or source.upper(), "price": price,
                        "vol_surge": _f(ev.get("vol_surge")),
                        "chg_pct": _f(ev.get("chg_pct"))})
        return res

    async def _scan_buysignals(self, date: str, cutoff_ms: int) -> List[Dict[str, Any]]:
        """Strong-bucket detector fires — mirrors server.js isStrongBuy(),
        LMC DELIBERATELY EXCLUDED per the operator's spec."""
        res: List[Dict[str, Any]] = []
        # (client, key, kind)
        specs: List[Tuple[aioredis.Redis, str, str]] = [
            (self._redis, f"PUMP_RIDER:DETECTIONS:{date}",   "pump_rider"),
            (self._redis, f"INSTANT_PUMP:DETECTIONS:{date}", "instant_pump"),
            (self._redis, f"VSP:DETECTIONS:{date}",          "vsp"),
            (self._redis, f"CCP:DETECTIONS:{date}",          "ccp"),
            (self._ra,    f"EARLY_PUMP:DETECTIONS:{date}",   "early_pump"),
        ]
        for client, key, kind in specs:
            try:
                raw = await client.lrange(key, -400, -1)
            except Exception:
                continue
            for r in raw:
                try:
                    ev = json.loads(r)
                except Exception:
                    continue
                ts = int(ev.get("ts") or 0)
                if ts < cutoff_ms:
                    continue
                if not self._is_strong_buy(kind, ev):
                    continue
                sym = str(ev.get("symbol") or "").upper()
                price = _f(ev.get("price") or ev.get("last_price")
                           or ev.get("trigger_close") or ev.get("trigger_price")
                           or ev.get("signal_price"))
                if not sym or price <= 0:
                    continue
                label = str(ev.get("label") or ev.get("tier") or kind).upper()
                vol_surge = _f(ev.get("vol_surge") or ev.get("vol_surge_x")
                               or ev.get("surge_5m") or ev.get("vol_surge_5m")
                               or ev.get("vs5m") or ev.get("vs1m"))
                chg_pct = _f(ev.get("chg_pct") or ev.get("price_change_pct")
                             or ev.get("chg_5m_pct") or ev.get("chg_5m"))
                res.append({"source": "buysignals:" + kind, "symbol": sym,
                            "ts": ts, "label": label, "price": price,
                            "vol_surge": vol_surge, "chg_pct": chg_pct})
        return res

    @staticmethod
    def _is_strong_buy(kind: str, ev: Dict[str, Any]) -> bool:
        lbl = str(ev.get("label") or ev.get("tier") or ev.get("rule_label") or "").upper()
        tier = str(ev.get("tier") or "").upper()
        score = _f(ev.get("score") or ev.get("pts") or 0)
        if kind == "instant_pump":
            return True
        if kind == "pump_rider":
            return tier in ("STRONG", "NORMAL")
        if kind == "early_pump":
            return score >= 80
        if kind == "vsp":
            return lbl.startswith("BIG_PUMP")
        if kind == "ccp":
            return lbl == "CALM_REVERSAL_UP"
        return False  # lmc EXCLUDED, everything else = watch-only

    # ── buy one coin ─────────────────────────────────────────────────
    async def _buy_one(self, cfg: Any, sig: Dict[str, Any]) -> Dict[str, Any]:
        symbol = sig["symbol"].upper()
        signal_price = _f(sig["price"])
        if signal_price <= 0:
            return {"status": "skipped", "reason": "bad signal_price"}

        # ── NO RE-BUY (rule A1/A5): once a coin was bought, never buy it
        # again (0h cooldown = forever).  Covers the same coins shown on
        # position-planner.html since it reads the same signal feeds.
        if await self._recently_bought(cfg, symbol):
            await self._log_event(event="skip_rebuy", source=sig["source"], symbol=symbol)
            return {"status": "skipped", "reason": "already bought (no re-buy)"}

        # ── entry filters (rules A2 vol-surge band + A4 anti-chase) ──
        if bool(getattr(cfg, "signalAutoBuyEntryFiltersEnabled", True)):
            vs = _f(sig.get("vol_surge"))
            if vs > 0:  # only gate when the signal carries a surge figure
                vmin = _f(getattr(cfg, "signalAutoBuyVolSurgeMin", 2.5), 2.5)
                vmax = _f(getattr(cfg, "signalAutoBuyVolSurgeMax", 8.0), 8.0)
                if vs < vmin or vs > vmax:
                    await self._log_event(event="skip_volband", source=sig["source"],
                                          symbol=symbol, vol_surge=vs, lo=vmin, hi=vmax)
                    return {"status": "skipped",
                            "reason": f"vol_surge {vs:.1f}x outside [{vmin},{vmax}]"}
            chg = _f(sig.get("chg_pct"))
            chg_max = _f(getattr(cfg, "signalAutoBuyChgMaxPct", 8.0), 8.0)
            if chg > 0 and chg_max > 0 and chg > chg_max:
                await self._log_event(event="skip_chase_candle", source=sig["source"],
                                      symbol=symbol, chg_pct=chg, max=chg_max)
                return {"status": "skipped", "reason": f"chg {chg:.1f}% > {chg_max}% (chasing)"}

        # No-chase: don't buy a coin that already ran above the signal.
        current_price = await self._current_price(symbol)
        if bool(getattr(cfg, "signalAutoBuyNoChase", True)):
            if current_price is not None and current_price > signal_price:
                await self._log_event(event="skip_chase", source=sig["source"],
                                      symbol=symbol, signal=signal_price, now=current_price)
                return {"status": "skipped", "reason": "current > signal (no-chase)"}

        if self._guard.is_banned():
            return {"status": "skipped", "reason": "binance ban active"}
        try:
            if await self._delist.is_delisted(symbol):
                return {"status": "skipped", "reason": "delisted"}
        except Exception:
            return {"status": "skipped", "reason": "delist check failed"}

        usdt = _f(getattr(cfg, "signalAutoBuyUsdt", 5.05), 5.05)
        max_pos = int(getattr(cfg, "signalAutoBuyMaxPositions", 19) or 19)

        # ── reserve a slot atomically (cap + dedup per coin) ─────────
        async with self._lock:
            if await self._redis.hexists(POS_KEY, symbol):
                return {"status": "skipped", "reason": "already holding"}
            count = await self._redis.hlen(POS_KEY)
            if count >= max_pos:
                await self._log_event(event="skip_cap", source=sig["source"],
                                      symbol=symbol, open_positions=count, cap=max_pos)
                return {"status": "skipped", "reason": f"cap {count}/{max_pos}"}
            await self._redis.hset(POS_KEY, symbol, json.dumps({
                "state": "RESERVED",
                "ts": int(time.time() * 1000),
                "source": sig["source"],
                "label": sig["label"],
                "signalPrice": signal_price,
            }))

        # ── place the MARKET buy (outside the lock) ──────────────────
        try:
            if self.live_mode:
                self.log.info(
                    "[SIG-AB] LIVE marketBuy %s %sUSDT (src=%s signal=%s now=%s)",
                    symbol, usdt, sig["source"], signal_price, current_price,
                )
                resp = await self._ws_api.place_order(
                    symbol=symbol, side="BUY", order_type="MARKET",
                    quote_order_qty=str(usdt),
                )
            else:
                self.log.info(
                    "[SIG-AB] PAPER marketBuy %s %sUSDT (src=%s signal=%s)",
                    symbol, usdt, sig["source"], signal_price,
                )
                qty = (_d(usdt) / _d(signal_price)) if signal_price > 0 else Decimal(0)
                resp = {
                    "orderId": int(time.time() * 1000),
                    "status": "FILLED",
                    "executedQty": str(qty),
                    "cummulativeQuoteQty": str(usdt),
                }

            order_id = int(resp.get("orderId") or 0)
            status = (resp.get("status") or "NEW").upper()
            executed = _d(resp.get("executedQty"))
            cqq = _d(resp.get("cummulativeQuoteQty"))
            fill_price = (cqq / executed) if (executed > 0 and cqq > 0) else _d(signal_price)

            pos = {
                "state": "OPEN",
                "buyOrderId": order_id,
                "orderStatus": status,
                "qty": str(executed),
                "fillPrice": float(fill_price),
                "signalPrice": signal_price,
                "usdt": usdt,
                "source": sig["source"],
                "label": sig["label"],
                "ts": int(time.time() * 1000),
            }
            await self._redis.hset(POS_KEY, symbol, json.dumps(pos))
            # Record in the NO-RE-BUY ledger so this coin is never auto-bought
            # again (until cooldown elapses, if one is configured).
            try:
                await self._redis.hset(BOUGHT_KEY, symbol, json.dumps({
                    "ts": int(time.time() * 1000), "source": sig["source"],
                    "fillPrice": float(fill_price),
                }))
            except Exception:
                pass
            await self._log_event(
                event="bought", source=sig["source"], symbol=symbol,
                usdt=usdt, qty=str(executed), fillPrice=float(fill_price),
                signalPrice=signal_price, orderId=order_id, status=status,
                live=self.live_mode,
            )
            self.log.info("[SIG-AB] %s BOUGHT qty=%s @ ~%s (#%s %s)",
                          symbol, executed, fill_price, order_id, status)
            return {"status": "bought", "symbol": symbol, "qty": str(executed)}

        except Exception as e:
            await self._release(symbol)
            self.log.error("[SIG-AB] market-buy %s failed: %s", symbol, e, exc_info=True)
            return {"status": "error", "reason": str(e)}

    async def _release(self, symbol: str) -> None:
        try:
            await self._redis.hdel(POS_KEY, symbol)
        except Exception:
            pass

    async def _recently_bought(self, cfg: Any, symbol: str) -> bool:
        """True if ``symbol`` is in the NO-RE-BUY ledger and the cooldown
        (if any) has not yet elapsed.  cooldown=0 ⇒ remembered forever."""
        try:
            raw = await self._redis.hget(BOUGHT_KEY, symbol)
        except Exception:
            return False
        if not raw:
            return False
        cooldown_h = int(getattr(cfg, "signalAutoBuyRebuyCooldownHours", 0) or 0)
        if cooldown_h <= 0:
            return True  # never re-buy
        try:
            ts = int(json.loads(raw).get("ts", 0))
        except Exception:
            return True
        age_h = (time.time() * 1000 - ts) / 3_600_000.0
        if age_h >= cooldown_h:
            # cooldown elapsed — forget it so it can be bought again.
            try:
                await self._redis.hdel(BOUGHT_KEY, symbol)
            except Exception:
                pass
            return False
        return True

    # ── cap self-healing ─────────────────────────────────────────────
    async def _sweep_reservations(self) -> None:
        """Drop reservations that never became an order (placement crashed)."""
        try:
            raw = await self._redis.hgetall(POS_KEY)
        except Exception:
            return
        now_ms = int(time.time() * 1000)
        for sym, val in (raw or {}).items():
            try:
                pos = json.loads(val)
            except Exception:
                await self._release(sym)
                continue
            if pos.get("state") == "RESERVED":
                age = (now_ms - int(pos.get("ts", now_ms))) / 1000.0
                if age > RESERVE_TIMEOUT_S:
                    self.log.warning("[SIG-AB] drop stale reservation %s", sym)
                    await self._release(sym)

    async def _reconcile_cap(self) -> None:
        """Free any slot whose coin the operator has sold (base balance is
        dust).  Reads the MAIN-redis BINANCE:BALANCE:<asset> cache that the
        BalanceService keeps in sync with the exchange."""
        try:
            raw = await self._redis.hgetall(POS_KEY)
        except Exception:
            return
        now_ms = int(time.time() * 1000)
        for sym, val in (raw or {}).items():
            try:
                pos = json.loads(val)
            except Exception:
                continue
            if pos.get("state") != "OPEN":
                continue
            if (now_ms - int(pos.get("ts", now_ms))) / 1000.0 < RECONCILE_MIN_AGE_S:
                continue
            asset = _base_asset(sym)
            if asset in STABLECOINS:
                continue
            try:
                braw = await self._redis.get(redis_keys.BALANCE_PREFIX + asset)
            except Exception:
                continue
            free = 0.0
            if braw:
                try:
                    brow = json.loads(braw)
                    free = _f(brow.get("free")) + _f(brow.get("locked"))
                except Exception:
                    free = 0.0
            # value the remaining balance; missing key OR dust ⇒ sold.
            price = await self._current_price(sym) or _f(pos.get("fillPrice"))
            value = free * (price or 0.0)
            if (not braw) or value < DUST_USDT:
                self.log.info("[SIG-AB] reconcile: %s sold/dust (free=%s val=$%.2f) — free slot",
                              sym, free, value)
                await self._log_event(event="reconcile_free", symbol=sym,
                                      free=free, value=value)
                await self._release(sym)

    async def _reconcile_paper(self) -> None:
        """Paper mode has no real balances — release a paper slot after a
        hold window so the simulation keeps cycling through new signals."""
        try:
            raw = await self._redis.hgetall(POS_KEY)
        except Exception:
            return
        now_ms = int(time.time() * 1000)
        for sym, val in (raw or {}).items():
            try:
                pos = json.loads(val)
            except Exception:
                await self._release(sym)
                continue
            if pos.get("state") != "OPEN":
                continue
            if (now_ms - int(pos.get("ts", now_ms))) / 1000.0 > PAPER_HOLD_S:
                await self._log_event(event="paper_release", symbol=sym)
                await self._release(sym)

    async def _current_price(self, symbol: str) -> Optional[float]:
        try:
            raw = await self._redis.hget(redis_keys.CURRENT_PRICE, symbol)
            if not raw:
                return None
            obj = json.loads(raw)
            p = float(obj.get("price") or 0)
            return p if p > 0 else None
        except Exception:
            return None
