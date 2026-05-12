"""metrics_collector.py
─────────────────────────────────────────────────────────────────────────────
Captures every decision the scalper makes — signal evaluated, signal
filtered, buy placed, fill, TP hit, exit — into Redis so the dashboard can
show a daily P&L breakdown and surface *why* a coin was bought or skipped.

Why a separate collector?
────────────────────────
The two scalpers (Fast + Virtual) currently log to stdout and a few ad-hoc
Redis keys.  When we want to know "did the falling-knife filter actually
help today?" or "which coins did we skip?" we have to grep logs.

This module gives both scalpers a single API:

    metrics.signal_evaluated(symbol, price, features)
    metrics.signal_skipped(symbol, rule, reason, features)
    metrics.buy_placed(symbol, price, size_usdt, features)
    metrics.fill_recorded(symbol, fill_price, qty, latency_ms)
    metrics.tp_hit(symbol, fill_price, tp_price, latency_min)
    metrics.exit_recorded(symbol, fill_price, exit_price, reason)

All entries land in Redis under ``METRICS:*`` keys with a 30-day TTL so the
analyse DB doesn't grow forever.  The dashboard reads these keys back and
renders charts/tables.

Schema
──────
    METRICS:DAILY:{date}              hash   aggregated counters
    METRICS:SIGNAL:{date}             list   raw signal events (capped 5000)
    METRICS:SKIP:{date}               list   skipped signals (capped 5000)
    METRICS:BUY:{date}                list   buy events with features
    METRICS:OUTCOME:{date}:{symbol}   hash   running outcome per coin
    METRICS:FILTER_BREAKDOWN:{date}   hash   counts per filter rule

Counters live in DAILY: signals_seen, signals_passed, signals_skipped,
buys_placed, fills, tp_hits, exits_loss, exits_win, etc.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

logger = logging.getLogger("booknow.metrics")

LIST_CAP = 5000           # keep last N events per list
TTL_DAYS = 30
TTL_SECONDS = TTL_DAYS * 24 * 3600


def _today() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class MetricsCollector:
    """Sync Redis client (matches both scalpers' style — neither uses asyncio
    for their Redis calls today).  The `client` argument should be an
    instance of ``redis.Redis`` with ``decode_responses=True``."""

    client: Any                       # redis.Redis
    enabled: bool = True
    namespace_prefix: str = "METRICS"

    # ── helpers ──────────────────────────────────────────────────────────
    def _k(self, *parts: str) -> str:
        return ":".join((self.namespace_prefix, *parts))

    def _push(self, key: str, payload: Dict[str, Any]) -> None:
        if not self.enabled or self.client is None:
            return
        try:
            self.client.lpush(key, json.dumps(payload))
            self.client.ltrim(key, 0, LIST_CAP - 1)
            self.client.expire(key, TTL_SECONDS)
        except Exception as exc:
            logger.debug("metrics push to %s failed: %s", key, exc)

    def _hincr(self, key: str, field: str, by: int = 1) -> None:
        if not self.enabled or self.client is None:
            return
        try:
            self.client.hincrby(key, field, by)
            self.client.expire(key, TTL_SECONDS)
        except Exception as exc:
            logger.debug("metrics hincrby on %s.%s failed: %s", key, field, exc)

    def _hset(self, key: str, mapping: Dict[str, Any]) -> None:
        if not self.enabled or self.client is None:
            return
        try:
            flat = {k: json.dumps(v) if isinstance(v, (dict, list)) else v
                    for k, v in mapping.items()}
            self.client.hset(key, mapping=flat)
            self.client.expire(key, TTL_SECONDS)
        except Exception as exc:
            logger.debug("metrics hset on %s failed: %s", key, exc)

    # ── event recorders ──────────────────────────────────────────────────
    def signal_evaluated(self, symbol: str, price: float,
                         features: Optional[Dict[str, Any]] = None,
                         decision: str = "pass") -> None:
        date = _today()
        self._push(self._k("SIGNAL", date), {
            "ts": _now_ms(), "symbol": symbol, "price": price,
            "decision": decision, "features": features or {},
        })
        self._hincr(self._k("DAILY", date), "signals_seen")
        if decision == "pass":
            self._hincr(self._k("DAILY", date), "signals_passed")

    def signal_skipped(self, symbol: str, rule: str, reason: str,
                       features: Optional[Dict[str, Any]] = None) -> None:
        date = _today()
        payload = {
            "ts": _now_ms(), "symbol": symbol, "rule": rule, "reason": reason,
            "features": features or {},
        }
        self._push(self._k("SKIP", date), payload)
        self._hincr(self._k("DAILY", date), "signals_skipped")
        self._hincr(self._k("FILTER_BREAKDOWN", date), rule)

    def buy_placed(self, symbol: str, price: float, size_usdt: float,
                   features: Optional[Dict[str, Any]] = None,
                   order_type: str = "market", offset_pct: float = 0.0,
                   signal_price: Optional[float] = None,
                   pre_signal_price: Optional[float] = None,
                   buy_1_limit_price: Optional[float] = None,
                   buy_2_limit_price: Optional[float] = None,
                   past_15min_low: Optional[float] = None,
                   past_15min_high: Optional[float] = None,
                   target_sell_005: Optional[float] = None,
                   target_sell_010: Optional[float] = None,
                   target_sell_015: Optional[float] = None) -> None:
        """Records a buy with FULL audit trail for the dashboard.

        Captured per buy event:
          • signal_price       — ask at signal time
          • pre_signal_price   — price 5 min before signal (for trend context)
          • buy_1_limit_price  — limit after offset applied (= the order on book)
          • buy_2_limit_price  — limit for next leg
          • past_15min_low/high — recent extremes around signal time
          • target_sell_005/010/015 — sell prices for $0.05/$0.10/$0.15 net
        """
        date = _today()
        payload = {
            "ts": _now_ms(), "symbol": symbol, "price": price,
            "size_usdt": size_usdt, "order_type": order_type,
            "offset_pct": offset_pct, "features": features or {},
            # NEW audit fields (iter 12)
            "signal_price": signal_price,
            "pre_signal_price": pre_signal_price,
            "buy_1_limit_price": buy_1_limit_price,
            "buy_2_limit_price": buy_2_limit_price,
            "past_15min_low": past_15min_low,
            "past_15min_high": past_15min_high,
            "target_sell_005": target_sell_005,
            "target_sell_010": target_sell_010,
            "target_sell_015": target_sell_015,
        }
        self._push(self._k("BUY", date), payload)
        self._hincr(self._k("DAILY", date), "buys_placed")
        # Initialise the per-coin OUTCOME / AUDIT record
        outcome_key = self._k("OUTCOME", date, symbol.replace("/", ""))
        audit_fields = {
            "symbol": symbol,
            "buy_ts": _now_ms(),
            "buy_price": price,
            "size_usdt": size_usdt,
            "filled": 0,
            "tp_hit": 0,
            "exited": 0,
            "features": features or {},
            # NEW audit fields
            "signal_price": signal_price if signal_price is not None else "",
            "pre_signal_price": pre_signal_price if pre_signal_price is not None else "",
            "buy_1_limit_price": buy_1_limit_price if buy_1_limit_price is not None else "",
            "buy_2_limit_price": buy_2_limit_price if buy_2_limit_price is not None else "",
            "past_15min_low": past_15min_low if past_15min_low is not None else "",
            "past_15min_high": past_15min_high if past_15min_high is not None else "",
            "target_sell_005": target_sell_005 if target_sell_005 is not None else "",
            "target_sell_010": target_sell_010 if target_sell_010 is not None else "",
            "target_sell_015": target_sell_015 if target_sell_015 is not None else "",
            "offset_pct": offset_pct,
            "order_type": order_type,
            "lowest_since_buy": "",          # updated by tick
            "highest_since_buy": "",         # updated by tick
        }
        self._hset(outcome_key, audit_fields)
        # Also push a compact AUDIT event so the new dashboard can list them
        # chronologically without needing to scan all OUTCOME keys.
        self._push(self._k("AUDIT", date), {
            "ts": _now_ms(), "symbol": symbol,
            "signal_price": signal_price,
            "pre_signal_price": pre_signal_price,
            "offset_pct": offset_pct,
            "buy_1_limit_price": buy_1_limit_price,
            "buy_1_actual_price": price,
            "buy_2_limit_price": buy_2_limit_price,
            "past_15min_low": past_15min_low,
            "past_15min_high": past_15min_high,
            "target_sell_005": target_sell_005,
            "target_sell_010": target_sell_010,
            "target_sell_015": target_sell_015,
            "size_usdt": size_usdt,
        })

    def fill_recorded(self, symbol: str, fill_price: float, qty: float,
                      latency_ms: int = 0) -> None:
        date = _today()
        outcome_key = self._k("OUTCOME", date, symbol.replace("/", ""))
        self._hset(outcome_key, {
            "filled": 1, "fill_price": fill_price, "qty": qty,
            "fill_ts": _now_ms(), "fill_latency_ms": latency_ms,
        })
        self._hincr(self._k("DAILY", date), "fills")

    def tp_hit(self, symbol: str, fill_price: float, tp_price: float,
               latency_min: float = 0.0) -> None:
        date = _today()
        outcome_key = self._k("OUTCOME", date, symbol.replace("/", ""))
        self._hset(outcome_key, {
            "tp_hit": 1, "tp_price": tp_price, "tp_ts": _now_ms(),
            "tp_latency_min": latency_min,
        })
        self._hincr(self._k("DAILY", date), "tp_hits")

    def exit_recorded(self, symbol: str, fill_price: float, exit_price: float,
                      reason: str, pnl_usdt: float = 0.0) -> None:
        date = _today()
        outcome_key = self._k("OUTCOME", date, symbol.replace("/", ""))
        self._hset(outcome_key, {
            "exited": 1, "exit_price": exit_price, "exit_reason": reason,
            "exit_ts": _now_ms(), "pnl_usdt": pnl_usdt,
        })
        bucket = "exits_win" if pnl_usdt >= 0 else "exits_loss"
        self._hincr(self._k("DAILY", date), bucket)
        # Total realised P&L stored as a fixed-point integer in cents
        try:
            self.client.hincrbyfloat(self._k("DAILY", date), "realized_pnl_usdt", pnl_usdt)
            self.client.expire(self._k("DAILY", date), TTL_SECONDS)
        except Exception:
            pass


def make_collector(redis_client: Any, enabled: bool = True) -> MetricsCollector:
    """Factory. ``redis_client`` is a ready-to-use ``redis.Redis``."""
    return MetricsCollector(client=redis_client, enabled=enabled)
