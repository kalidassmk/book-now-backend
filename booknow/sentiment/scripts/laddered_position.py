"""laddered_position.py
─────────────────────────────────────────────────────────────────────────────
Laddered Recovery state machine: a single-coin, 3-tier averaging-down entry
with cancel-buy-3-on-buy-2-fill and hard-stop-only-after-buy-3 logic.

The state lives in Redis under SCALPER:LADDER_STATE so a restart doesn't
lose track of in-flight orders. The Fast / Virtual Scalpers call
:func:`new_position` once on signal, then :func:`tick` each loop iteration
to react to fills. Both can share the same persistence layer — only the
execution side differs (real Binance vs paper).

States
──────
  PENDING_BUY_1  buy 1 placed, waiting for fill
  ACTIVE_1       buy 1 filled; buy 2 and buy 3 limits resting; TP order live
  ACTIVE_2       buy 2 filled, buy 3 cancelled; TP refreshed at new avg
  ACTIVE_3       buy 3 filled (gap scenario); hard stop now monitored
  EXITING        TP or hard stop fired, finalising
  CLOSED         all units sold; ready for next signal

Helpers
───────
  weighted_avg(legs) → float
  tp_price(avg, tp_pct, tick_size) → float
  hard_stop_price(buy_3_price, stop_pct, tick_size) → float
  serialise(state) / deserialise(raw) — for Redis round-trip
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, asdict, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger("booknow.ladder")

# State constants
PENDING_BUY_1 = "PENDING_BUY_1"
ACTIVE_1      = "ACTIVE_1"
ACTIVE_2      = "ACTIVE_2"
ACTIVE_3      = "ACTIVE_3"
EXITING       = "EXITING"
CLOSED        = "CLOSED"

# Exit reason tags
EXIT_TP   = "tp_at_avg"
EXIT_STOP = "hard_stop_buy3"
EXIT_MANUAL = "manual"

# Redis key
LADDER_STATE_KEY        = "SCALPER:LADDER_STATE"
LADDER_ACTIVE_SYMBOL    = "SCALPER:LADDER_ACTIVE_SYMBOL"   # single-coin lock


@dataclass
class Leg:
    """One rung of the ladder."""
    label: str              # 'buy_1' / 'buy_2' / 'buy_3'
    target_price: float     # the price we want to fill at
    size_usdt: float        # USDT we want to spend on this rung
    order_id: Optional[str] = None
    qty_filled: float = 0.0
    fill_price: float = 0.0   # weighted-avg fill price for the rung
    fill_ts: int = 0          # epoch-ms
    status: str = "pending"   # pending | filled | cancelled | failed


@dataclass
class LadderState:
    """All state for one active ladder. Persisted to Redis between ticks."""
    symbol: str
    signal_price: float
    signal_ts: int            # epoch-ms when signal fired
    state: str = PENDING_BUY_1
    buy_1: Optional[Leg] = None
    buy_2: Optional[Leg] = None
    buy_3: Optional[Leg] = None
    tp_order_id: Optional[str] = None
    tp_target_price: float = 0.0
    hard_stop_price: float = 0.0
    below_avg_started_ts: int = 0   # for Time-to-Break-Even metric
    total_underwater_ms: int = 0    # accumulator for TBE
    recovered_to_break_even: bool = False
    closed_ts: int = 0
    exit_reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "symbol": self.symbol,
            "signal_price": self.signal_price,
            "signal_ts": self.signal_ts,
            "state": self.state,
            "tp_order_id": self.tp_order_id,
            "tp_target_price": self.tp_target_price,
            "hard_stop_price": self.hard_stop_price,
            "below_avg_started_ts": self.below_avg_started_ts,
            "total_underwater_ms": self.total_underwater_ms,
            "recovered_to_break_even": self.recovered_to_break_even,
            "closed_ts": self.closed_ts,
            "exit_reason": self.exit_reason,
        }
        for tag, leg in (("buy_1", self.buy_1), ("buy_2", self.buy_2), ("buy_3", self.buy_3)):
            d[tag] = asdict(leg) if leg is not None else None
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "LadderState":
        def _leg(x):
            return Leg(**x) if x else None
        return cls(
            symbol=d["symbol"],
            signal_price=float(d.get("signal_price") or 0),
            signal_ts=int(d.get("signal_ts") or 0),
            state=d.get("state") or PENDING_BUY_1,
            buy_1=_leg(d.get("buy_1")),
            buy_2=_leg(d.get("buy_2")),
            buy_3=_leg(d.get("buy_3")),
            tp_order_id=d.get("tp_order_id"),
            tp_target_price=float(d.get("tp_target_price") or 0),
            hard_stop_price=float(d.get("hard_stop_price") or 0),
            below_avg_started_ts=int(d.get("below_avg_started_ts") or 0),
            total_underwater_ms=int(d.get("total_underwater_ms") or 0),
            recovered_to_break_even=bool(d.get("recovered_to_break_even") or False),
            closed_ts=int(d.get("closed_ts") or 0),
            exit_reason=d.get("exit_reason") or "",
        )

    # ── helpers ──────────────────────────────────────────────────────────
    def filled_legs(self) -> List[Leg]:
        return [l for l in (self.buy_1, self.buy_2, self.buy_3) if l and l.status == "filled" and l.qty_filled > 0]

    def total_qty(self) -> float:
        return sum(l.qty_filled for l in self.filled_legs())

    def total_cost(self) -> float:
        return sum(l.qty_filled * l.fill_price for l in self.filled_legs())

    def weighted_avg(self) -> float:
        q = self.total_qty()
        return (self.total_cost() / q) if q > 0 else 0.0

    def total_invested_usdt(self) -> float:
        return sum(l.size_usdt for l in self.filled_legs())


def tp_price(avg: float, tp_pct: float, tick_size: float = 0.00000001) -> float:
    """Compute TP price = avg × (1 + tp_pct/100), rounded down to tick."""
    raw = avg * (1 + tp_pct / 100.0)
    if tick_size and tick_size > 0:
        return _round_down(raw, tick_size)
    return raw


def hard_stop_price(buy_3_price: float, stop_pct: float, tick_size: float = 0.00000001) -> float:
    """Hard stop = buy_3 × (1 - stop_pct/100)."""
    raw = buy_3_price * (1 - stop_pct / 100.0)
    if tick_size and tick_size > 0:
        return _round_down(raw, tick_size)
    return raw


def _round_down(value: float, step: float) -> float:
    """Round value DOWN to the nearest multiple of step."""
    if step <= 0: return value
    return (int(value / step)) * step


# ── Redis persistence layer ──────────────────────────────────────────────

def load_state(redis_client) -> Optional[LadderState]:
    """Fetch the active ladder from Redis, or None if there isn't one."""
    if not redis_client:
        return None
    try:
        raw = redis_client.get(LADDER_STATE_KEY)
    except Exception as exc:
        logger.debug("ladder load failed: %s", exc)
        return None
    if not raw: return None
    try:
        return LadderState.from_dict(json.loads(raw))
    except Exception as exc:
        logger.warning("corrupt ladder state, discarding: %s", exc)
        return None


def save_state(redis_client, state: LadderState) -> None:
    if not redis_client: return
    try:
        redis_client.set(LADDER_STATE_KEY, json.dumps(state.to_dict()))
        # Also mirror active symbol for fast lookup
        if state.state not in (CLOSED, EXITING):
            redis_client.set(LADDER_ACTIVE_SYMBOL, state.symbol)
        else:
            redis_client.delete(LADDER_ACTIVE_SYMBOL)
    except Exception as exc:
        logger.debug("ladder save failed: %s", exc)


def clear_state(redis_client) -> None:
    if not redis_client: return
    try:
        redis_client.delete(LADDER_STATE_KEY)
        redis_client.delete(LADDER_ACTIVE_SYMBOL)
    except Exception:
        pass


def active_symbol(redis_client) -> Optional[str]:
    """Cheap lookup: is a ladder currently in flight, and for what symbol?"""
    if not redis_client: return None
    try:
        val = redis_client.get(LADDER_ACTIVE_SYMBOL)
        return val if val else None
    except Exception:
        return None


def is_active(redis_client) -> bool:
    return active_symbol(redis_client) is not None


# ── Closed-trade summary for metrics ─────────────────────────────────────

def summarise_closed_trade(state: LadderState, exit_price: float) -> Dict[str, Any]:
    """Build a dict suitable for METRICS:LADDER:{date} list push."""
    avg = state.weighted_avg()
    qty = state.total_qty()
    invested = state.total_invested_usdt()
    exit_value = exit_price * qty
    # Binance spot fees: 0.1% each side
    fees = (invested + exit_value) * 0.001
    net_pnl = (exit_value - invested) - fees

    # RER: did we recover to break-even after buy 2 or buy 3 fired?
    rer_eligible = (state.buy_2 and state.buy_2.status == "filled") or (state.buy_3 and state.buy_3.status == "filled")

    # DM (Drawdown Multiplier): ratio of buy_3 qty (or buy_2 qty if buy_3 didn't fill) to buy_1 qty
    dm = 1.0
    if state.buy_1 and state.buy_1.qty_filled > 0:
        if state.buy_3 and state.buy_3.qty_filled > 0:
            dm = state.buy_3.qty_filled / state.buy_1.qty_filled
        elif state.buy_2 and state.buy_2.qty_filled > 0:
            dm = state.buy_2.qty_filled / state.buy_1.qty_filled

    # TBE: total underwater minutes
    tbe_min = state.total_underwater_ms / 60_000.0 if state.total_underwater_ms else 0.0

    return {
        "symbol": state.symbol,
        "signal_price": state.signal_price,
        "signal_ts": state.signal_ts,
        "closed_ts": int(time.time() * 1000),
        "exit_reason": state.exit_reason,
        "exit_price": exit_price,
        "buys_filled": len(state.filled_legs()),
        "weighted_avg": avg,
        "qty": qty,
        "invested_usdt": invested,
        "exit_value_usdt": exit_value,
        "fees_usdt": fees,
        "net_pnl_usdt": net_pnl,
        # New strategy-specific metrics
        "rer_eligible": rer_eligible,
        "rer_recovered": state.recovered_to_break_even,
        "tbe_minutes": tbe_min,
        "drawdown_multiplier": dm,
    }
