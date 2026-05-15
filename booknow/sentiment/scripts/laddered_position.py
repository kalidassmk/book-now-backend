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

# Redis keys — 2026-05-11 iter 2: switched from single-key to per-symbol
# hash so up to N ladders can run concurrently (operator chose max=3).
LADDER_STATES_HASH       = "SCALPER:LADDER_STATES"         # Fast Scalper hash
VIRTUAL_LADDER_STATES_HASH = "VIRTUAL:LADDER_STATES"       # Virtual Scalper hash
# Both hashes are treated together for global capacity decisions.
ALL_LADDER_HASHES        = (LADDER_STATES_HASH, VIRTUAL_LADDER_STATES_HASH)

# Legacy single-key fallbacks kept for one-shot migration paths.
LADDER_STATE_KEY        = "SCALPER:LADDER_STATE"
LADDER_ACTIVE_SYMBOL    = "SCALPER:LADDER_ACTIVE_SYMBOL"   # legacy single-coin lock

# Cooldown — after a ladder closes (TP or hard stop), the same coin is
# blocked for COOLDOWN_SECONDS so the bot doesn't immediately re-enter
# the same trade. Implemented as a Redis key with TTL.
COOLDOWN_KEY_PREFIX     = "SCALPER:LADDER_COOLDOWN:"
DEFAULT_COOLDOWN_SECONDS = 4 * 3600   # 4 hours


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
    # 2026-05-12 iter 15: trailing-TP state. Once price exceeds tp_target,
    # we cancel the limit TP and start trailing the running peak. Exit when
    # current_price <= peak × (1 - ladderTrailingTpPct/100). Lets winners
    # run beyond the static TP and capture the bigger-move upside.
    trailing_active: bool = False
    peak_price_since_tp: float = 0.0
    # 2026-05-13 iter 16: pending-pump-dump tracker. While Buy 1 is a
    # resting LIMIT and price has been ABOVE the limit (limit not filling
    # because market is too high), we track the running peak. If the peak
    # exceeds limit by pending_pump_threshold% AND then price drops
    # pending_dump_from_peak% from that peak, we cancel before the limit
    # fills into a falling knife (GIGGLE/USDT 2026-05-13 pattern).
    peak_since_signal: float = 0.0
    closed_ts: int = 0
    exit_reason: str = ""
    # 2026-05-15 iter 39: per-minute USDT volume baseline from the
    # 60 min BEFORE Buy 1 was placed. Used by the liquidity-death
    # exit to score "vol_now vs vol_pre" — the strongest predictor
    # of "this coin won't pump back" identified in the HBAR/QNT/FLOKI
    # forensic. Defaults to 0 if features didn't include it (old states).
    pre_vol_baseline_usdt: float = 0.0
    # 2026-05-15 iter 43: Volatility-Adaptive Entry + TP. Set at ladder
    # start when adaptive mode is on. 0 means "use static config".
    dyn_buy1_offset_pct: float = 0.0
    dyn_buy2_offset_pct: float = 0.0
    dyn_tp_target_usdt: float = 0.0
    dyn_strategy: str = ""           # CALM / NORMAL / VOLATILE / X_VOLATILE / STATIC

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
            "trailing_active": self.trailing_active,
            "peak_price_since_tp": self.peak_price_since_tp,
            "peak_since_signal": self.peak_since_signal,
            "closed_ts": self.closed_ts,
            "exit_reason": self.exit_reason,
            "pre_vol_baseline_usdt": self.pre_vol_baseline_usdt,
            "dyn_buy1_offset_pct": self.dyn_buy1_offset_pct,
            "dyn_buy2_offset_pct": self.dyn_buy2_offset_pct,
            "dyn_tp_target_usdt": self.dyn_tp_target_usdt,
            "dyn_strategy": self.dyn_strategy,
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
            trailing_active=bool(d.get("trailing_active") or False),
            peak_price_since_tp=float(d.get("peak_price_since_tp") or 0),
            peak_since_signal=float(d.get("peak_since_signal") or 0),
            closed_ts=int(d.get("closed_ts") or 0),
            exit_reason=d.get("exit_reason") or "",
            pre_vol_baseline_usdt=float(d.get("pre_vol_baseline_usdt") or 0),
            dyn_buy1_offset_pct=float(d.get("dyn_buy1_offset_pct") or 0),
            dyn_buy2_offset_pct=float(d.get("dyn_buy2_offset_pct") or 0),
            dyn_tp_target_usdt=float(d.get("dyn_tp_target_usdt") or 0),
            dyn_strategy=d.get("dyn_strategy") or "",
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


def required_tp_pct_for_net_profit(buy_size_usdt: float, target_net_usdt: float,
                                    fee_rate_per_side: float) -> float:
    """Reverse-engineer the TP percentage that nets `target_net_usdt`
    after both sides of fees at `fee_rate_per_side` (e.g. 0.00075 = 0.075%).

    Formula: tp_pct = (target_net / buy_size) × 100 + 2 × fee_rate × 100

    For target=$0.05, buy=$12, fee=0.075%:
      = (0.05/12)×100 + 0.15 = 0.4167 + 0.15 = 0.567 %
    """
    if buy_size_usdt <= 0 or target_net_usdt <= 0:
        return 0.0
    profit_pct = (target_net_usdt / buy_size_usdt) * 100.0
    fee_burden = 2 * fee_rate_per_side * 100.0
    return profit_pct + fee_burden


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


# ── Redis persistence layer (multi-ladder hash storage) ──────────────────

def load_state(redis_client, symbol: str) -> Optional[LadderState]:
    """Fetch the ladder for one symbol, or None if not active."""
    if not redis_client or not symbol:
        return None
    try:
        raw = redis_client.hget(LADDER_STATES_HASH, symbol)
    except Exception as exc:
        logger.debug("ladder load failed: %s", exc)
        return None
    if not raw: return None
    try:
        return LadderState.from_dict(json.loads(raw))
    except Exception as exc:
        logger.warning("corrupt ladder state for %s, discarding: %s", symbol, exc)
        try:
            redis_client.hdel(LADDER_STATES_HASH, symbol)
        except Exception:
            pass
        return None


def save_state(redis_client, state: LadderState) -> None:
    if not redis_client or not state.symbol: return
    try:
        if state.state == CLOSED:
            redis_client.hdel(LADDER_STATES_HASH, state.symbol)
        else:
            redis_client.hset(LADDER_STATES_HASH, state.symbol, json.dumps(state.to_dict()))
    except Exception as exc:
        logger.debug("ladder save failed: %s", exc)


def clear_state(redis_client, symbol: str) -> None:
    if not redis_client or not symbol: return
    try:
        redis_client.hdel(LADDER_STATES_HASH, symbol)
    except Exception:
        pass


def list_active_symbols(redis_client) -> List[str]:
    """All symbols with an active ladder right now."""
    if not redis_client: return []
    try:
        keys = redis_client.hkeys(LADDER_STATES_HASH)
        return list(keys) if keys else []
    except Exception:
        return []


def count_active(redis_client) -> int:
    if not redis_client: return 0
    try:
        return redis_client.hlen(LADDER_STATES_HASH) or 0
    except Exception:
        return 0


def load_all_states(redis_client) -> List[LadderState]:
    """All active LadderState records, ready to iterate per loop tick."""
    if not redis_client: return []
    try:
        all_raw = redis_client.hgetall(LADDER_STATES_HASH)
    except Exception:
        return []
    out: List[LadderState] = []
    for sym, raw in (all_raw or {}).items():
        try:
            out.append(LadderState.from_dict(json.loads(raw)))
        except Exception:
            try:
                redis_client.hdel(LADDER_STATES_HASH, sym)
            except Exception:
                pass
    return out


# ── Back-compat shims (callers in mid-update use these) ──────────────────

def is_active(redis_client, symbol: Optional[str] = None) -> bool:
    """If symbol is given, returns whether that symbol has an active ladder.
    If omitted, returns whether ANY ladder is active."""
    if symbol:
        return load_state(redis_client, symbol) is not None
    return count_active(redis_client) > 0


def active_symbol(redis_client) -> Optional[str]:
    """First active symbol — kept for backward-compatible callers that only
    cared about presence/identity in single-coin mode."""
    syms = list_active_symbols(redis_client)
    return syms[0] if syms else None


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


# ── Per-symbol cooldown helpers ──────────────────────────────────────────

def set_cooldown(redis_client, symbol: str, seconds: int = DEFAULT_COOLDOWN_SECONDS) -> None:
    """Mark this symbol as 'just-traded' — block new ladders for `seconds`."""
    if not redis_client or not symbol or seconds <= 0:
        return
    try:
        key = COOLDOWN_KEY_PREFIX + symbol
        redis_client.set(key, "1", ex=seconds)
    except Exception:
        pass


def is_on_cooldown(redis_client, symbol: str) -> bool:
    """True if the symbol's cooldown is still active."""
    if not redis_client or not symbol:
        return False
    try:
        return bool(redis_client.exists(COOLDOWN_KEY_PREFIX + symbol))
    except Exception:
        return False


def cooldown_remaining_seconds(redis_client, symbol: str) -> int:
    """Seconds left on the cooldown (0 if not active or unknown)."""
    if not redis_client or not symbol:
        return 0
    try:
        ttl = redis_client.ttl(COOLDOWN_KEY_PREFIX + symbol)
        return max(0, int(ttl)) if ttl is not None else 0
    except Exception:
        return 0


# ── Combined (Fast + Virtual) helpers for global capacity decisions ──────

def list_active_symbols_combined(redis_client) -> List[str]:
    """Symbols with an active ladder in EITHER scalper's hash. Used for the
    global concurrent-trade cap (2026-05-11 iter 5: cap is now total
    across the whole system, not per-scalper)."""
    if not redis_client: return []
    out = set()
    for hash_name in ALL_LADDER_HASHES:
        try:
            keys = redis_client.hkeys(hash_name) or []
            out.update(keys)
        except Exception:
            pass
    return list(out)


def count_total_active(redis_client) -> int:
    """Total ladders in flight across both Fast and Virtual Scalpers."""
    return len(list_active_symbols_combined(redis_client))


def is_active_anywhere(redis_client, symbol: str) -> bool:
    """True if either scalper currently has a ladder for this symbol."""
    return symbol in list_active_symbols_combined(redis_client)
