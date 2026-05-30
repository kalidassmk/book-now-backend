"""
order_flow.py
─────────────────────────────────────────────────────────────────────────────
Per-symbol order-flow analysis implementing the scalper checklist.

Ingests Binance ``aggTrade`` events (market buys/sells) and ``depth`` snapshots
(order book) and continuously evaluates the order-flow checklist scalpers use:

Before BUYING:
    ✅ Delta turning positive
    ✅ Market buys increasing
    ✅ Buy wall below price
    ✅ No large sell wall above
    ✅ Volume spike

Before SELLING:
    ✅ Delta negative
    ✅ Market sells increasing
    ✅ Large sell wall above
    ✅ Buy wall disappears

When every condition on a side is met the analyzer emits BUY / SELL; otherwise
HOLD.

Trade-side convention (Binance ``aggTrade``):
    ``m`` (isBuyerMaker) == True  → aggressor is a SELLER → market SELL
    ``m`` (isBuyerMaker) == False → aggressor is a BUYER  → market BUY
"""

from __future__ import annotations

import time
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Tuple

from booknow.scalper.config import ScalperConfig


class OrderFlowAnalyzer:
    """Maintains rolling order-flow state for a single symbol."""

    def __init__(self, symbol: str, config: ScalperConfig):
        self.symbol = symbol.upper()
        self.config = config

        # (ts_sec, qty, quote_qty, is_buy) for every aggregated trade.
        self._trades: Deque[Tuple[float, float, float, bool]] = deque()

        # Latest order book snapshot: sorted lists of (price, qty).
        self.bids: List[Tuple[float, float]] = []
        self.asks: List[Tuple[float, float]] = []
        self.last_price: Optional[float] = None

        # Wall state carried between evaluations (for "buy wall disappears").
        self._prev_had_buy_wall: bool = False

        # Cached snapshot of the most recent evaluation.
        self.snapshot: Dict[str, Any] = {}

    # ── Ingest ───────────────────────────────────────────────────────────

    def on_trade(self, data: Dict[str, Any]) -> None:
        """Handle a Binance ``aggTrade`` payload."""
        try:
            price = float(data["p"])
            qty = float(data["q"])
            # Event time is in ms; fall back to local clock if missing.
            ts = float(data.get("T", time.time() * 1000)) / 1000.0
            is_buyer_maker = bool(data.get("m", False))
        except (KeyError, TypeError, ValueError):
            return

        is_buy = not is_buyer_maker  # aggressor bought when buyer is NOT maker
        self.last_price = price
        self._trades.append((ts, qty, qty * price, is_buy))
        self._evict_old(ts)

    def on_depth(self, data: Dict[str, Any]) -> None:
        """Handle a Binance partial-book ``depth`` payload."""
        bids = data.get("bids") or data.get("b")
        asks = data.get("asks") or data.get("a")
        if bids is None or asks is None:
            return
        try:
            self.bids = [(float(p), float(q)) for p, q in bids if float(q) > 0]
            self.asks = [(float(p), float(q)) for p, q in asks if float(q) > 0]
        except (TypeError, ValueError):
            return
        # Keep the book sorted: bids high→low, asks low→high.
        self.bids.sort(key=lambda x: x[0], reverse=True)
        self.asks.sort(key=lambda x: x[0])

    def _evict_old(self, now: float) -> None:
        """Drop trades older than the baseline horizon."""
        cutoff = now - self.config.baseline_sec
        while self._trades and self._trades[0][0] < cutoff:
            self._trades.popleft()

    # ── Metrics ──────────────────────────────────────────────────────────

    def _window_stats(self, start: float, end: float) -> Dict[str, Any]:
        """Aggregate buy/sell volume and counts within [start, end)."""
        buy_vol = sell_vol = 0.0
        buy_n = sell_n = 0
        for ts, qty, _quote, is_buy in self._trades:
            if start <= ts < end:
                if is_buy:
                    buy_vol += qty
                    buy_n += 1
                else:
                    sell_vol += qty
                    sell_n += 1
        return {
            "buy_vol": buy_vol,
            "sell_vol": sell_vol,
            "buy_n": buy_n,
            "sell_n": sell_n,
            "total_vol": buy_vol + sell_vol,
            "trades": buy_n + sell_n,
            "delta": buy_vol - sell_vol,
        }

    def _detect_walls(self) -> Dict[str, Any]:
        """Identify large bid (buy) and ask (sell) walls in the visible book."""

        def biggest(levels: List[Tuple[float, float]]):
            if not levels:
                return None, 0.0, 0.0
            avg = sum(q for _, q in levels) / len(levels)
            price, qty = max(levels, key=lambda x: x[1])
            return price, qty, avg

        bid_price, bid_qty, bid_avg = biggest(self.bids)
        ask_price, ask_qty, ask_avg = biggest(self.asks)

        mult = self.config.wall_multiple
        has_buy_wall = bid_avg > 0 and bid_qty >= mult * bid_avg
        has_sell_wall = ask_avg > 0 and ask_qty >= mult * ask_avg

        return {
            "has_buy_wall": has_buy_wall,
            "buy_wall_price": bid_price if has_buy_wall else None,
            "buy_wall_qty": bid_qty if has_buy_wall else 0.0,
            "has_sell_wall": has_sell_wall,
            "sell_wall_price": ask_price if has_sell_wall else None,
            "sell_wall_qty": ask_qty if has_sell_wall else 0.0,
        }

    # ── Evaluate ─────────────────────────────────────────────────────────

    def evaluate(self, now: Optional[float] = None) -> Dict[str, Any]:
        """Compute all checklist conditions and the resulting signal."""
        now = now if now is not None else time.time()
        w = self.config.window_sec

        cur = self._window_stats(now - w, now)
        prev = self._window_stats(now - 2 * w, now - w)

        # Baseline average per-window volume over the longer horizon.
        baseline = self._window_stats(now - self.config.baseline_sec, now)
        n_windows = max(self.config.baseline_sec / w, 1.0)
        avg_window_vol = baseline["total_vol"] / n_windows

        walls = self._detect_walls()

        # ── BUY checklist ──────────────────────────────────────────────
        delta_positive = cur["delta"] > 0
        delta_rising = cur["delta"] > prev["delta"]
        delta_turning_positive = delta_positive and delta_rising

        market_buys_increasing = cur["buy_vol"] > prev["buy_vol"] and cur["buy_n"] > 0
        buy_wall_below = walls["has_buy_wall"]
        no_large_sell_wall_above = not walls["has_sell_wall"]
        volume_spike = (
            avg_window_vol > 0
            and cur["total_vol"] >= self.config.volume_spike_multiple * avg_window_vol
        )

        # ── SELL checklist ─────────────────────────────────────────────
        delta_negative_turning = cur["delta"] < 0 and cur["delta"] < prev["delta"]
        market_sells_increasing = cur["sell_vol"] > prev["sell_vol"] and cur["sell_n"] > 0
        large_sell_wall_above = walls["has_sell_wall"]
        buy_wall_disappeared = self._prev_had_buy_wall and not walls["has_buy_wall"]

        enough_flow = cur["trades"] >= self.config.min_trades

        buy_conditions = {
            "delta_turning_positive": delta_turning_positive,
            "market_buys_increasing": market_buys_increasing,
            "buy_wall_below_price": buy_wall_below,
            "no_large_sell_wall_above": no_large_sell_wall_above,
            "volume_spike": volume_spike,
        }
        sell_conditions = {
            "delta_negative": delta_negative_turning,
            "market_sells_increasing": market_sells_increasing,
            "large_sell_wall_above": large_sell_wall_above,
            "buy_wall_disappears": buy_wall_disappeared,
        }

        buy_ready = enough_flow and all(buy_conditions.values())
        sell_ready = enough_flow and all(sell_conditions.values())

        if buy_ready:
            signal = "BUY"
        elif sell_ready:
            signal = "SELL"
        else:
            signal = "HOLD"

        # Persist wall state for the next "buy wall disappears" check.
        self._prev_had_buy_wall = walls["has_buy_wall"]

        self.snapshot = {
            "symbol": self.symbol,
            "timestamp": now,
            "last_price": self.last_price,
            "signal": signal,
            "buy_score": sum(buy_conditions.values()),
            "buy_score_max": len(buy_conditions),
            "sell_score": sum(sell_conditions.values()),
            "sell_score_max": len(sell_conditions),
            "buy_conditions": buy_conditions,
            "sell_conditions": sell_conditions,
            "metrics": {
                "delta": round(cur["delta"], 6),
                "delta_prev": round(prev["delta"], 6),
                "buy_volume": round(cur["buy_vol"], 6),
                "sell_volume": round(cur["sell_vol"], 6),
                "buy_trades": cur["buy_n"],
                "sell_trades": cur["sell_n"],
                "window_volume": round(cur["total_vol"], 6),
                "avg_window_volume": round(avg_window_vol, 6),
                "volume_ratio": round(cur["total_vol"] / avg_window_vol, 3)
                if avg_window_vol > 0
                else 0.0,
            },
            "walls": walls,
            "enough_flow": enough_flow,
        }
        return self.snapshot
