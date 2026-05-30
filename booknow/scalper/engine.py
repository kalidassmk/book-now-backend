"""
engine.py
─────────────────────────────────────────────────────────────────────────────
Scalper engine — wires the order-flow stream to per-symbol analyzers.

Owns one :class:`OrderFlowStreamService` and one :class:`OrderFlowAnalyzer` per
symbol, plus a 1 Hz evaluation loop that refreshes every snapshot and records
signal transitions. The HTTP layer reads live snapshots off this engine; main.py
starts/stops it alongside the other WS services.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from typing import Any, Deque, Dict, List, Optional

from booknow.scalper.config import ScalperConfig
from booknow.scalper.order_flow import OrderFlowAnalyzer
from booknow.scalper.stream import OrderFlowStreamService


logger = logging.getLogger("booknow.scalper.engine")


class ScalperEngine:
    """Boot, run, and tear down the order-flow analysis fleet."""

    def __init__(self, config: Optional[ScalperConfig] = None):
        self.config = config or ScalperConfig()
        self.analyzers: Dict[str, OrderFlowAnalyzer] = {
            sym: OrderFlowAnalyzer(sym, self.config) for sym in self.config.symbols
        }
        self.stream = OrderFlowStreamService(self.config, self._on_event)
        self._eval_task: Optional[asyncio.Task] = None
        self._running = False
        self.started_at: Optional[float] = None

        # Rolling log of the most recent signal changes across all symbols.
        self.signal_log: Deque[Dict[str, Any]] = deque(maxlen=100)
        self._last_signal: Dict[str, str] = {}

    # ── Stream callback ──────────────────────────────────────────────────

    def _on_event(self, symbol: str, event_type: str, data: Dict[str, Any]) -> None:
        analyzer = self.analyzers.get(symbol)
        if analyzer is None:
            return
        if event_type == "aggTrade":
            analyzer.on_trade(data)
        elif event_type == "depth":
            analyzer.on_depth(data)

    # ── Lifecycle ────────────────────────────────────────────────────────

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self.started_at = time.time()
        await self.stream.start()
        self._eval_task = asyncio.create_task(self._evaluation_loop(), name="scalper-eval")
        logger.info("[ScalperEngine] started for %s", ", ".join(self.config.symbols))

    async def stop(self) -> None:
        self._running = False
        if self._eval_task is not None:
            self._eval_task.cancel()
            try:
                await self._eval_task
            except (asyncio.CancelledError, Exception):
                pass
            self._eval_task = None
        await self.stream.stop()
        logger.info("[ScalperEngine] stopped")

    async def _evaluation_loop(self) -> None:
        """Re-evaluate every symbol on a fixed cadence; log signal changes."""
        try:
            while self._running:
                now = time.time()
                for symbol, analyzer in self.analyzers.items():
                    snap = analyzer.evaluate(now)
                    self._record_signal(symbol, snap)
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            pass

    def _record_signal(self, symbol: str, snap: Dict[str, Any]) -> None:
        signal = snap.get("signal", "HOLD")
        if signal != "HOLD" and self._last_signal.get(symbol) != signal:
            self.signal_log.appendleft(
                {
                    "symbol": symbol,
                    "signal": signal,
                    "price": snap.get("last_price"),
                    "timestamp": snap.get("timestamp"),
                    "metrics": snap.get("metrics", {}),
                }
            )
            logger.info(
                "[ScalperEngine] %s → %s @ %s (delta=%s vol_ratio=%s)",
                symbol, signal, snap.get("last_price"),
                snap.get("metrics", {}).get("delta"),
                snap.get("metrics", {}).get("volume_ratio"),
            )
        self._last_signal[symbol] = signal

    # ── Readers (used by the HTTP layer) ─────────────────────────────────

    def get_snapshot(self, symbol: str) -> Optional[Dict[str, Any]]:
        analyzer = self.analyzers.get(symbol.upper())
        return analyzer.snapshot if analyzer else None

    def get_all_snapshots(self) -> List[Dict[str, Any]]:
        return [a.snapshot for a in self.analyzers.values() if a.snapshot]

    def recent_signals(self, limit: int = 50) -> List[Dict[str, Any]]:
        return list(self.signal_log)[:limit]

    def status(self) -> Dict[str, Any]:
        return {
            "running": self._running,
            "connected": self.stream.connected,
            "symbols": self.config.symbols,
            "tiers": self.config.tiers,
            "tier_order": self.config.tier_order,
            "started_at": self.started_at,
            "uptime_sec": round(time.time() - self.started_at, 1) if self.started_at else 0,
            "last_message_age_sec": round(time.time() - self.stream.last_message_ts, 2)
            if self.stream.last_message_ts
            else None,
            "config": {
                "window_sec": self.config.window_sec,
                "baseline_sec": self.config.baseline_sec,
                "wall_multiple": self.config.wall_multiple,
                "volume_spike_multiple": self.config.volume_spike_multiple,
                "min_trades": self.config.min_trades,
            },
        }
