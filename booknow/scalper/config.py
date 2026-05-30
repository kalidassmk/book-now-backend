"""
config.py
─────────────────────────────────────────────────────────────────────────────
Tunable parameters for the order-flow scalper.

These are runtime-tunable thresholds for the analyzer — kept as a small
dataclass (rather than in :mod:`booknow.config.settings`) so the scalper can be
embedded and tuned independently. All values fall back to env vars so they can
be set without code changes; the engine reads them once at bootstrap.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import List


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _default_symbols() -> List[str]:
    raw = os.environ.get("SCALPER_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT")
    return [s.strip().upper() for s in raw.split(",") if s.strip()]


@dataclass
class ScalperConfig:
    """Tunable parameters for the order-flow analyzer."""

    # Symbols to stream (Binance spot, e.g. BTCUSDT).
    symbols: List[str] = field(default_factory=_default_symbols)

    # Binance combined-stream websocket base URL.
    ws_base: str = os.environ.get(
        "BINANCE_WS_BASE", "wss://stream.binance.com:9443/stream"
    )

    # Depth stream levels and update speed (20 levels @ 100ms is plenty).
    depth_levels: int = _env_int("SCALPER_DEPTH_LEVELS", 20)
    depth_speed_ms: int = _env_int("SCALPER_DEPTH_SPEED_MS", 100)

    # Rolling window (seconds) used to measure delta, trade flow and volume.
    window_sec: float = _env_float("SCALPER_WINDOW_SEC", 5.0)

    # Longer baseline (seconds) used to detect a volume spike vs. normal flow.
    baseline_sec: float = _env_float("SCALPER_BASELINE_SEC", 60.0)

    # A price level counts as a "wall" when its size is this multiple of the
    # average size across the visible book on that side.
    wall_multiple: float = _env_float("SCALPER_WALL_MULTIPLE", 3.0)

    # A volume "spike" is current-window volume above this multiple of the
    # average per-window volume over the baseline.
    volume_spike_multiple: float = _env_float("SCALPER_VOLUME_SPIKE_MULTIPLE", 2.0)

    # Minimum number of trades in the window before signals are valid (avoids
    # firing on a single print in a thin window).
    min_trades: int = _env_int("SCALPER_MIN_TRADES", 5)

    def stream_path(self) -> str:
        """Build the combined-stream query path for all configured symbols."""
        streams: List[str] = []
        for sym in self.symbols:
            s = sym.lower()
            streams.append(f"{s}@aggTrade")
            streams.append(f"{s}@depth{self.depth_levels}@{self.depth_speed_ms}ms")
        return "/".join(streams)

    def ws_url(self) -> str:
        return f"{self.ws_base}?streams={self.stream_path()}"
