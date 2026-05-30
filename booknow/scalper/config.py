"""
config.py
─────────────────────────────────────────────────────────────────────────────
Tunable parameters for the order-flow scalper.

These are runtime-tunable thresholds for the analyzer — kept as a small
dataclass (rather than in :mod:`booknow.config.settings`) so the scalper can be
embedded and tuned independently. All values fall back to env vars so they can
be set without code changes; the engine reads them once at bootstrap.

Symbols are organised into **market-cap tiers** (Large / Mid / Small Cap) so the
dashboard can show all major coins segregated by tier. The full ``symbols`` list
is just the union of every tier.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Dict, List


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


# Curated major Binance USDT spot pairs, grouped by market-cap tier. Order
# within each tier is roughly descending market cap. Adjust via SCALPER_TIERS
# (JSON) or SCALPER_SYMBOLS (flat list -> one "Major Coins" tier).
DEFAULT_TIERS: Dict[str, List[str]] = {
    "Large Cap": [
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT",
        "XRPUSDT", "DOGEUSDT", "ADAUSDT", "TRXUSDT",
    ],
    "Mid Cap": [
        "AVAXUSDT", "LINKUSDT", "DOTUSDT", "LTCUSDT",
        "BCHUSDT", "NEARUSDT", "UNIUSDT", "ATOMUSDT",
    ],
    "Small Cap": [
        "APTUSDT", "ARBUSDT", "OPUSDT", "FILUSDT",
        "INJUSDT", "SUIUSDT", "SEIUSDT", "TIAUSDT",
    ],
}


def _default_tiers() -> Dict[str, List[str]]:
    """Build the tier map from env, falling back to the curated defaults.

    Precedence:
      1. ``SCALPER_TIERS`` — a JSON object ``{"Tier": ["SYM", ...], ...}``.
      2. ``SCALPER_SYMBOLS`` — a flat comma list, placed under "Major Coins".
      3. :data:`DEFAULT_TIERS`.
    """
    raw_tiers = os.environ.get("SCALPER_TIERS")
    if raw_tiers:
        try:
            parsed = json.loads(raw_tiers)
            tiers = {
                str(label): [str(s).strip().upper() for s in syms if str(s).strip()]
                for label, syms in parsed.items()
            }
            if any(tiers.values()):
                return tiers
        except (json.JSONDecodeError, AttributeError, TypeError):
            pass

    raw_syms = os.environ.get("SCALPER_SYMBOLS")
    if raw_syms:
        syms = [s.strip().upper() for s in raw_syms.split(",") if s.strip()]
        if syms:
            return {"Major Coins": syms}

    return {label: list(syms) for label, syms in DEFAULT_TIERS.items()}


def _flatten_tiers(tiers: Dict[str, List[str]]) -> List[str]:
    """Union of all tier symbols, de-duplicated, preserving first-seen order."""
    seen: Dict[str, None] = {}
    for syms in tiers.values():
        for s in syms:
            seen.setdefault(s, None)
    return list(seen.keys())


def _default_symbols() -> List[str]:
    return _flatten_tiers(_default_tiers())


@dataclass
class ScalperConfig:
    """Tunable parameters for the order-flow analyzer."""

    # Market-cap tiers -> symbols. The dashboard renders one section per tier.
    tiers: Dict[str, List[str]] = field(default_factory=_default_tiers)

    # Symbols to stream (Binance spot). Defaults to the union of all tiers.
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

    def __post_init__(self) -> None:
        # Keep symbols consistent with tiers: if the caller supplied custom
        # tiers but no explicit symbols, derive the symbol list from them.
        if not self.symbols:
            self.symbols = _flatten_tiers(self.tiers)

    @property
    def tier_order(self) -> List[str]:
        """Tier labels in display order."""
        return list(self.tiers.keys())

    def tier_of(self, symbol: str) -> str:
        """Return the market-cap tier label for a symbol (or 'Other')."""
        sym = symbol.upper()
        for label, syms in self.tiers.items():
            if sym in syms:
                return label
        return "Other"

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
