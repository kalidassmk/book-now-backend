"""
tickers_ws_cache.py — COMPATIBILITY SHIM
─────────────────────────────────────────────────────────────────────────────
The real implementation moved to
``python-engine/booknow/binance/tickers_cache.py`` during the
python-engine consolidation. This file stays so legacy callers in
binance-sentiment-engine continue to import as before
(``from tickers_ws_cache import TickersCache, get_default_cache``)
until they're absorbed into the python-engine.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ENGINE = Path(__file__).resolve().parent.parent / "python-engine"
if _ENGINE.is_dir() and str(_ENGINE) not in sys.path:
    sys.path.insert(0, str(_ENGINE))

from booknow.binance.tickers_cache import (  # noqa: E402, F401
    TickersCache,
    WS_URL,
    get_default_cache,
    logger,
)

__all__ = ["TickersCache", "WS_URL", "get_default_cache"]
