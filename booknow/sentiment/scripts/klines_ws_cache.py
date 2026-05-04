"""
klines_ws_cache.py — COMPATIBILITY SHIM
─────────────────────────────────────────────────────────────────────────────
The real implementation moved to
``python-engine/booknow/binance/klines_cache.py`` during the
python-engine consolidation (commit baa545a..). This file remains so
legacy callers in binance-sentiment-engine that still do
``from klines_ws_cache import KlinesCache`` keep working without
modification — they will be absorbed into the python-engine in a later
phase, and this shim deleted along with the surrounding sentiment
scripts at the end of the migration.

Symbols re-exported: ``KlinesCache``, ``MAX_STREAMS_PER_CONN``,
``BINANCE_REST``, ``BINANCE_WS_BASE``, ``logger`` (and the private
``_stream_name`` helper for any code that imported it).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Make python-engine importable from this script's working directory.
_ENGINE = Path(__file__).resolve().parent.parent / "python-engine"
if _ENGINE.is_dir() and str(_ENGINE) not in sys.path:
    sys.path.insert(0, str(_ENGINE))

from booknow.binance.klines_cache import (  # noqa: E402, F401
    KlinesCache,
    MAX_STREAMS_PER_CONN,
    BINANCE_REST,
    BINANCE_WS_BASE,
    logger,
    _stream_name,
)

__all__ = [
    "KlinesCache",
    "MAX_STREAMS_PER_CONN",
    "BINANCE_REST",
    "BINANCE_WS_BASE",
    "logger",
]

# Quiet style note: keep the import-side-effect path resolution. The
# trailing `os` import is unused but kept here in case the engine path
# resolution ever needs to consult environment vars (e.g. BOOKNOW_HOME).
_ = os  # silence "unused import"
