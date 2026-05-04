"""
booknow.api
─────────────────────────────────────────────────────────────────────────────
FastAPI HTTP layer (Phase 14). Replaces the Spring REST surface that the
dashboard reads, byte-for-byte where it matters and JSON-shape compatible
with the existing dashboard frontend.

Public entry point:

    from booknow.api import build_app, AppState

    state = AppState(...)            # built in main.py from already-wired services
    app = build_app(state)           # FastAPI instance
    # uvicorn.Server(uvicorn.Config(app, port=8083)).serve()
"""

from booknow.api.app import build_app
from booknow.api.state import AppState

__all__ = ["build_app", "AppState"]
