"""
deps.py
─────────────────────────────────────────────────────────────────────────────
Tiny FastAPI dependency resolvers.

Routes don't construct services — they ``Depends(get_state)`` and pull
what they need off :class:`AppState`. We stash the state on the FastAPI
``app.state`` in :func:`booknow.api.app.build_app`, and read it back here.
"""

from __future__ import annotations

from fastapi import Request

from booknow.api.state import AppState


def get_state(request: Request) -> AppState:
    """Pulls the AppState off the underlying FastAPI ``app.state``.

    Cast for static checkers; the real type is ``AppState`` because
    that's what we set in :func:`build_app`.
    """
    return request.app.state.engine_state  # type: ignore[no-any-return]
