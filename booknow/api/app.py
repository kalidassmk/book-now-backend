"""
app.py
─────────────────────────────────────────────────────────────────────────────
FastAPI app factory + a tiny uvicorn wrapper main.py uses to run the
HTTP server alongside the trading core in the same event loop.

The factory takes an already-built :class:`AppState` and returns a
``FastAPI`` instance with every router mounted. We don't construct
services here — that's main.py's job — so tests can build their own
state and pass it straight in.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from booknow.api.routes_binance import router as binance_router
from booknow.api.routes_config import router as config_router
from booknow.api.routes_diag import router as diag_router
from booknow.api.routes_trading import router as trading_router
from booknow.api.routes_wallet import router as wallet_router
from booknow.api.state import AppState


logger = logging.getLogger("booknow.api")


def build_app(state: AppState) -> FastAPI:
    """Build the FastAPI instance and stash ``state`` on it.

    Every route reads its services off ``app.state.engine_state``
    via :func:`booknow.api.deps.get_state`.
    """
    app = FastAPI(
        title="BookNow Engine API",
        version="0.1.0",
        # Match Java's Spring behaviour: no auto-redirect on trailing slashes
        # since the dashboard hits exact paths.
        redirect_slashes=False,
    )

    # CORS — same wide-open policy Java's @CrossOrigin had. The dashboard
    # is on a different port, so this is the path of least surprise.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.state.engine_state = state

    app.include_router(trading_router)
    app.include_router(binance_router)
    app.include_router(wallet_router)
    app.include_router(config_router)
    app.include_router(diag_router)  # iter 79 — /api/v1/diag/logs/*

    @app.get("/", tags=["meta"])
    async def root():
        return {
            "name": "BookNow Engine API",
            "live_mode": state.settings.live_mode,
            "endpoints": [
                "/api/v1/health",
                "/api/v1/start",
                "/api/v1/stop",
                "/api/v1/config",
                "/api/v1/orders/open",
                "/api/v1/binance/account",
                "/api/v1/binance/btc-price",
                "/api/wallet/balances",
                "/api/wallet/dust",
            ],
        }

    return app


class HttpServer:
    """Runs uvicorn inside the engine's asyncio event loop.

    main.py creates one of these, calls ``await server.start()`` once
    every service is wired, and ``await server.stop()`` during the
    shutdown sequence. We use uvicorn's programmatic ``Server`` so we
    don't fork or spawn a separate process.
    """

    def __init__(self, app: FastAPI, host: str = "0.0.0.0", port: int = 8083):
        # Late import — keeps uvicorn out of the import graph during tests
        # that build the app for shape checks but don't run it.
        import uvicorn

        self._config = uvicorn.Config(
            app,
            host=host,
            port=port,
            log_level="info",
            access_log=False,        # noisy with the dashboard polling
            loop="asyncio",
        )
        self._server = uvicorn.Server(self._config)
        self._task: Optional[asyncio.Task] = None
        self.host = host
        self.port = port

    async def start(self) -> None:
        """Spawn the uvicorn loop as a background task."""
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._server.serve(), name="http-server")
        # Give uvicorn a moment to bind so we report success-or-fail
        # while bootstrap is still in scope.
        for _ in range(50):
            await asyncio.sleep(0.05)
            if self._server.started:
                logger.info("[http] listening on http://%s:%d", self.host, self.port)
                return
        logger.warning(
            "[http] uvicorn did not report started within 2.5s — continuing",
        )

    async def stop(self) -> None:
        if self._task is None:
            return
        # Polite first; uvicorn flips this and exits its serve() loop.
        self._server.should_exit = True
        try:
            await asyncio.wait_for(self._task, timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("[http] uvicorn didn't exit in 5s — cancelling")
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        self._task = None
        logger.info("[http] stopped")
