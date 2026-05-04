"""
settings.py
─────────────────────────────────────────────────────────────────────────────
Static, environment-driven configuration for the python-engine.

Replaces Spring's `application.properties`. Loads `.env` automatically
(precedence: `python-engine/.env` first, then the dashboard's existing
`.env` so we share Binance keys without duplicating them).

Runtime, dashboard-editable settings (auto-buy on/off, profit target,
fast-scalp mode, etc.) are NOT here — those live in Redis under the
TRADING_CONFIG key, exactly as the Java backend used. See
`config/trading_config.py` (added in Phase 2) for that.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_DASHBOARD_ENV = _PROJECT_ROOT / "dashboard" / ".env"
_ENGINE_ENV    = _PROJECT_ROOT / "python-engine" / ".env"


class Settings(BaseSettings):
    """All values can be overridden via env vars or .env files."""

    # ── Binance credentials ──────────────────────────────────────────────
    # Names mirror what the dashboard already exports so we share them.
    binance_api_key: str = Field(default="", alias="BINANCE_API_KEY")
    binance_secret_key: str = Field(default="", alias="BINANCE_SECRET_KEY")

    # ── Redis ────────────────────────────────────────────────────────────
    redis_host: str = Field(default="127.0.0.1", alias="REDIS_HOST")
    redis_port: int = Field(default=6379, alias="REDIS_PORT")
    redis_db: int = Field(default=0, alias="REDIS_DB")

    # ── Engine run-mode toggles ──────────────────────────────────────────
    # When False, the trading executor logs intended orders but never
    # places them on Binance. Mirrors Java's `trading.live-mode`.
    live_mode: bool = Field(default=False, alias="BOOKNOW_LIVE_MODE")

    # FastAPI HTTP port (replaces Spring's 8083).
    http_port: int = Field(default=8083, alias="BOOKNOW_HTTP_PORT")

    # Verbose logging (DEBUG vs INFO).
    debug: bool = Field(default=False, alias="BOOKNOW_DEBUG")

    # ── Sentiment-engine supervisor (Phase 12) ──────────────────────────
    # When True, the engine spawns the consolidated sentiment analyzers
    # (under ``booknow/sentiment/scripts/``, see Phase 19) as supervised
    # subprocesses alongside the trading core. Set to False if you want
    # the engine to run its own loops only (paper-trade rule testing etc.).
    sentiment_enabled: bool = Field(default=True, alias="BOOKNOW_SENTIMENT_ENABLED")
    # Override the directory if the analyzers live somewhere unusual.
    # Defaults to ``booknow/sentiment/scripts`` (in-tree) — see Phase 19.
    sentiment_dir: str = Field(default="", alias="BOOKNOW_SENTIMENT_DIR")

    model_config = SettingsConfigDict(
        # Try the engine's own .env first, fall back to the dashboard's.
        # Pydantic ignores entries that are missing.
        env_file=(str(_DASHBOARD_ENV), str(_ENGINE_ENV)),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    """Cached accessor. All callers should go through this."""
    return Settings()
