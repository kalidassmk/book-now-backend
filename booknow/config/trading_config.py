"""
trading_config.py
─────────────────────────────────────────────────────────────────────────────
Dashboard-editable trading configuration.

Equivalent of Java's ``TradingConfig`` model + ``TradingConfigService``.
Stored as a single JSON value at the Redis key ``TRADING_CONFIG`` so
the dashboard can modify it live and the engine picks up changes
without a restart.

Field names match the Java POJO so the existing dashboard's reader/
writer code continues to work unchanged.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, fields
from typing import Any, Dict, Optional

import redis.asyncio as aioredis

from booknow.repository import redis_keys


logger = logging.getLogger("booknow.trading_config")


@dataclass
class TradingConfig:
    """Mirrors the Java ``TradingConfig.java``.

    Defaults are tuned for the user's stated style: $12 buy with a
    fixed $0.20 USDT take-profit, fast-scalp mode on, max-hold 5 min.
    """

    # ── Core safety ──────────────────────────────────────────────────────
    autoBuyEnabled: bool = False     # OFF by default — operator opts in
    buyAmountUsdt: float = 12.0

    # ── Profit target ────────────────────────────────────────────────────
    # If profitAmountUsdt > 0 it overrides profitPct (matches Java logic).
    profitPct: float = 0.0
    profitAmountUsdt: float = 0.20

    # ── Order placement ──────────────────────────────────────────────────
    limitBuyOffsetPct: float = 0.3   # buy this % below market
    tslPct: float = 2.0              # trailing stop-loss

    # ── Fast-scalp behaviour ─────────────────────────────────────────────
    fastScalpMode: bool = True
    maxHoldSeconds: int = 300
    marketExitOnTimeout: bool = True

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TradingConfig":
        """Tolerantly construct from a dict — unknown keys ignored."""
        known = {f.name for f in fields(cls)}
        clean = {k: v for k, v in data.items() if k in known}
        return cls(**clean)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class TradingConfigService:
    """Async wrapper that fetches the config from Redis on each read.

    Java refreshes on every getter call to "react immediately to
    dashboard changes". Python does the same — Redis hits are ~µs and
    the alternative (cache) hides bugs where the dashboard toggles a
    flag and nothing happens.
    """

    def __init__(self, redis_client: aioredis.Redis):
        self._redis = redis_client
        self._cached: Optional[TradingConfig] = None

    async def init(self) -> TradingConfig:
        """Seed Redis with defaults if the key doesn't exist yet."""
        config = await self.refresh()
        return config

    async def refresh(self) -> TradingConfig:
        """Fetch latest from Redis. On first miss, save defaults."""
        try:
            raw = await self._redis.get(redis_keys.TRADING_CONFIG)
            if raw is None:
                logger.info("[TradingConfig] No config in Redis — seeding defaults")
                config = TradingConfig()
                await self.save(config)
                self._cached = config
                return config
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("[TradingConfig] Corrupted config JSON — using cached/defaults")
                if self._cached is None:
                    self._cached = TradingConfig()
                return self._cached
            config = TradingConfig.from_dict(data)
            self._cached = config
            return config
        except Exception as e:
            logger.error("[TradingConfig] Redis read failed: %s — using cached/defaults", e)
            if self._cached is None:
                self._cached = TradingConfig()
            return self._cached

    async def save(self, config: TradingConfig) -> None:
        await self._redis.set(redis_keys.TRADING_CONFIG, json.dumps(config.to_dict()))
        self._cached = config
        logger.info("[TradingConfig] Saved: %s", config.to_dict())

    async def get(self) -> TradingConfig:
        """The hot-path getter — same contract as Java's getConfig()."""
        return await self.refresh()
