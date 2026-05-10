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

    Defaults aligned with operator's "Option B" sizing (2026-05-10):
      $6 buy, +1.0 % TP (≈ $0.05 net per win after fees), -0.65 % limit-buy
      offset, patient hold (no tight stop). Fast-scalp mode on.
    """

    # ── Core safety ──────────────────────────────────────────────────────
    autoBuyEnabled: bool = False     # OFF by default — operator opts in
    buyAmountUsdt: float = 6.0       # 2026-05-10: Option B sizing (was 30.0)

    # ── Profit target ────────────────────────────────────────────────────
    # If profitAmountUsdt > 0 it overrides profitPct (matches Java logic).
    # 1.0 % = $0.06 gross / ≈$0.05 net per win on a $6 buy after 0.2 %
    # round-trip Binance fees.
    profitPct: float = 1.0
    profitAmountUsdt: float = 0.0

    # ── Stop loss (Fast Scalper consumes; Virtual Scalper too) ──────────
    # 2026-05-10: DISABLED by default (set to 0). Operator chose Option B
    # "patient hold" — wait for TP even on heavy paper losses. Set this to
    # a positive USDT amount to re-enable a stop-loss exit; both scalpers
    # treat 0 (or negative) as "no stop, no SL leg on the OCO".
    stopLossUsdt: float = 0.0        # 0 = disabled (was 0.06)

    # ── Order placement ──────────────────────────────────────────────────
    # 0.65 % limit-buy offset comes from the 2026-05-10 backtest: that is
    # where dips actually fill (≈ 60 % fill rate in 60 min) AND the +1 % TP
    # is reachable (≈ 42 % TP-hit rate of fills).
    limitBuyOffsetPct: float = 0.65  # buy this % below market signal
    # 2026-05-10: 60 → 3600 (60 min). Option B's -0.65% offset needs the
    # full hour to fill (~63 % fill rate at 60 min in the backtest);
    # 60 seconds was a leftover from the old -0.09 % era.
    limitBuyTimeoutSec: int = 3600   # cancel limit-buy if not filled in this window
    tslPct: float = 2.0              # trailing stop-loss (legacy)

    # ── Fast-scalp behaviour ─────────────────────────────────────────────
    fastScalpMode: bool = True
    maxHoldSeconds: int = 3600
    marketExitOnTimeout: bool = True

    # ── Virtual Scalper live mode ────────────────────────────────────────
    virtualScalperLiveMode: bool = False   # set true to make Virtual Scalper trade real money

    # ── 24h market-context filter (post-mortem-derived) ─────────────────
    # Reject buy entries when the symbol's 24h ticker fails any of these.
    minChange24hPct: float = -1.0    # skip falling-knife coins
    minRange24hPct:  float = 5.0     # skip too-quiet coins
    minVol24hUsd:    float = 2_000_000.0  # liquidity floor

    # ── Falling-knife filter (skip top-of-pump buys) ─────────────────────
    # Derived from 2026-05-10 backtest: XEC/LUNC/LUMIA/etc deep losses came
    # from buying coins that had already pumped or were too volatile.
    # Layering this filter on the same 12 signals would have skipped all 4
    # deep losers (XEC×2, LUNC, LUMIA) without losing a single winner.
    fallingKnifeFilterEnabled: bool = True
    maxChange24hPct: float = 8.0       # skip if 24h change > +8%
    maxRange1hPct: float = 6.0         # skip if 1h hi-lo range > 6%
    overboughtSkipEnabled: bool = True # skip if 24h>0 AND 60m>+1.5%
    overbought60mPct: float = 1.5      # 60m threshold for overbought combo

    # ── Fast-drop-without-volume filter (Pattern C, post-signal) ─────────
    # 2026-05-10 trajectory analysis showed BIO/SOPH (today's losers) both
    # dropped to -0.5% within minutes of signal WITHOUT a volume surge,
    # while winner JOE crossed -1% just as fast but had a 28× volume
    # explosion (panic capitulation → bounce). This filter monitors price
    # + volume during the limit-buy wait and CANCELS the order if the
    # bad pattern fires — saving us from filling into a slow bleed.
    fastDropFilterEnabled: bool = True
    fastDropDetectMinutes: int = 3        # how long after signal we watch
    fastDropThresholdPct: float = 0.5     # price must dip >= this % below signal
    volSurgeThresholdMultiplier: float = 2.0  # vol-1m / pre-baseline must exceed this to keep order

    # ── Metrics collection ───────────────────────────────────────────────
    metricsEnabled: bool = True

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
