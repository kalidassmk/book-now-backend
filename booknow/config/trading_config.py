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
    buyAmountUsdt: float = 12.0      # 2026-05-11: $12/leg (was 6.0)

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
    # 2026-05-11: scale-aware default 0.12 (= 1 % of $12 buy) for clarity
    # in the dataclass, but live Redis still keeps it disabled.
    stopLossUsdt: float = 0.0        # 0 = disabled

    # ── Order placement ──────────────────────────────────────────────────
    # 2026-05-11 iter 3: offset 0.65 → 0.30 after P&L analysis showed only
    # 20% of passed signals dipped to -0.65% within 30 min, leaving ~60 of
    # 60 daily signals unfilled. -0.30% should bring fill rate to ~85%.
    # Trade-off: TP from signal becomes +0.70% (was +0.34%), so each fill
    # needs more upward movement to win — but the 5× jump in fill count
    # is projected to overcome the lower per-fill win rate.
    limitBuyOffsetPct: float = 0.30  # buy this % below market signal
    # 2026-05-11: 1800 → 3600 (60 min). Even with the tighter offset some
    # coins take 35-55 min to dip; the bigger window costs nothing on
    # unfilled orders (we cancel cleanly).
    limitBuyTimeoutSec: int = 3600   # cancel limit-buy if not filled in this window
    tslPct: float = 2.0              # trailing stop-loss (legacy)

    # ── Fast-scalp behaviour ─────────────────────────────────────────────
    fastScalpMode: bool = True
    maxHoldSeconds: int = 3600
    marketExitOnTimeout: bool = True

    # ── Trend-reversal exit ─────────────────────────────────────────────
    # 2026-05-11: 8 of 10 losers yesterday hit TP after we panic-exited via
    # EMA-9 < EMA-21. Operator chose to disable this exit (set to False in
    # live Redis) so positions get the full +1% TP shot. Code default stays
    # True so a Redis wipe doesn't accidentally undo the change to a more
    # conservative setup.
    trendReversalExitEnabled: bool = True

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
    #
    # 2026-05-11 iter 2: P&L analysis showed 18 of 31 filter-skipped
    # signals would have hit TP (58% false-positive rate). Loosening the
    # gates: pump 8% → 12%, overbought-60m 1.5% → 2.5%. The original
    # extreme losers (XEC +19%, LUNC +10%) still get filtered.
    fallingKnifeFilterEnabled: bool = True
    maxChange24hPct: float = 12.0      # skip if 24h change > +12% (was 8.0)
    maxRange1hPct: float = 6.0         # skip if 1h hi-lo range > 6%
    overboughtSkipEnabled: bool = True # skip if 24h>0 AND 60m>+2.5%
    overbought60mPct: float = 2.5      # 60m threshold for overbought combo (was 1.5)

    # ── Fast-drop-without-volume filter (Pattern C, post-signal) ─────────
    # 2026-05-10 trajectory analysis showed BIO/SOPH (today's losers) both
    # dropped to -0.5% within minutes of signal WITHOUT a volume surge,
    # while winner JOE crossed -1% just as fast but had a 28× volume
    # explosion (panic capitulation → bounce). This filter monitors price
    # + volume during the limit-buy wait and CANCELS the order if the
    # bad pattern fires — saving us from filling into a slow bleed.
    #
    # 2026-05-10 iteration 2: bumped threshold 0.5 → 0.7 after backtest
    # (3 wrongly-cancelled winners — PLUME / BMT / SAHARA — all had only
    # ~ -0.5 % dips in the first 3 min, while bad fills BIO / LUMIA / LUNC
    # progressed deeper. 0.7 % keeps the bad-fill catches and lets shallow
    # slow-drifters through. Will re-evaluate after a week of live data.
    fastDropFilterEnabled: bool = True
    fastDropDetectMinutes: int = 3        # how long after signal we watch
    fastDropThresholdPct: float = 0.7     # price must dip >= this % below signal
    volSurgeThresholdMultiplier: float = 2.0  # vol-1m / pre-baseline must exceed this to keep order

    # ── Laddered Recovery strategy (2026-05-11) ──────────────────────────
    # 3-tier averaging-down entry pattern, single coin at a time:
    #   1. Buy 1: aggressive-limit at current ask (instant fill, no spread surprise)
    #   2. Buy 2: limit at signal × (1 - ladderBuy2OffsetPct/100)
    #   3. Buy 3: limit at signal × (1 - ladderBuy3OffsetPct/100)  — cancelled when Buy 2 fills
    # Exit:
    #   • TP at weighted-avg × (1 + ladderTpFromAvgPct/100)
    #   • Hard stop activated ONLY after Buy 3 fills (gap scenario), at
    #     buy_3_price × (1 - ladderHardStopBelowBuy3Pct/100)
    # 2026-05-11 iter 2: replaced singleCoinModeEnabled with
    # maxConcurrentLadders (operator wants 3). Per leg sizing bumped
    # $6 → $12 so max total exposure per ladder = $36 (3 fills × $12);
    # across 3 concurrent ladders, max $108 USDT in flight.
    ladderedRecoveryEnabled: bool = True
    maxConcurrentLadders: int = 3
    # Legacy: kept for back-compat. Code now uses maxConcurrentLadders.
    singleCoinModeEnabled: bool = False
    ladderBuy1SizeUsdt: float = 12.0
    ladderBuy2SizeUsdt: float = 12.0
    ladderBuy3SizeUsdt: float = 12.0
    ladderBuy2OffsetPct: float = 0.5    # buy 2 at signal × 0.995
    ladderBuy3OffsetPct: float = 1.0    # buy 3 at signal × 0.99
    ladderTpFromAvgPct: float = 1.0     # TP at weighted_avg × 1.01
    ladderHardStopBelowBuy3Pct: float = 1.0  # stop at buy3 × 0.99 if buy3 filled
    # 2026-05-11 iter 3: True (was False). Operator wants instant Buy 1
    # so Buy 2/3 limits go on the book *simultaneously* — no waiting for
    # an aggressive limit to fill before placing the averaging-down legs.
    ladderBuy1UseMarketOrder: bool = True
    # 2026-05-11 iter 4: per-coin cooldown. After a ladder closes the
    # same symbol is blocked for N seconds so the bot doesn't immediately
    # re-enter the same trade.
    ladderCooldownSeconds: int = 14400   # 4 hours

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
