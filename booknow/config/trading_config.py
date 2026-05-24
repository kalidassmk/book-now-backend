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
    buyAmountUsdt: float = 50.0      # 2026-05-12 iter 12: $50/leg (was 55)

    # ── Profit target ────────────────────────────────────────────────────
    # If profitAmountUsdt > 0 it overrides profitPct (matches Java logic).
    # 2026-05-11 iter 4: TP 1.0 → 0.6 % so a $12 Buy 1 sell nets ~$0.05
    # (gross $0.072 - $0.024 fees). Multi-leg fills scale linearly
    # (~$0.05 per filled $12 leg).
    # 2026-05-23 iter 53: profitAmountUsdt 0.20 → 0.40 (operator request
    # — bigger per-win target across all bot paths).
    profitPct: float = 0.6
    profitAmountUsdt: float = 0.4

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
    # 2026-05-12 iter 15: default True (was False). Operator's intended
    # mode is live virtual trading. Combined with the new merge-on-POST in
    # routes_config, this ensures the value survives dashboard saves AND
    # a fresh Redis (e.g. volume wipe) without needing manual re-flips.
    virtualScalperLiveMode: bool = True

    # ── 24h market-context filter (post-mortem-derived) ─────────────────
    # Reject buy entries when the symbol's 24h ticker fails any of these.
    # 2026-05-12 iter 13: relaxed volume floor back to $2M — $5M was
    # starving the signal stream (most alt scalps live in $2M–$5M range).
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

    # ── Post-pump-bleed (daily-timeframe) filter ─────────────────────────
    # 2026-05-12: added after the JTO loss. JTO pumped from $0.345 → $0.70
    # ~4-5 days ago and has been bleeding back ever since. Every short-
    # timeframe filter saw a calm market because the pump was outside their
    # window. This filter runs on daily klines and blocks coins that:
    #   pumped >= postPumpThresholdPct% off a 10-day pre-pump baseline,
    #   are now >= postPumpOffPeakMinPct% off that peak,
    #   sit below their MA7 (short-term avg breaking down),
    #   peaked >= postPumpMinDaysSincePeak days ago (i.e. not still pumping).
    # All four gates must fire — singletons false-positive on healthy
    # volatility. Defaults verified against JTO + ICP (both blocked) and
    # BTC/ETH/SOL/XRP/ATOM/TAO/TIA/SEI (none blocked) on 2026-05-12 data.
    postPumpFilterEnabled: bool = True
    postPumpThresholdPct: float = 30.0       # pump = +30% over baseline
    postPumpOffPeakMinPct: float = 10.0      # current ≥ 10% below pump peak
    postPumpMinDaysSincePeak: int = 2        # peak ≥ 2 days ago
    postPumpLookbackDays: int = 15           # scan last 15 days for the peak
    postPumpBaselineDays: int = 10           # 10-day pre-pump baseline window
    # iter 21 (2026-05-13): MA7 gate was too sensitive — single-day bounces
    # of <1% above MA7 disabled the whole filter on real post-pump cases.
    # Default False (gate disabled); flip True only if a regression appears.
    postPumpRequireBelowMa7: bool = False

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
    # 2026-05-12 iter 11: 2-leg ladder ($55 / $55 / Buy 3 DISABLED).
    # Max exposure per ladder: $110 (2 × $55). Only 1 ladder at a time.
    # Buy 3 off via size=0 → min_notional check skips placement.
    ladderedRecoveryEnabled: bool = True
    maxConcurrentLadders: int = 1
    singleCoinModeEnabled: bool = True
    # 2026-05-12 iter 15: $25 → $48 per leg (operator request). Wallet
    # ~$100-110 → $48×2 = $96 ladder + 3% margin = $98.88 threshold,
    # fits with ~$3 headroom. Captures more $ per trade since the
    # static $0.40 net target activates trailing-TP at ~1% gain (vs
    # ~1.6% needed at $25/leg) — so winners are more likely to enter
    # trail mode and catch the bigger moves.
    ladderBuy1SizeUsdt: float = 48.0     # iter 15: $48/leg
    ladderBuy2SizeUsdt: float = 48.0
    ladderBuy3SizeUsdt: float = 0.0      # 0 = Buy 3 disabled
    ladderBuy2OffsetPct: float = 0.5    # buy 2 at signal × 0.995
    ladderBuy3OffsetPct: float = 1.0    # buy 3 at signal × 0.99
    ladderTpFromAvgPct: float = 0.6     # static TP fallback when target-net not set
    # 2026-05-11 iter 8: dynamic TP based on a $ net-profit target.
    # When ladderTargetNetProfitUsdt > 0 the bot computes the TP %
    # automatically: tp_pct = (target_net / buy_size) × 100 + 2 × fee_rate_pct
    # which guarantees the configured net profit after BOTH sides' fees.
    # Set to 0 to fall back to the static ladderTpFromAvgPct above.
    # 2026-05-13 iter 19: $0.40 → $0.20 (operator request — TP fires
    # faster). Math: on $48 leg the TP sits at +0.567% above Buy 1
    # instead of +0.983%. That's a more reachable target on the kind of
    # alt moves we see, and the trailing-TP layer still lets winners
    # ride further when momentum continues past the TP level.
    # 2026-05-15 iter 42: $0.20 → $0.15. Operator request — slightly more
    # reachable TP so trades close faster (NXPC pattern: user manually
    # cancelled the $0.20-target TP and re-priced lower for a quicker exit).
    # 2026-05-23 iter 53: $0.15 → $0.40. Operator request — bigger per-win
    # target now that filter stack catches the worst losers.
    ladderTargetNetProfitUsdt: float = 0.4
    # Per-side fee rate. 0.00075 = 0.075 % (BNB-for-fees discount enabled);
    # set to 0.001 (0.1 %) if BNB-for-fees is OFF.
    ladderFeeRatePerSide: float = 0.00075
    ladderHardStopBelowBuy3Pct: float = 1.0  # stop at buy3 × 0.99 if buy3 filled

    # ── Time-based ladder exit (iter 14) ─────────────────────────────────
    # Root cause for the operator's bigger losses: they were panic-selling
    # underwater ladders manually because there was no time-based exit.
    # These two knobs cap the maximum time a ladder can sit without TP
    # firing, so the bot makes the close decision (not the operator).
    #
    #   ladderTimeExitEnabled:       master switch
    #   ladderMaxHoldSeconds:        force market-sell after this many seconds
    #                                regardless of P&L (default 4h)
    #   ladderBreakevenExitEnabled:  smarter early exit — if ladder ever went
    #                                underwater AND price returns to break-even
    #                                (+ buffer), exit immediately at market
    #   ladderBreakevenBufferPct:    buffer above avg. MUST exceed round-trip
    #                                fees (0.15% = 2 × 0.075% per side) for
    #                                the exit to actually be net-zero or
    #                                positive. 2026-05-14 iter 35: was 0.05%,
    #                                which guaranteed a small loss every time
    #                                ("break-even recovery" sold DOGE for
    #                                −$0.034). Bumped to 0.20% = fees (0.15%)
    #                                + 0.05% true profit margin.
    ladderTimeExitEnabled: bool = True
    ladderMaxHoldSeconds: int = 14400         # 4 hours
    ladderBreakevenExitEnabled: bool = True
    ladderBreakevenBufferPct: float = 0.50

    # ── Hard stop from avg (iter 37, 2026-05-15) ─────────────────────────
    # Caps the worst-case loss on a ladder that goes deeply underwater.
    # The existing ladderHardStopBelowBuy3Pct only engages AFTER Buy 3
    # fills, but Buy 3 is currently disabled (size=0) — so there is no
    # hard stop at all today. A ladder that keeps falling can lose the
    # full $96 minus whatever the panic-sell catches.
    #
    # Pattern from QNT / HBAR forensic (2026-05-14): both averaged-down
    # ladders kept dropping for hours; user panic-sold at −0.3% to
    # −0.5% from avg per leg. With $96 deployed, anything past −1.5%
    # from avg is catastrophic for daily P&L.
    #
    # New logic: in any ACTIVE_* state, if live price <= avg ×
    # (1 - ladderHardStopFromAvgPct/100), force-exit at market on the
    # filled qty. Reason recorded as "hard_stop_from_avg".
    #
    # Default 1.5 % → max ~$1.50 realised loss on a $96 ladder when both
    # legs filled; ~$0.75 when only Buy 1 filled. Painful but bounded.
    # Set to 0 to disable.
    ladderHardStopFromAvgEnabled: bool = True
    ladderHardStopFromAvgPct: float = 1.5

    # ── Liquidity-Death adaptive hard-stop (iter 39, 2026-05-15) ──────────
    # Forensic of QNT/HBAR/FLOKI on 2026-05-14/15 showed the real signal
    # for "this coin won't pump back" is VOLUME COLLAPSE, not price drop:
    #
    #   Metric             QNT (loss)   HBAR (loss)   FLOKI (win)
    #   --------------     ----------   -----------   -----------
    #   Drop max           -0.36%       -0.63%        -0.40%   ← all similar
    #   Vol ratio (hold/pre) 0.63x      0.28x         2.60x    ← 4-9× gap
    #   Trades/min          16           60           793       ← 13-50× gap
    #
    # All three coins dropped a similar amount; the fixed 1.5 % hard
    # stop wouldn't have fired on ANY. The difference between win and
    # loss was whether volume returned. When volume dies, the price
    # can't bounce; the ladder just rots underwater until the operator
    # panic-sells.
    #
    # New algorithm — multi-factor "Dead Coin Score":
    #
    #   Tier 1 (catastrophic): drop ≥ ldCatastrophicDropPct  → EXIT NOW
    #     (replaces the iter37 fixed 1.5%; widened to 2.5% because the
    #      smart tiers below catch losers EARLIER on liquidity signals.)
    #
    #   Tier 2 (liquidity-death score):  only when held ≥ ldMinHoldMin
    #     AND drop ≥ ldMinDropPct. Score points from N factors:
    #       +3  vol_ratio < ldVolCollapseThreshold
    #       +2  vol_ratio < ldVolCollapseThreshold × 0.5  (extreme)
    #       +2  no candle since fill reached avg × 0.999 (never recovered)
    #       +2  drop ≥ 1.0%
    #       +2  lower-lows share ≥ ldLowerLowsThreshold
    #       +1  red-candle share ≥ ldRedShareThreshold
    #     Exit when score ≥ ldExitScoreThreshold.
    #
    #   Tier 3 (stagnation): held ≥ ldStagnationHoldMin AND drop in
    #     [-1%, 0%] AND vol_ratio < ldVolCollapseThreshold AND price
    #     hasn't approached TP. Frees capital from "dead but not dropping"
    #     positions like HBAR which sat at -0.28% for 5h.
    #
    # Worked examples (validated on the forensic data):
    #   QNT  vol=0.63x ll=34% red=46% drop=0.20%  → score ≈ 3  → HOLD ✓
    #     (QNT actually recovered to +$0.04 net; user panic-cancelled.)
    #   HBAR vol=0.28x ll=38% red=47% drop=0.28%  → caught by Tier 3 ✓
    #     after 60min instead of bleeding 5h.
    #   FLOKI vol=2.60x → no factors fire → HOLD ✓ → TP hit at +$0.23.
    liquidityDeathExitEnabled: bool = True
    liquidityDeathCatastrophicDropPct: float = 2.5   # absolute floor
    liquidityDeathMinHoldMin: int = 10               # wait this long before evaluating
    liquidityDeathMinDropPct: float = 0.3            # need at least this much loss
    liquidityDeathLookbackMin: int = 10              # # of 1m candles to analyse
    liquidityDeathVolCollapseThreshold: float = 0.7  # vol_now < 70% baseline = dead
    liquidityDeathLowerLowsThreshold: float = 0.55   # ≥55% lower-lows in window
    liquidityDeathRedShareThreshold: float = 0.60    # ≥60% red candles
    liquidityDeathExitScoreThreshold: int = 6        # sum of factors to exit
    liquidityDeathStagnationHoldMin: int = 60        # Tier 3 hold floor
    liquidityDeathStagnationMaxDropPct: float = 1.0

    # ── Post-Buy-2 Careful Monitor (iter 41, 2026-05-15) ──────────────────
    # Once Buy 2 fills, the ladder has DOUBLE exposure ($96 instead of
    # $48) and we've already averaged down once. That's a different
    # risk profile than ACTIVE_1:
    #
    #  • DON'T panic-sell on the same triggers as ACTIVE_1 (5-min grace).
    #  • DO grab any profit immediately (averaging down worked — exit).
    #  • DO exit at break-even with a TIGHTER buffer (just covers fees).
    #  • DO accept a small loss if the trade is clearly stuck (better
    #    than waiting for the smarter iter39 to fire on a larger loss).
    #  • DO cap the worst case TIGHTER than ACTIVE_1.
    #
    # Decision tree (priority order, first match wins):
    #
    #   0. CATASTROPHIC: drop >= 2.5%  (iter39 floor still in force)
    #   1. GRACE PERIOD (first 5 min after Buy 2 fill): only #0 fires.
    #      Give the average-down time to work.
    #   2. QUICK PROFIT: current >= avg × (1 + quickProfitPct/100).
    #      Lock in any profit (default +0.2% above new avg).
    #   3. TIGHT BREAKEVEN: ever_underwater_after_buy2 AND
    #      current >= avg × (1 + tightBreakevenBufferPct/100).
    #      Exit at flat (default +0.15% — JUST covers fees).
    #   4. NO RECOVERY: held >= patienceMin AND drop >= noRecoveryDropPct
    #      AND never reached avg × 0.999 since Buy 2 fill.
    #      Accept a minimal loss to free capital.
    #   5. HARD STOP: drop >= hardStopPct (tighter than iter37's 1.5%).
    #      Cap worst case at ~$1.44 on $96 ladder.
    #   6. Fall through to iter39 / iter37 / iter14 (still active as
    #      safety nets if none of the above fires).
    #
    # Worked example: Buy 1 = $78.17, Buy 2 = $77.78 (-0.5%), new
    # avg = $77.97.
    #   • +0.2% quick profit  ⇒ exit at $78.13   (lock small win)
    #   • +0.15% breakeven    ⇒ exit at $78.09   (after going underwater)
    #   • −1.5% hard stop     ⇒ exit at $76.80   (cap worst case)
    active2MonitorEnabled: bool = True
    active2GracePeriodMinutes: int = 5          # patient grace window
    active2QuickProfitPct: float = 0.2          # any profit → exit
    active2TightBreakevenBufferPct: float = 0.15 # tighter than ACTIVE_1's 0.5%
    active2PatienceMinutes: int = 20            # how long before no-recovery exit
    active2NoRecoveryDropPct: float = 0.5       # min loss to accept on no-recovery
    active2HardStopPct: float = 1.5             # worst-case cap from new avg

    # ── Volatility-Adaptive Entry + Dynamic TP (iter 43, 2026-05-15) ──────
    # Forensic of today's 5 Fast Scalper trades showed every single one
    # DIPPED after buy (−1% to −10%) — 3 of 5 peaked within 1-5 min after
    # entry. The bot was buying at the top of small rallies. The depth
    # of the post-buy dip correlated strongly with `range_1h_pct`:
    #
    #   MLN  1h range 5.70% → max drop 9.82%
    #   IOTA       0.50%   →           4.46%   (low range = surprise dip)
    #   LINK       0.98%   →           3.00%
    #   QNT        0.88%   →           1.32%
    #   NXPC       1.02%   →           1.07%
    #
    # New algorithm: at signal time, pick one of 4 strategies based on
    # range_1h_pct from the features dict. Each tier sets:
    #   • ladderBuy1OffsetPct  — how far below signal to place Buy 1
    #   • ladderBuy2OffsetPct  — how far below signal for Buy 2
    #   • ladderTargetNetProfitUsdt — dynamic TP based on expected swing
    #
    #  range_1h          strategy          buy1_off  buy2_off  tp_target
    #  < 1.0%   (calm)   AGGRESSIVE_BUY1   0.15%     0.5%      $0.15
    #  1.0–2.0% (normal) MODERATE          0.30%     0.8%      $0.20
    #  2.0–4.0% (volat)  WAIT_FOR_DIP      0.70%     1.5%      $0.30
    #  > 4.0%   (xvolat) WAIT_DEEP_DIP     1.50%     2.5%      $0.50
    #
    # Static config keys above still control the FALLBACK when adaptive
    # mode is off. When ON, adaptive params override at ladder_start time.
    adaptiveEntryEnabled: bool = True
    # Boundaries on the 1h range %
    adaptiveTierCalmMaxPct: float = 1.0
    adaptiveTierNormalMaxPct: float = 2.0
    adaptiveTierVolatileMaxPct: float = 4.0
    # Per-tier Buy 1 offset (% below signal)
    adaptiveBuy1OffsetCalm: float = 0.15
    adaptiveBuy1OffsetNormal: float = 0.30
    adaptiveBuy1OffsetVolatile: float = 0.70
    adaptiveBuy1OffsetXVolatile: float = 1.50
    # Per-tier Buy 2 offset (% below signal)
    adaptiveBuy2OffsetCalm: float = 0.50
    adaptiveBuy2OffsetNormal: float = 0.80
    adaptiveBuy2OffsetVolatile: float = 1.50
    adaptiveBuy2OffsetXVolatile: float = 2.50
    # Per-tier TP target ($ net)
    adaptiveTpTargetCalm: float = 0.15
    adaptiveTpTargetNormal: float = 0.20
    adaptiveTpTargetVolatile: float = 0.30
    adaptiveTpTargetXVolatile: float = 0.50

    # ── Macro-Top Exhaustion Filter (iter 44, 2026-05-15) ────────────────
    # Catches the PENDLE/USDT pattern from 2026-05-14: a coin that rallied
    # massively (+84% in 30 days), is currently near the top of that
    # rally (92% of 30d high), and is showing distribution (4 red days
    # in last 7). The existing `postPumpFilterEnabled` requires the coin
    # to ALREADY have crashed off-peak — but the most damaging entries
    # are RIGHT AT the top BEFORE the crash.
    #
    # Three conditions must ALL be true to block:
    #   1. 30-day return  >= macroTopMinReturnPct   (default 50%)
    #   2. Buy price      >= 30d_high × (macroTopWithinHighPct/100)
    #                         (within X% of 30d high, default 90% → within 10%)
    #   3. Red daily closes in last 7  >= macroTopMinRedDaysIn7  (default 3)
    #
    # Validated against 8 historical trades:
    #   PENDLE: 30d +84%, 92% of high, 4/7 red → BLOCK ✓ (the bad one)
    #   MOVR:   30d +116%, 59% of high → not within 90% → PASS ✓
    #   TIA:    30d +40% → under 50% → PASS ✓
    #   IMX:    30d +26% → under 50% → PASS ✓
    #   DOGE/FLOKI/NXPC/MLN: all PASS ✓
    macroTopFilterEnabled: bool = True
    macroTopMinReturnPct: float = 50.0       # 30-day return >= this
    macroTopWithinHighPct: float = 90.0      # buy_price / 30d_high >= this
    macroTopMinRedDaysIn7: int = 3           # need this many red days in last 7

    # ── Volatility Regime Filter (iter 45, 2026-05-15) ───────────────────
    # Catches the MLN/USDT pattern: a coin recently crashed and is now
    # in an extreme volatility regime. The deep trace showed:
    #
    #   2026-05-13  MLN  -28.43%  (massive crash, daily range $3.15→$2.08)
    #   2026-05-14  MLN  +27.85%  (dead-cat bounce, range $2.17→$3.92)
    #   2026-05-15  MLN  +9.68%   (today: range $2.79→$4.08)
    #
    # We bought during a small intraday rally. The chaos continued and
    # price dropped 9.46% from our fill before partially recovering.
    # iter39 caught it at −2.5% cat-stop but it would be better to not
    # have entered at all.
    #
    # iter45 blocks when EITHER:
    #   1. Max daily range (high-low)/low > volRegimeMaxDailyRangePct (20%)
    #      over the last 5 days — signals violent oscillation
    #   2. Any day in last 5 closed ≤ open × (1 - volRegimeBigCrashPct/100)
    #      — a recent flash crash. Default 15% (the -28.43% crash easily).
    #
    # Validated against 11 historical trades:
    #   MLN: 80.65% range, -28.43% worst day → BLOCK ✓
    #   All 10 other trades pass with zero false positives:
    #   PENDLE 16% / -5%, TIA 16% / -2%, IMX 17% / -3%, FLOKI 10% / -5%,
    #   IOTA 9% / -7%, NXPC 13% / -6%, MOVR 17% / -6%, LINK 8% / -4%, etc.
    volRegimeFilterEnabled: bool = True
    volRegimeMaxDailyRangePct: float = 20.0      # any 5d day range > this
    volRegimeBigCrashPct: float = 15.0           # any 5d day close ≤ open × (1−this)
    volRegimeLookbackDays: int = 5               # window size

    # ── Market Stress Exit (iter 46, 2026-05-15) ─────────────────────────
    # Mid-trade exit BEFORE iter39's 2.5% catastrophic when broader
    # market signals confirm the drop won't reverse. Built from the
    # LINK/USDT 2026-05-15 deep trace:
    #
    #   Time   LINK_drop   BTC_drop   Volume signal
    #   ────   ─────────   ────────   ─────────────
    #   min 75   -0.43%    -0.42%     normal
    #   min 105  -0.62%    -0.38%     normal (never_recovered=true)
    #   min 130  -1.11%    -0.74%     vol $259K (17× baseline!)  ← exit here
    #   min 170  -2.08%    -1.99%     $51K
    #   min 188  -3.33%    -2.52%     iter39 would fire ~min 178 at -2.5%
    #
    # LINK followed BTC almost step-for-step. At min 130 BTC was down
    # 0.74% AND a massive volume spike (17× baseline) signaled clear
    # distribution. iter39 sat waiting for catastrophic (-2.5%) which
    # didn't fire until min 178 — we ignored 48 min of warning.
    #
    # Three triggers (any one fires when held ≥30min AND drop ≥0.5%):
    #   1. BTC weakness:    BTC down ≥ marketStressBtcWeaknessPct% in
    #                       last 30 min  (default 0.5%)
    #   2. Vol capitulation: 1m volume > marketStressVolSpikeMult ×
    #                       pre-signal-baseline AND candle is red
    #                       (default 5×)
    #   3. Red velocity:    ≥ marketStressRedShareThreshold of last 10
    #                       candles are red  (default 70%)
    marketStressExitEnabled: bool = True
    marketStressMinHoldMin: int = 30
    marketStressMinDropPct: float = 1.0      # iter 46 tuned 2026-05-15: 0.5 → 1.0 (kills 3 false positives)
    marketStressBtcWeaknessPct: float = 0.5
    marketStressBtcLookbackMin: int = 30
    marketStressVolSpikeMult: float = 5.0
    marketStressRedShareThreshold: float = 0.7  # only when drop in [0, this]

    # ── Buy 2 staleness cancel (iter 37, 2026-05-15) ─────────────────────
    # If Buy 2 LIMIT hasn't filled within N minutes after Buy 1, the
    # retrace we were averaging-down for never came. Cancel Buy 2 and
    # let the original TP order ride on Buy 1 alone (TP is still placed
    # at avg×(1+tp_pct) which, with only Buy 1, equals Buy 1×(1+tp_pct)
    # — fully reachable on normal momentum).
    #
    # Pattern from FLOKI vs HBAR / QNT: FLOKI's Buy 2 filled in 2 min
    # (fast retrace = healthy mean-reversion = winner). HBAR's Buy 2
    # took 21 min (sustained downtrend = avg-down deepens the hole).
    # Cancelling stale Buy 2 prevents adding capital to a losing trend.
    #
    # The TP order placed when Buy 1 filled was sized for total qty
    # (Buy 1 + Buy 2) at the avg=Buy 1×0.9975 price. If Buy 2 never
    # fills, the bot will refresh the TP from `total_qty()` which only
    # counts filled legs → effectively sells Buy 1 qty at Buy 1 ×
    # (1 + tp_pct). That's the desired behaviour.
    ladderBuy2StalenessEnabled: bool = True
    ladderBuy2StalenessMinutes: int = 10

    # ── Trailing TP (iter 15) ────────────────────────────────────────────
    # Captures bigger upside on winners. When the static TP (set to net
    # ladderTargetNetProfitUsdt) is *reached*, instead of selling we cancel
    # the limit TP and start trailing the running peak. We market-sell
    # when price retraces ladderTrailingTpPct% from that peak.
    #
    # Why: ETHFI on 2026-05-12 hit static TP at +$0.18 net but the 24h
    # high after that point would have been +$1.84 — 10× the captured
    # profit. Trailing TP would have caught most of that.
    #
    # Combined with break-even exit: if price reaches TP → trail. If price
    # never reaches TP and reverses → break-even exit catches it. If price
    # never reaches break-even either → time exit caps the hold time.
    ladderTrailingTpEnabled: bool = True
    ladderTrailingTpPct: float = 0.5          # trail by 0.5% from peak

    # ── Pending-pump-dump cancel (iter 16, 2026-05-13) ────────────────
    # While Buy 1 is sitting as a resting LIMIT (PENDING_BUY_1), monitor
    # the price action. If price first pumps ≥ pendingPumpThresholdPct
    # above our limit (a real bullish move that left our limit cheap)
    # AND then drops ≥ pendingDumpFromPeakPct from that peak, the move
    # has reversed and our limit is about to fill INTO a falling knife.
    # Cancel before the fill.
    #
    # Why: 2026-05-13 GIGGLE/USDT — Buy 1 limit at ~$35.75. Price pumped
    # to $36.09 (+0.95% above limit) then crashed to $35.74 (-0.97% from
    # peak). The operator had to manually cancel; without that, the limit
    # would have filled at $35.75 into a continuing downtrend.
    #
    # Both gates must fire — single-side checks false-positive on normal
    # noise. The minimum age prevents cancelling on a single-tick spike
    # right after order placement.
    pendingPumpDumpCancelEnabled: bool = True
    pendingPumpThresholdPct: float = 0.5      # peak must be >= +0.5% above limit
    pendingDumpFromPeakPct: float = 0.5       # current must be >= 0.5% below peak
    pendingMinAgeSeconds: int = 60            # wait at least 60s before allowing cancel

    # ── Near-top pump filter (iter 38, 2026-05-15) ───────────────────────
    # Pre-buy gate that catches the "bought near the top of a 24h pump"
    # pattern that slipped past every other filter for QNT/USDT on
    # 2026-05-15:
    #   • 24h change      = +7.46%   (under fallingKnife maxChange24hPct=12)
    #   • from 24h high   = -0.82%   (we bought 0.82% below the day's peak)
    #   • 24h range       = $72.51 → $79.34  (≈ 9.4% intraday)
    #   • Bought at       = $78.17 → +7.8% above 24h low
    #
    # The trade entered RIGHT at the top of a multi-day pump and lost
    # money. The existing filters missed it because:
    #   - fallingKnife: +7.46% under the 12% ceiling
    #   - postPumpBleed: looks for +30% pumps in the lookback window
    #   - overbought60mPct: only +0.08% in the last 60min
    #
    # This filter is intentionally narrow — it ONLY fires when BOTH
    # conditions are true:
    #   1. 24h is up materially (≥ nearTopPumpMin24hChangePct)
    #   2. Current price is within nearTopPumpMaxFromHighPct of the
    #      24h high (i.e. no healthy pullback yet)
    #
    # Worked examples:
    #   QNT 2026-05-15: +7.46% / -0.82% from high → BOTH gates fire → SKIP ✓
    #   Coin +8% / -4% from high → first gate fires, second doesn't
    #     (-4% is a healthy pullback) → ALLOW
    #   Coin +2% / -0.5% from high → first gate doesn't fire → ALLOW
    #   BTC +1.5% / 0% from high → first gate doesn't fire → ALLOW
    nearTopPumpFilterEnabled: bool = True
    nearTopPumpMin24hChangePct: float = 5.0    # only when 24h pump >= 5%
    nearTopPumpMaxFromHighPct: float = 2.0     # only when within 2% of 24h high
    # 2026-05-11 iter 3: True (was False). Operator wants instant Buy 1
    # so Buy 2/3 limits go on the book *simultaneously* — no waiting for
    # an aggressive limit to fill before placing the averaging-down legs.
    # NOTE: when ladderBuy1OffsetPct > 0 below, Buy 1 is a LIMIT at the
    # configured offset (this flag is ignored).
    ladderBuy1UseMarketOrder: bool = True
    # 2026-05-11 iter 7: lets operator place Buy 1 as a LIMIT below the
    # signal price (e.g. 0.1 % → Buy 1 at signal × 0.999) instead of a
    # market order. Set to 0 to keep the current market-order behaviour.
    # When >0, ladderBuy1UseMarketOrder is ignored and Buy 1 waits up to
    # limitBuyTimeoutSec for fill.
    # 2026-05-12 iter 12: 0.05 → 0.15 (deeper limit, better entries).
    ladderBuy1OffsetPct: float = 0.15
    # 2026-05-11 iter 4: per-coin cooldown. After a ladder closes the
    # same symbol is blocked for N seconds so the bot doesn't immediately
    # re-enter the same trade.
    ladderCooldownSeconds: int = 14400   # 4 hours

    # ── Hard stop-loss + pre-buy filter pipeline (iter 48, 2026-05-23) ──
    # Operator request: R1/R2/R3 rules historically had NO hard stop-loss
    # and NO falling-knife/vol-regime/etc. filters — only the scalpers did.
    # That caused FIDA-style buys where a "falling knife" got bought blindly
    # by R3 and held with only the trailing-stop-loss (which doesn't fire
    # if the price drops immediately without first peaking).
    #
    # Two protections added in iter 48:
    #
    # 1. Hard stop-loss in PositionMonitor: market-sell when price drops
    #    hardStopLossPct% below the buy price, regardless of TSL state.
    #    Tight default (0.5%) matches the operator's risk preference.
    #
    # 2. Pre-buy filter call: try_buy() consults the dashboard's existing
    #    /api/check-coin endpoint (the same one Pattern Bot uses). That
    #    endpoint runs the full filter stack — falling-knife, vol-regime,
    #    post-pump, near-top, macro-top, overbought, VWAP, RSI, EMA-slope,
    #    market-stress, bad-hours, blacklist. If verdict.blocked, the
    #    buy is skipped. On network error checkCoinFailClosed=True means
    #    "don't trade if we can't check" (safer); set False to fall back
    #    to permissive on dashboard outage.
    hardStopLossPct: float = 0.5
    useCheckCoinFilterEnabled: bool = True
    checkCoinFailClosed: bool = True
    checkCoinTimeoutSec: float = 1.5
    iter48HardSlAndFiltersAppliedAt: str = "2026-05-23"

    # ── Fee-buffer + per-symbol cooldown (iter 52, 2026-05-23) ──────────
    # The MEUSDT incident: R3-STRONG fired three times on the same symbol
    # within 6 seconds (rapid-fire because _triggered clears on mark_sold).
    # On the 3rd buy, BNB had been exhausted by 4 prior fees so Binance
    # took that fee from the base asset itself — leaving us with ~973.88
    # ME held but trying to limit-sell the full 974.61.  Binance rejected
    # with "insufficient balance" — TP never placed, HARD-SL also couldn't
    # market-sell, position stranded at -5% (~$4.80 loss).
    #
    # Two safety fixes:
    #
    # 1. sellQtyFeeBuffer (default 0.999) — multiply the sell qty by this
    #    before round_quantity so a fee paid in base asset doesn't cause
    #    rejection.  Mirrors the Fast Scalper's existing approach.
    #
    # 2. rulesCooldownSeconds (default 300) — after a SELL on a symbol,
    #    Redis key RULES_COOLDOWN:<sym> gets a TTL.  try_buy checks this
    #    key first and skips when present.  Stops the rapid-fire pattern
    #    where stale ST timing data + cleared _triggered = instant re-buy.
    sellQtyFeeBuffer: float = 0.999
    # iter 59 (2026-05-23): cooldown 5min → 4h after the COS double-loss.
    # Bought COS at $0.001428 → stopped out 4s later, then re-bought at
    # $0.001493 24min later (higher price!) → stopped out again.  Both
    # losses while the same coin was still mid-pump.  4h cooldown
    # prevents the same-coin retry pattern.
    rulesCooldownSeconds: int = 14400  # 4h (was 300 = 5min)
    iter52CooldownFeeBufferAppliedAt: str = "2026-05-23"

    # ── Pump-mode TP (iter 59, 2026-05-23) ────────────────────────────
    # When a coin is clearly pumping at buy-fill time, the static
    # $0.40 net TP caps the winner too early.  COS pumped from $0.001428
    # (our buy) to $0.001569 (peak) — would have netted ~+$7.85 with
    # peak-trail instead of −$0.69 with static TP.
    #
    # Pump detection (any of these triggers pump mode):
    #   1. last 5min price change >= pumpModeMin5mChangePct
    #   2. last 30min price change >= pumpModeMin30mChangePct
    #   3. last 15/30 1m candles green >= pumpModeGreenCount
    #      AND last-5min vol >= pumpModeVolSurgeMult × prior-25min
    #
    # When in pump mode:
    #   - SKIP placing the static +$0.40 limit-sell (let winners run)
    #   - Track peak from fill price
    #   - Exit at MARKET when price <= peak × (1 - pumpModeTrailPct/100)
    #   - Max-hold extended to pumpModeMaxHoldSeconds (4h default)
    #   - HARD-SL at hardStopLossPct still fires (-0.5%) as floor
    pumpModeEnabled: bool = True
    pumpModeMin5mChangePct: float = 2.0
    pumpModeMin30mChangePct: float = 3.0
    pumpModeGreenCount: int = 10        # out of last 15 1m candles
    pumpModeVolSurgeMult: float = 3.0
    pumpModeTrailPct: float = 1.5       # exit when price <= peak × (1 - 1.5%)
    pumpModeMaxHoldSeconds: int = 14400 # 4h pump max-hold
    iter59PumpModeAppliedAt: str = "2026-05-23"

    # ── PumpRider detector (iter 55, 2026-05-23) ─────────────────────────
    # New subprocess that catches volume-leads-price pumps directly on 1m
    # closes instead of waiting for R1/R2/R3's ST timing data to update.
    # See booknow/sentiment/scripts/pump_rider.py for the design notes
    # (MEUSDT 07:47-07:50 pump post-mortem).
    pumpRiderEnabled: bool = True
    pumpRiderVolMultipleThreshold: float = 2.5       # last 1m candle vol vs 20-candle baseline
    pumpRiderMinPriceChangePct: float = 0.8          # last candle close-vs-open %
    pumpRiderMinPriorVolMultiple: float = 1.5        # warm-up confirmation
    pumpRiderMinVol24hUsd: float = 1_000_000.0       # liquidity floor
    pumpRiderMaxCumulativeGainPct: float = 8.0       # skip mature pumps
    pumpRiderMaxLookbackCandles: int = 10            # look-back window for cum-gain
    pumpRiderTopSymbols: int = 200                   # iter 61: 50→200 — scan all USDT pairs
    pumpRiderCooldownSec: int = 600                  # 10 min per-symbol
    pumpRiderSellPctLabel: float = 5.0               # forwarded to try_buy (profitAmountUsdt wins)
    iter55PumpRiderAppliedAt: str = "2026-05-23"

    # iter 61 (2026-05-24) — multi-window tiered PumpRider.
    # COS pumped at 08:25 UTC (chg_5m=+1.14%, vol_surge_5m=4.95x).  The
    # old 1m-only rules required chg_1m>=+0.8% which COS missed (only
    # +0.61% on that minute).  New 5m-window rule catches it.
    # Tiers:
    #   EARLY   alert only      vol_surge_5m >= 2x AND chg_5m >= 0.5%
    #   NORMAL  buy             vol_surge_5m >= 3x AND chg_5m >= 1.0%  (catches COS)
    #   STRONG  buy             vol_surge >= 5x AND chg_5m or chg_1m >= 1.5%
    #   MEGA    alert only      chg_5m >= 5% OR chg_1m >= 3% (chasing top)
    pumpRiderStrongVolMult: float = 5.0
    pumpRiderStrongChg5mPct: float = 1.5
    pumpRiderNormal5mVolMult: float = 3.0
    pumpRiderNormal5mChgPct: float = 1.0
    pumpRiderEarly5mVolMult: float = 2.0
    pumpRiderEarly5mChgPct: float = 0.5
    pumpRiderMega5mPct: float = 5.0
    pumpRiderMega1mPct: float = 3.0
    iter61TieredPumpRiderAppliedAt: str = "2026-05-24"

    # iter 62 (2026-05-24) — EARLY_PUMP auto-buy + chg_1h slow detection.
    # Closes two gaps from the spot-movers analysis:
    #   - NIL/PLUME pumped while EP detected them (score 84/75) but EP
    #     doesn't trigger buys — only alerts.  Adding auto-buy at score≥85.
    #   - SUPER/EIGEN/ONDO/WLD pumped at 05:00 UTC with slow steady gain
    #     (1.5%/h) — too slow for chg_5m/chg_30m rules.  chg_1h ≥ 2%
    #     catches them.
    pumpRiderSlow1hChgPct: float = 2.0    # NORMAL tier if 1h change >= 2%
    # iter 64 (2026-05-24) — require mild vol confirmation on the chg_1h rule
    # so pure-price drift (slow grind with no vol behind it) is rejected.
    # vol_surge_5m >= 1.5 means recent 5m vol must be at least 1.5x the prior
    # 25m baseline — gentle enough to not block real slow pumps, strict enough
    # to filter out organic drift on illiquid pairs.
    pumpRiderSlow1hVolMult: float = 1.5
    earlyPumpAutoBuyScore: int = 85       # 0 = disabled; otherwise min EP score
    earlyPumpAutoBuyMaxAgeSec: int = 300  # only act on detections <5min old
    iter62EpAutoBuyAndChg1hAppliedAt: str = "2026-05-24"
    iter64Slow1hVolConfirmAppliedAt: str = "2026-05-24"

    # iter 65 (2026-05-24) — Resistance-break gate for PumpRider.
    # Pre-existing NORMAL/STRONG tiers fire on momentum (price+vol) alone,
    # which lets them buy +1% pops INSIDE a tight range that promptly
    # fade. Add a structural check: require trigger_close to break (or be
    # within tolerance of) the prior-N-bar high. If not, downgrade the
    # signal to EARLY (alert only, no auto-buy).
    #   • Lookback 60 bars = prior 1h on 1m candles.
    #   • Tolerance 0.2% — allows buying just as the break prints rather
    #     than waiting for the close to clear cleanly.
    #   • Applies only to NORMAL/STRONG (EARLY/MEGA are alert-only).
    pumpRiderResistanceBreakEnabled: bool = True
    pumpRiderResistanceLookbackBars: int = 60
    pumpRiderResistanceTolerancePct: float = 0.2
    iter65ResistanceBreakAppliedAt: str = "2026-05-24"

    # iter 66 (2026-05-24) — Orderbook depth pre-check in try_buy.
    # Volume-floor ($2M/24h) alone doesn't guarantee depth AT THE TOP OF
    # BOOK — a coin can trade $5M/day with a 0.5% spread and slip badly
    # on a $96 market buy. Before placing a market buy, fetch top-20
    # asks and verify the cumulative ask depth within `pctOfPrice` of
    # last is at least `multiplier × leg size`. Reject with reason
    # `thin_orderbook` if not.
    #   • multiplier=3.0 → need 3× our leg size in available asks
    #   • pctOfPrice=0.5 → only count asks within 0.5% of last
    #   • timeoutMs=2000 — fail-open on timeout (don't block buys if
    #     Binance is slow), but log a warning
    orderbookDepthCheckEnabled: bool = True
    orderbookDepthMultiplier: float = 3.0
    orderbookDepthPctOfPrice: float = 0.5
    orderbookDepthTimeoutMs: int = 2000
    iter66OrderbookDepthAppliedAt: str = "2026-05-24"

    # iter 56 (2026-05-23) — Early-Pump watchlist intersection.
    # The 51-event Early Pump backtest (today, 2026-05-23) showed the
    # signal alone is unprofitable across every TP/SL combo (best:
    # -$2/10 trades at score 80+).  But the underlying factor structure
    # (green-candle share, vol building, HH/HL, trade-freq surge, buy
    # pressure) is real — it just doesn't pump fast enough alone.
    # Combining with PumpRider's reactive vol-spike trigger should
    # filter to fewer-but-better setups.
    #   off:     ignore Early Pump (legacy behaviour)
    #   prefer:  scan watchlist symbols first (rank-boost, no gate)
    #   require: ONLY buy on coins that are BOTH on PumpRider scan AND
    #            on the Early-Pump watchlist (intersection-only)
    pumpRiderWatchlistMode: str = "prefer"
    pumpRiderWatchlistScoreMin: int = 60
    pumpRiderWatchlistMaxAgeSec: int = 1800          # 30-min freshness
    iter56WatchlistAppliedAt: str = "2026-05-23"

    # ── Telegram alerts (iter 58, 2026-05-23) ────────────────────────────
    # Operator request: get pushed to phone whenever the bot buys, fills,
    # or sells a coin.  Requires TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID
    # env vars; see booknow/util/alerts.py for setup steps.
    alertsEnabled: bool = True
    iter58AlertsAppliedAt: str = "2026-05-23"

    # ── Velocity-aware TSL (iter 57, 2026-05-23) ─────────────────────────
    # The ORCAUSDT post-mortem: bought $1.397, peak $1.400 (10:50), low
    # $1.395 (10:56).  Old TSL fired the moment trail-stop was breached
    # — but the dip took 6 minutes (0.06%/min), basically noise.  iter
    # 57 adds a velocity gate: TSL only fires if drop velocity (% per
    # minute from the peak) >= tslMinDropPctPerMin.  Slow drifts wait;
    # HARD-SL at -0.5% from buy is still the catastrophic floor.
    tslMinDropPctPerMin: float = 0.15

    # ── Dynamic / chasing take-profit (iter 47, 2026-05-23) ──────────────
    # Replaces the static "+$0.20 net" limit-sell with a ratcheting one.
    # See trailing_tp.py for the state machine.  This iter also fixes the
    # DivisionByZero bug where _place_limit_sell was called with
    # executedQty=0 (the normal state of a fresh LIMIT buy that hasn't
    # filled yet).  The fix: defer the limit-sell to the buy-fill
    # executionReport handler in main.py.
    dynamicTpEnabled: bool = True
    dynamicTpMoveStepPct: float = 0.3       # move TP up after +0.3% above current TP
    iter47DynamicTpAppliedAt: str = "2026-05-23"

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
