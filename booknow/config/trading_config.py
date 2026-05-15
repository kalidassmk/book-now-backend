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
    profitPct: float = 0.6
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
    ladderTargetNetProfitUsdt: float = 0.20
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
    active2HardStopPct: float = 1.5             # worst-case cap from new avg  # only when drop in [0, this]

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
