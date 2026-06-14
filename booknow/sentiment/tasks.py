"""
tasks.py
─────────────────────────────────────────────────────────────────────────────
Definitions of the sentiment-engine subprocesses the python-engine
should supervise.

Mirrors ``binance-sentiment-engine/start_utilities.py``'s ``UTILITIES``
list. Three task kinds:

  - SETUP        run-once at boot, await completion before persistent
                 tasks start (e.g. fee_calculator_util.py seeds a
                 Redis key the others read).
  - PERSISTENT   long-running daemon; supervisor restarts on death
                 with a configurable backoff.
  - SCHEDULED    fire-and-await, sleep ``interval_s``, repeat. Used
                 for hourly refreshers like sync_symbols.py.

These run as subprocesses (matching the existing layout) so we don't
have to refactor 3,388 lines of analyzer code on this branch. Any
analyzer that wants to migrate in-process can drop its module under
``booknow.sentiment`` and we delete its entry here in a follow-up.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple


@dataclass(frozen=True)
class SubprocessTask:
    """One sentiment-engine subprocess.

    ``cmd_argv`` is ``shlex.split``-ready and includes the script name
    plus any flags. The supervisor prepends the project's Python
    interpreter and runs with the sentiment-engine directory as cwd.
    """

    name: str
    cmd_argv: Tuple[str, ...]
    kind: str  # "setup" | "persistent" | "scheduled"
    interval_s: float = 0.0          # only for scheduled
    restart_delay_s: float = 5.0     # for persistent

    def __post_init__(self) -> None:
        if self.kind not in ("setup", "persistent", "scheduled"):
            raise ValueError(f"unknown task kind: {self.kind}")
        if self.kind == "scheduled" and self.interval_s <= 0:
            raise ValueError(f"{self.name}: scheduled task needs interval_s > 0")


# Mirrors start_utilities.py exactly so we don't lose any behaviour.
# Tuple form so subclassing/mutation accidents don't bite us.
SENTIMENT_TASKS: List[SubprocessTask] = [
    # ── Setup (sequential, run before persistent tasks) ──────────────
    SubprocessTask(
        name="Fee Intelligence",
        cmd_argv=("fee_calculator_util.py",),
        kind="setup",
    ),

    # ── Scheduled refreshers ─────────────────────────────────────────
    SubprocessTask(
        name="Symbol Sync",
        cmd_argv=("sync_symbols.py",),
        kind="scheduled",
        interval_s=3600,  # every 1 hour
    ),

    # ── Persistent analyzers (parallel, restart on death) ────────────
    SubprocessTask(
        name="Market Scanner",
        cmd_argv=("market_sentiment_engine.py",),
        kind="persistent",
    ),
    SubprocessTask(
        name="Fast Move Analyzer",
        cmd_argv=("volume_price_analyzer.py", "--daemon"),
        kind="persistent",
    ),
    SubprocessTask(
        name="Fast Scalper",
        cmd_argv=("ultra_fast_scalper.py",),
        kind="persistent",
    ),
    SubprocessTask(
        name="Profit Analyzer",
        cmd_argv=("profit_reached_analyzer.py",),
        kind="persistent",
    ),
    SubprocessTask(
        name="Pattern Recorder",
        cmd_argv=("success_pattern_recorder.py",),
        kind="persistent",
    ),
    SubprocessTask(
        name="Pattern Matcher",
        cmd_argv=("pattern_matching_engine.py",),
        kind="persistent",
    ),
    SubprocessTask(
        name="Profit Trend",
        cmd_argv=("profit_020_trend_analyzer.py",),
        kind="persistent",
    ),
    SubprocessTask(
        name="Virtual Scalper",
        cmd_argv=("virtual_scalp_executor.py",),
        kind="persistent",
    ),
    SubprocessTask(
        name="VP History Recorder",
        cmd_argv=("vp_history_recorder.py",),
        kind="persistent",
    ),
    SubprocessTask(
        name="Stale Cleaner",
        cmd_argv=("stale_cleaner.py",),
        kind="persistent",
    ),
    # iter 55 (2026-05-23) — volume-leads-price pump detector.  Calls
    # the python backend's /api/v1/order/pattern-buy/{sym} endpoint so
    # positions inherit iter47/48/52/54 protections.
    SubprocessTask(
        name="Pump Rider",
        cmd_argv=("pump_rider.py",),
        kind="persistent",
    ),
    # iter 69 (2026-05-24) — Volume-Spike Pattern (VSP) classifier.
    # Detects volume spikes and uses taker-buy-ratio + candle structure
    # to classify direction (BIG_PUMP / BIG_DUMP / MODERATE / UNCERTAIN)
    # and magnitude (0-100).  Paper mode for first 7 days, then live.
    # Live buys delegate via the same pattern-buy endpoint so they
    # inherit iter65/66 + all safety gates.
    SubprocessTask(
        name="Volume Spike Pattern",
        cmd_argv=("volume_spike_pattern.py",),
        kind="persistent",
    ),
    # iter 70 (2026-05-24) — Low Market Cap + High Volume (LMC).
    # Small caps (7d avg vol < $10M) suddenly getting 3x+ volume =
    # explosive-move chance.  Scores 0-100 from 6 factors, classifies
    # direction (PUMP/DUMP/NEUTRAL) via hourly candles + VWAP.
    # Paper mode default; live mode delegates EXPLOSIVE_PUMP buys.
    SubprocessTask(
        name="Low MCap Explosive",
        cmd_argv=("low_mcap_explosive.py",),
        kind="persistent",
    ),
    # iter 72 (2026-05-24) — Calm Consolidation Pattern (CCP).
    # Detects price flat + volume drying up = sellers exhausted /
    # accumulation phase.  Direction bias from position in 24h/7d
    # range: near low = REVERSAL_UP setup, near high = BREAKDOWN_RISK.
    # Paper mode default; live mode delegates CALM_REVERSAL_UP buys.
    SubprocessTask(
        name="Calm Consolidation",
        cmd_argv=("calm_consolidation.py",),
        kind="persistent",
    ),
    # iter167 (2026-06-14) — Smart Buy Re-pricer ("chase-down"), PHASE 1
    # DRY-RUN. Watches opt-in coins' taker buy-vs-sell flow; when sell
    # volume dominates, computes a lower limit-buy price and PUBLISHES a
    # dry-run reprice signal (BUY_REPRICER:SIGNALS:<date>). Places NO real
    # orders — live execution (buyRepricerLiveEnabled) is not wired yet.
    SubprocessTask(
        name="Buy Repricer",
        cmd_argv=("buy_repricer.py",),
        kind="persistent",
    ),
    # iter168 (2026-06-14) — Auto Exit Bracket ("away-mode protector"),
    # PHASE 1 DRY-RUN. Scans ALL holdings that have NO open sell order and
    # publishes an intended +30% take-profit / -4% stop-loss bracket
    # (AUTO_EXIT:STATE + AUTO_EXIT:SIGNALS:<date>) computed from each coin's
    # actual BUY cost basis. Places NO real orders — live OCO execution
    # (autoExitLiveEnabled) is Phase 2 and intentionally not wired yet. This
    # watcher is INDEPENDENT of HARD_DISABLE_AUTOSELL (turning it on does not
    # re-enable any bot ladder/auto exits).
    SubprocessTask(
        name="Auto Exit Bracket",
        cmd_argv=("auto_exit_bracket.py",),
        kind="persistent",
    ),
]
