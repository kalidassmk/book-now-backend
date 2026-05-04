"""
main.py
─────────────────────────────────────────────────────────────────────────────
Single entrypoint for the BookNow Python engine.

Run:
    python -m booknow.main
    # or, after `pip install -e python-engine/`:
    booknow

This module is the orchestrator that boots every async task in the
system, in the order they depend on each other:

    1. Logging, settings, rate-limit guard
    2. Redis client (singleton) + ping
    3. TradingConfigService (Redis-backed, dashboard-editable)
    4. WS-API + REST clients (Binance transports)
    5. FilterService + DelistService (cache exchange info / delistings)
    6. MarketStreamService (one combined WS, fans into Redis)
    7. Four processors: ULF0to3, FastAnalyse, TimeAnalyser, FastMoveFilter
    8. TradeState + TrailingStopLoss + CoinAnalyzer + TradeExecutor
    9. PositionMonitor (max-hold timer + TSL trigger)
   10. Rules engine: RuleOne, RuleTwo, RuleThree
   11. SubsystemRegistry (risk_management / fakeout / volume / trend / meta)
   12. SentimentSupervisor (subprocess fleet)
   13. Live-mode services: BalanceService + DustService + UserDataStream
   14. FastAPI HTTP layer (uvicorn in this same event loop)

Shutdown reverses the order so nothing tears down a dependency in use.
SIGINT and SIGTERM both trigger the shutdown event; ``/api/v1/stop``
sends SIGTERM to ourselves to reuse the same path.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
from typing import List, Optional

from booknow.analysis.coin_analyzer import CoinAnalyzer
from booknow.api import AppState, build_app
from booknow.api.app import HttpServer
from booknow.binance.balances import BalanceService
from booknow.binance.delist import DelistService
from booknow.binance.dust import DustService
from booknow.binance.filters import FilterService
from booknow.binance.rate_limit import get_default as get_rate_limit_guard
from booknow.binance.rest_api import RestApiClient
from booknow.binance.user_data import UserDataStreamService
from booknow.binance.ws_api import WsApiClient
from booknow.binance.ws_streams import MarketStreamService
from booknow.config.settings import get_settings
from booknow.processors.fast_analyse import FastAnalyse
from booknow.processors.fast_move_filter import FastMoveFilter
from booknow.processors.time_analyser import TimeAnalyser
from booknow.processors.ulf_0_to_3 import UlfZeroToThree
from booknow.repository.redis_client import close_redis, get_redis
from booknow.config.trading_config import TradingConfigService
from booknow.rules.rule_one import RuleOne
from booknow.rules.rule_two import RuleTwo
from booknow.rules.rule_three import RuleThree
from booknow.sentiment.supervisor import SentimentSupervisor
from booknow.sentiment.tasks import SENTIMENT_TASKS
from booknow.subsystems import SubsystemRegistry
from booknow.trading.executor import TradeExecutor
from booknow.trading.monitor import LoggingExecutor, PositionMonitor
from booknow.trading.state import TradeState
from booknow.trading.tsl import TrailingStopLoss


def _configure_logging(debug: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )
    # Quiet down noisy libraries until we actually need their detail.
    for name in ("websockets", "asyncio", "urllib3", "httpx"):
        logging.getLogger(name).setLevel(logging.WARNING)


async def _bootstrap() -> None:
    settings = get_settings()
    _configure_logging(settings.debug)
    log = logging.getLogger("booknow.main")

    log.info("BookNow Python engine starting…")
    log.info(
        "  live_mode=%s  http_port=%d  redis=%s:%d  debug=%s",
        settings.live_mode, settings.http_port,
        settings.redis_host, settings.redis_port, settings.debug,
    )
    if not settings.binance_api_key:
        log.warning("  BINANCE_API_KEY is not set — trading endpoints will fail until configured.")

    guard = get_rate_limit_guard()
    log.info("  rate-limit guard ready (banned=%s)", guard.is_banned())

    # Redis client (lazy singleton). Touching it now so any connection
    # error surfaces during boot rather than at the first event.
    redis = get_redis()
    try:
        await redis.ping()
        log.info("  redis ping OK")
    except Exception as e:
        log.warning("  redis ping FAILED: %s — continuing; tasks may fail until Redis is up", e)

    # ── TradingConfig (Redis-backed, dashboard-editable) ─────────────
    config_service = TradingConfigService(redis_client=redis)
    initial_config = await config_service.init()
    log.info(
        "  trading-config loaded: autoBuy=%s fastScalp=%s buyAmount=$%s profit=$%s tsl=%s%% maxHold=%ss",
        initial_config.autoBuyEnabled, initial_config.fastScalpMode,
        initial_config.buyAmountUsdt, initial_config.profitAmountUsdt,
        initial_config.tslPct, initial_config.maxHoldSeconds,
    )

    # WS-API client — always instantiated (it doesn't connect until used).
    # Live methods (signed orders) only fire when settings.live_mode is True.
    ws_api = WsApiClient(
        api_key=settings.binance_api_key,
        secret_key=settings.binance_secret_key,
    )

    # ── Phase 6: shared REST client + cache services ─────────────────
    rest = RestApiClient(
        api_key=settings.binance_api_key,
        secret_key=settings.binance_secret_key,
    )

    # FilterService + DelistService run regardless of live_mode — both
    # only hit public endpoints and the rest of the engine reads their
    # caches before placing any order.
    filter_service = FilterService(redis_client=redis, rest=rest)
    await filter_service.start()
    log.info("  filter-service task started")

    delist_service = DelistService(redis_client=redis, rest=rest)
    await delist_service.start()
    log.info("  delist-service task started (current set: %d symbols)",
             len(await delist_service.get_set()))

    # ── Phase 5: market data fan-in (always on, public stream) ───────
    # Pulls its delist set from DelistService so newly-announced removals
    # propagate without a restart.
    market_stream = MarketStreamService(
        redis_client=redis,
        delist=await delist_service.get_set(),
    )
    await market_stream.start()
    log.info("  market-stream task started")

    # ── Phase 8: four processor loops ────────────────────────────────
    # All four read what market_stream writes and emit derived signals
    # the rules engine (Phase 11) and dashboards consume.
    ulf            = UlfZeroToThree(redis_client=redis)
    fast_analyse   = FastAnalyse(redis_client=redis)
    time_analyser  = TimeAnalyser(redis_client=redis)
    fast_move      = FastMoveFilter(redis_client=redis)
    for proc in (ulf, fast_analyse, time_analyser, fast_move):
        await proc.start()
    log.info("  processors started: ulf_0_to_3, fast_analyse, time_analyser, fast_move_filter")

    # ── Phase 9 + 10: state, TSL, monitor, and the real TradeExecutor ─
    trade_state = TradeState()
    tsl = TrailingStopLoss(trailing_percentage=initial_config.tslPct)

    # CoinAnalyzer — used by both the trade-executor's pre-buy gate and
    # the /api/v1/analyze/{symbol} endpoint. One per engine, shared.
    coin_analyzer = CoinAnalyzer()

    # The real executor — paper mode is just live_mode=False on this same
    # class; it logs intended orders without sending them. Replaces the
    # LoggingExecutor stub Phase 9 used.
    trade_executor = TradeExecutor(
        redis_client=redis,
        ws_api=ws_api,
        filter_service=filter_service,
        delist_service=delist_service,
        trade_state=trade_state,
        tsl=tsl,
        config_service=config_service,
        dust_service=None,            # filled in below in live mode
        coin_analyzer=coin_analyzer,
        live_mode=settings.live_mode,
    )

    position_monitor = PositionMonitor(
        redis_client=redis,
        trade_state=trade_state,
        tsl=tsl,
        executor=trade_executor,
        max_hold_seconds=initial_config.maxHoldSeconds,
    )
    await position_monitor.start()
    log.info(
        "  position-monitor started (executor=%s, TSL=%.1f%%, max-hold=%ss)",
        "TradeExecutor[live]" if settings.live_mode else "TradeExecutor[paper]",
        initial_config.tslPct, initial_config.maxHoldSeconds,
    )

    # ── Phase 11: Rules engine — R1 / R2 / R3 ────────────────────────
    # Each reads ST*/CURRENT_PRICE and calls trade_executor.try_buy()
    # when its pattern fires. Sell-listeners on TradeState clear the
    # per-symbol triggered guard on close so the same coin can be
    # scalped again on the next signal.
    rule_one   = RuleOne(   redis_client=redis, trade_state=trade_state,
                            trade_executor=trade_executor, config_service=config_service)
    rule_two   = RuleTwo(   redis_client=redis, trade_state=trade_state,
                            trade_executor=trade_executor, config_service=config_service)
    rule_three = RuleThree( redis_client=redis, trade_state=trade_state,
                            trade_executor=trade_executor, config_service=config_service)
    for r in (rule_one, rule_two, rule_three):
        await r.start()
    log.info("  rules engine started: rule_one (R1-FULL/PARTIAL/ULTRA), rule_two, rule_three")

    # ── Phase 13: subsystem fetchers ─────────────────────────────────
    # Owns one shared KlinesCache (multiplexed WS) + one httpx client +
    # one fapi client. Five fetchers hang off it (risk_management,
    # fakeout_detector, volume_profile, trend_alignment, meta_model).
    # Each subsystem strategy module gets the fetcher it needs in its
    # constructor — no CCXT dependency, and a single WS connection
    # serves the whole engine.
    subsystems = SubsystemRegistry()
    await subsystems.start()
    log.info(
        "  subsystem fetchers ready: risk_management, fakeout_detector, "
        "volume_profile, trend_alignment, meta_model"
    )

    # ── Phase 12: sentiment supervisor (subprocesses) ────────────────
    # Boots the existing binance-sentiment-engine analyzers under a
    # single Python process. Each runs as an asyncio-managed
    # subprocess; the supervisor restarts persistent ones on death
    # and re-runs scheduled ones every interval. Toggle off via
    # BOOKNOW_SENTIMENT_ENABLED=false to run the trading core alone.
    sentiment_supervisor: Optional[SentimentSupervisor] = None
    if settings.sentiment_enabled:
        from pathlib import Path as _Path
        if settings.sentiment_dir:
            sentiment_dir = _Path(settings.sentiment_dir).resolve()
        else:
            # python-engine/booknow/main.py
            #   .parent → python-engine/booknow
            # The legacy binance-sentiment-engine/ tree was consolidated
            # into the engine package in Phase 19; the supervisor spawns
            # its subprocesses from this in-tree directory now.
            sentiment_dir = _Path(__file__).resolve().parent / "sentiment" / "scripts"
        if not sentiment_dir.exists():
            log.warning(
                "  sentiment supervisor SKIPPED — directory not found: %s "
                "(set BOOKNOW_SENTIMENT_DIR or BOOKNOW_SENTIMENT_ENABLED=false)",
                sentiment_dir,
            )
        else:
            # Per-task log files land alongside the main engine log so a
            # `tail -f logs/sentiment/<name>.log` from the repo root just works.
            sentiment_log_dir = (
                _Path(__file__).resolve().parent.parent.parent / "logs" / "sentiment"
            )
            sentiment_supervisor = SentimentSupervisor(
                tasks=SENTIMENT_TASKS,
                sentiment_dir=sentiment_dir,
                log_dir=sentiment_log_dir,
            )
            await sentiment_supervisor.start()
            log.info(
                "  sentiment supervisor started from %s (per-task logs: %s)",
                sentiment_dir, sentiment_log_dir,
            )
    else:
        log.info("  sentiment supervisor disabled (BOOKNOW_SENTIMENT_ENABLED=false)")

    # ── Phase 4 + dust: live-mode-only services ──────────────────────
    user_data: UserDataStreamService | None = None
    dust_service: DustService | None = None
    balance_service: BalanceService | None = None
    if settings.live_mode:
        if not (settings.binance_api_key and settings.binance_secret_key):
            log.error("  live_mode=True but Binance keys missing — user-data-stream + dust disabled")
        else:
            balance_service = BalanceService(redis_client=redis, ws_api=ws_api)
            dust_service = DustService(
                redis_client=redis, rest=rest, filter_service=filter_service,
            )
            await dust_service.start()
            # Hand the dust service to the executor so +TARGET HIT and
            # forced exits sweep leftover base-asset to BNB automatically.
            trade_executor.set_dust_service(dust_service)

            # User-data-stream pushes balance updates to BalanceService
            # AND triggers DustService's per-account dust evaluation.
            async def _on_balance_snapshot(balances):
                await dust_service.evaluate_balances(balances)

            # executionReport callback: when our outstanding limit-sell
            # at +$0.20 fills on Binance, close the position immediately
            # (don't wait for the position monitor's next tick to notice).
            # Sweep dust to BNB after a clean +TARGET HIT.
            async def _on_execution_report(event):
                if event.get("X") != "FILLED":
                    return
                symbol = event.get("s")
                order_id = event.get("i")
                if not symbol or order_id is None:
                    return
                pos = trade_state.get_position(symbol)
                if pos is None:
                    return
                if pos.open_sell_order_id is None or pos.open_sell_order_id != order_id:
                    return
                price = event.get("p") or event.get("L") or "0"
                log.info(
                    "[+TARGET HIT] limit-sell #%s for %s filled @ %s — closing position",
                    order_id, symbol, price,
                )
                trade_state.mark_sold(symbol)
                tsl.reset(symbol)
                # Sweep the leftover base-asset dust.
                base = symbol[:-4] if symbol.endswith("USDT") else symbol
                try:
                    await dust_service.sweep_to_bnb(base)
                except Exception as e:
                    log.warning("[+TARGET HIT] dust sweep failed for %s: %s", base, e)

            user_data = UserDataStreamService(
                ws_api=ws_api,
                balance_service=balance_service,
                on_execution_report=_on_execution_report,
            )
            # Tee every account snapshot into the dust evaluator. Uses
            # the public listener API rather than monkey-patching the
            # method, so static analysis stays clean.
            balance_service.add_snapshot_listener(dust_service.evaluate_balances)

            await balance_service.seed_from_rest()
            await user_data.start()
            log.info("  user-data-stream + dust-service started")
    else:
        log.info("  user-data-stream + dust-service skipped (live_mode=False)")

    # ── Phase 14: FastAPI HTTP layer ─────────────────────────────────
    # Mounts every dashboard-facing endpoint the Spring REST surface
    # used to host. Started last so AppState has every service wired
    # already; stopped first during shutdown so in-flight HTTP calls
    # don't hit a half-torn-down engine.
    app_state = AppState(
        settings=settings,
        redis=redis,
        config_service=config_service,
        trade_state=trade_state,
        trade_executor=trade_executor,
        ws_api=ws_api,
        rest=rest,
        filter_service=filter_service,
        balance_service=balance_service,  # None in paper mode, set in live
        dust_service=dust_service,
        coin_analyzer=coin_analyzer,
    )
    fastapi_app = build_app(app_state)
    http_server = HttpServer(fastapi_app, host="0.0.0.0", port=settings.http_port)
    await http_server.start()

    log.info("Engine running. Press Ctrl-C to stop.")

    # Idle until interrupted. Subsequent phases will spawn their own
    # tasks here; main.py's job is to supervise them and shut down
    # cleanly on SIGINT/SIGTERM.
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass  # Windows; KeyboardInterrupt will bubble.
    try:
        await stop.wait()
    finally:
        log.info("BookNow Python engine stopping…")
        # HTTP server first — refuse new requests so the shutdown of
        # services below doesn't race with in-flight calls. Existing
        # connections are drained by uvicorn during stop().
        try:
            await http_server.stop()
        except Exception as e:
            log.warning("  http-server stop error: %s", e)
        # Sentiment supervisor next — its child processes hit Binance
        # and Redis on their own; let them shut down before we close
        # things they depend on.
        if sentiment_supervisor is not None:
            try:
                await sentiment_supervisor.stop()
            except Exception as e:
                log.warning("  sentiment-supervisor stop error: %s", e)
        # Rules next (they call into the executor), then position
        # monitor (also calls the executor) — by the time we tear
        # executor state down, no caller is mid-flight.
        for r in (rule_three, rule_two, rule_one):
            try:
                await r.stop()
            except Exception as e:
                log.warning("  %s stop error: %s", r.name, e)
        try:
            await position_monitor.stop()
        except Exception as e:
            log.warning("  position-monitor stop error: %s", e)
        # Then processors, then market_stream — same reasoning.
        for proc in (fast_move, time_analyser, fast_analyse, ulf):
            try:
                await proc.stop()
            except Exception as e:
                log.warning("  %s stop error: %s", proc.name, e)
        try:
            await market_stream.stop()
        except Exception as e:
            log.warning("  market-stream stop error: %s", e)
        # Subsystem registry: closes its KlinesCache WS + httpx client.
        # Safe to do here — nothing in the trade core depends on it.
        try:
            await subsystems.stop()
        except Exception as e:
            log.warning("  subsystems stop error: %s", e)
        if user_data is not None:
            try:
                await user_data.stop()
            except Exception as e:
                log.warning("  user-data-stream stop error: %s", e)
        if dust_service is not None:
            try:
                await dust_service.stop()
            except Exception as e:
                log.warning("  dust-service stop error: %s", e)
        try:
            await delist_service.stop()
        except Exception as e:
            log.warning("  delist-service stop error: %s", e)
        try:
            await filter_service.stop()
        except Exception as e:
            log.warning("  filter-service stop error: %s", e)
        try:
            await coin_analyzer.aclose()
        except Exception:
            pass
        try:
            await rest.aclose()
        except Exception:
            pass
        try:
            await close_redis()
        except Exception:
            pass
        log.info("BookNow Python engine stopped.")


def run() -> None:
    """Sync entrypoint exposed via the `booknow` console script."""
    try:
        asyncio.run(_bootstrap())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    run()
