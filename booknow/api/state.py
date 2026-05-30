"""
state.py
─────────────────────────────────────────────────────────────────────────────
Container that bundles every long-lived service the HTTP routes need.

main.py builds one of these once it has wired the trading core, then
hands it to ``build_app(state)``. Routes pull what they need via
:func:`get_state` (FastAPI dependency).

Keeping this in one place means the routes don't need to know how the
engine was bootstrapped — they just see "give me the executor / config
service / WS-API client".
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

# These imports describe types only. We forward-ref them via TYPE_CHECKING
# so api/* modules don't pull half the engine into their import graph
# until they're actually used.
if TYPE_CHECKING:  # pragma: no cover
    import redis.asyncio as aioredis
    from booknow.analysis.coin_analyzer import CoinAnalyzer
    from booknow.binance.balances import BalanceService
    from booknow.binance.dust import DustService
    from booknow.binance.filters import FilterService
    from booknow.binance.rest_api import RestApiClient
    from booknow.binance.ws_api import WsApiClient
    from booknow.config.trading_config import TradingConfigService
    from booknow.config.settings import Settings
    from booknow.scalper.engine import ScalperEngine
    from booknow.trading.executor import TradeExecutor
    from booknow.trading.state import TradeState


@dataclass
class AppState:
    """Read-only handle to the running engine's services.

    Routes never mutate fields on this — they call methods on the
    underlying services. The dataclass is just a typed bag.
    """

    settings: "Settings"
    redis: "aioredis.Redis"
    config_service: "TradingConfigService"
    trade_state: "TradeState"
    trade_executor: "TradeExecutor"
    ws_api: "WsApiClient"
    rest: "RestApiClient"
    filter_service: "FilterService"
    balance_service: Optional["BalanceService"] = None  # live_mode only
    dust_service: Optional["DustService"] = None        # live_mode only
    coin_analyzer: Optional["CoinAnalyzer"] = None
    scalper_engine: Optional["ScalperEngine"] = None     # order-flow scalper
