"""
booknow.scalper
─────────────────────────────────────────────────────────────────────────────
Order-flow scalping subsystem.

Streams Binance ``aggTrade`` + ``depth`` over websockets and evaluates the
scalper order-flow checklist (delta, market buy/sell pressure, order-book walls
and volume spikes) to produce live BUY / SELL / HOLD signals.

Public entry point:

    from booknow.scalper import ScalperEngine
    engine = ScalperEngine()
    await engine.start()
    ...
    await engine.stop()
"""

from booknow.scalper.config import ScalperConfig
from booknow.scalper.engine import ScalperEngine
from booknow.scalper.order_flow import OrderFlowAnalyzer

__all__ = ["ScalperConfig", "ScalperEngine", "OrderFlowAnalyzer"]
