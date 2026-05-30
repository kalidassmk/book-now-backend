"""
Offline unit tests for the order-flow scalper logic (no network required).

Feeds synthetic aggTrade + depth events into the analyzer and asserts that the
checklist conditions and BUY/SELL/HOLD signals fire as documented.

Run: python -m pytest tests/test_scalper_order_flow.py
  or: python tests/test_scalper_order_flow.py
"""

import time

from booknow.scalper.config import ScalperConfig
from booknow.scalper.order_flow import OrderFlowAnalyzer


def _book(mid: float, buy_wall: bool, sell_wall: bool):
    """A 20-level book; optionally plant a large wall on a side."""
    bids = [{"price": mid - i, "qty": 1.0} for i in range(1, 21)]
    asks = [{"price": mid + i, "qty": 1.0} for i in range(1, 21)]
    if buy_wall:
        bids[3]["qty"] = 50.0  # ~50x the others → wall
    if sell_wall:
        asks[3]["qty"] = 50.0
    return {
        "bids": [[b["price"], b["qty"]] for b in bids],
        "asks": [[a["price"], a["qty"]] for a in asks],
    }


def _trade(ts: float, price: float, qty: float, is_buy: bool):
    # m == True → buyer is maker → market SELL; so is_buy → m=False
    return {"p": str(price), "q": str(qty), "T": int(ts * 1000), "m": not is_buy}


def test_buy_signal():
    cfg = ScalperConfig(window_sec=5.0, baseline_sec=30.0, min_trades=3)
    a = OrderFlowAnalyzer("BTCUSDT", cfg)
    t0 = time.time()

    a.on_depth(_book(100, buy_wall=False, sell_wall=False))
    a.on_trade(_trade(t0 - 8, 100, 0.5, is_buy=False))
    a.on_trade(_trade(t0 - 7, 100, 0.5, is_buy=False))
    a.evaluate(now=t0 - 5)  # establishes prev-window + wall state

    a.on_depth(_book(100, buy_wall=True, sell_wall=False))
    for i in range(8):
        a.on_trade(_trade(t0 - 4 + i * 0.4, 100 + i * 0.1, 3.0, is_buy=True))

    snap = a.evaluate(now=t0)
    bc = snap["buy_conditions"]
    assert bc["delta_turning_positive"], snap["metrics"]
    assert bc["market_buys_increasing"], snap["metrics"]
    assert bc["buy_wall_below_price"], snap["walls"]
    assert bc["no_large_sell_wall_above"], snap["walls"]
    assert bc["volume_spike"], snap["metrics"]
    assert snap["signal"] == "BUY", snap


def test_sell_signal():
    cfg = ScalperConfig(window_sec=5.0, baseline_sec=30.0, min_trades=3)
    a = OrderFlowAnalyzer("ETHUSDT", cfg)
    t0 = time.time()

    a.on_depth(_book(100, buy_wall=True, sell_wall=False))
    a.on_trade(_trade(t0 - 8, 100, 2.0, is_buy=True))
    a.on_trade(_trade(t0 - 7, 100, 2.0, is_buy=True))
    a.evaluate(now=t0 - 5)

    a.on_depth(_book(100, buy_wall=False, sell_wall=True))
    for i in range(8):
        a.on_trade(_trade(t0 - 4 + i * 0.4, 100 - i * 0.1, 3.0, is_buy=False))

    snap = a.evaluate(now=t0)
    sc = snap["sell_conditions"]
    assert sc["delta_negative"], snap["metrics"]
    assert sc["market_sells_increasing"], snap["metrics"]
    assert sc["large_sell_wall_above"], snap["walls"]
    assert sc["buy_wall_disappears"], snap["walls"]
    assert snap["signal"] == "SELL", snap


def test_hold_when_mixed():
    cfg = ScalperConfig(window_sec=5.0, baseline_sec=30.0, min_trades=3)
    a = OrderFlowAnalyzer("SOLUSDT", cfg)
    t0 = time.time()
    a.on_depth(_book(100, buy_wall=False, sell_wall=False))
    for i in range(6):
        a.on_trade(_trade(t0 - 4 + i * 0.5, 100, 0.5, is_buy=(i % 2 == 0)))
    snap = a.evaluate(now=t0)
    assert snap["signal"] == "HOLD", snap


if __name__ == "__main__":
    test_buy_signal()
    test_sell_signal()
    test_hold_when_mixed()
    print("All order-flow scalper tests passed.")
