"""
Execution Layer — Binance API order placement and account management.

Supports both LIVE and PAPER (simulation) modes.
"""

import time
import hmac
import hashlib
import logging
import requests
import urllib3
from dataclasses import dataclass, field
from urllib.parse import urlencode

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

log = logging.getLogger("regime_trader.executor")

BASE_URL = "https://api.binance.com"


@dataclass
class Position:
    """Tracks an open position."""
    symbol: str
    side: str            # BUY or SELL
    entry_price: float
    quantity: float
    stop_loss: float
    take_profit: float
    entry_time: float = 0.0
    order_id: str = ""
    strategy: str = ""
    pnl_pct: float = 0.0


@dataclass
class TradeRecord:
    """Completed trade record."""
    symbol: str
    side: str
    entry_price: float
    exit_price: float
    quantity: float
    pnl_pct: float
    pnl_usdt: float
    strategy: str
    entry_time: float
    exit_time: float
    reason: str = ""


class BinanceExecutor:
    """
    Handles order placement and account queries via Binance REST API.

    Modes:
        live=False (default): Paper trading — logs orders but doesn't execute
        live=True: Real orders via Binance API
    """

    def __init__(self, api_key: str = "", api_secret: str = "",
                 live: bool = False, trade_amount_usdt: float = 100.0):
        self.api_key = api_key
        self.api_secret = api_secret
        self.live = live
        self.trade_amount_usdt = trade_amount_usdt

        self.position: Position | None = None
        self.trade_history: list[TradeRecord] = []
        self.total_pnl_usdt = 0.0
        self.win_count = 0
        self.loss_count = 0

        self._session = requests.Session()
        self._session.verify = False  # Disable SSL verification
        self._session.headers.update({
            "X-MBX-APIKEY": self.api_key,
        })

    # ── Public API ────────────────────────────────────────────────────

    def open_position(self, symbol: str, price: float, stop_loss: float,
                      take_profit: float, strategy: str,
                      position_size_pct: float = 100.0) -> Position | None:
        """Open a new position (BUY)."""
        if self.position is not None:
            log.info("[Executor] Already in position %s. Skipping new entry.", self.position.symbol)
            return None

        # Scale trade amount by position_size_pct
        effective_amount = self.trade_amount_usdt * (position_size_pct / 100.0)
        quantity = effective_amount / price if price > 0 else 0

        if quantity <= 0:
            log.warning("[Executor] Invalid quantity for %s at price %.8f", symbol, price)
            return None

        if self.live:
            order = self._place_market_buy(symbol, quantity)
            if not order:
                return None
            order_id = str(order.get("orderId", ""))
            fill_price = float(order.get("fills", [{}])[0].get("price", price))
        else:
            order_id = f"PAPER-{int(time.time() * 1000)}"
            fill_price = price
            log.info(
                "📝 [PAPER BUY] %s qty=%.6f @ %.8f | SL=%.8f TP=%.8f | %s",
                symbol, quantity, price, stop_loss, take_profit, strategy
            )

        self.position = Position(
            symbol=symbol,
            side="BUY",
            entry_price=fill_price,
            quantity=quantity,
            stop_loss=stop_loss,
            take_profit=take_profit,
            entry_time=time.time(),
            order_id=order_id,
            strategy=strategy,
        )

        return self.position

    def close_position(self, current_price: float, reason: str = "") -> TradeRecord | None:
        """Close the current position (SELL)."""
        if self.position is None:
            return None

        pos = self.position
        pnl_pct = ((current_price - pos.entry_price) / pos.entry_price) * 100
        pnl_usdt = (current_price - pos.entry_price) * pos.quantity

        if self.live:
            order = self._place_market_sell(pos.symbol, pos.quantity)
            if not order:
                return None
            fill_price = float(order.get("fills", [{}])[0].get("price", current_price))
        else:
            fill_price = current_price
            emoji = "🟢" if pnl_pct > 0 else "🔴"
            log.info(
                "%s [PAPER SELL] %s qty=%.6f @ %.8f | PnL: %.2f%% ($%.4f) | %s",
                emoji, pos.symbol, pos.quantity, current_price,
                pnl_pct, pnl_usdt, reason
            )

        record = TradeRecord(
            symbol=pos.symbol,
            side="SELL",
            entry_price=pos.entry_price,
            exit_price=fill_price,
            quantity=pos.quantity,
            pnl_pct=round(pnl_pct, 2),
            pnl_usdt=round(pnl_usdt, 4),
            strategy=pos.strategy,
            entry_time=pos.entry_time,
            exit_time=time.time(),
            reason=reason,
        )

        self.trade_history.append(record)
        self.total_pnl_usdt += pnl_usdt
        if pnl_pct > 0:
            self.win_count += 1
        else:
            self.loss_count += 1

        self.position = None
        return record

    def check_stops(self, current_price: float) -> TradeRecord | None:
        """Check if current price hits stop-loss or take-profit."""
        if self.position is None:
            return None

        pos = self.position
        pos.pnl_pct = ((current_price - pos.entry_price) / pos.entry_price) * 100

        if current_price <= pos.stop_loss:
            return self.close_position(current_price, reason="STOP_LOSS")
        elif current_price >= pos.take_profit:
            return self.close_position(current_price, reason="TAKE_PROFIT")

        return None

    def get_balance(self, asset: str = "USDT") -> float:
        """Get available balance for an asset."""
        if not self.live:
            return self.trade_amount_usdt * 10  # Simulated balance

        try:
            endpoint = "/api/v3/account"
            params = {"timestamp": int(time.time() * 1000)}
            params["signature"] = self._sign(params)
            resp = self._session.get(f"{BASE_URL}{endpoint}", params=params, timeout=10)
            data = resp.json()
            for bal in data.get("balances", []):
                if bal["asset"] == asset:
                    return float(bal["free"])
        except Exception as e:
            log.error("[Executor] Balance check failed: %s", e)
        return 0.0

    def get_stats(self) -> dict:
        """Get trading statistics."""
        total_trades = self.win_count + self.loss_count
        win_rate = (self.win_count / total_trades * 100) if total_trades > 0 else 0
        return {
            "total_trades": total_trades,
            "wins": self.win_count,
            "losses": self.loss_count,
            "win_rate": round(win_rate, 1),
            "total_pnl_usdt": round(self.total_pnl_usdt, 4),
            "current_position": self.position.symbol if self.position else None,
        }

    # ── Binance API Helpers ───────────────────────────────────────────

    def _place_market_buy(self, symbol: str, quantity: float) -> dict | None:
        return self._place_order(symbol, "BUY", quantity)

    def _place_market_sell(self, symbol: str, quantity: float) -> dict | None:
        return self._place_order(symbol, "SELL", quantity)

    def _place_order(self, symbol: str, side: str, quantity: float) -> dict | None:
        try:
            endpoint = "/api/v3/order"
            params = {
                "symbol": symbol,
                "side": side,
                "type": "MARKET",
                "quantity": f"{quantity:.8f}",
                "timestamp": int(time.time() * 1000),
            }
            params["signature"] = self._sign(params)

            log.info("[Binance API] %s %s qty=%.8f", side, symbol, quantity)
            resp = self._session.post(f"{BASE_URL}{endpoint}", params=params, timeout=10)
            data = resp.json()

            if resp.status_code != 200:
                log.error("[Binance API] Order failed: %s", data)
                return None

            log.info("[Binance API] Order filled: orderId=%s status=%s",
                     data.get("orderId"), data.get("status"))
            return data

        except Exception as e:
            log.error("[Binance API] Order error: %s", e)
            return None

    def _sign(self, params: dict) -> str:
        """Create HMAC-SHA256 signature for Binance API."""
        query_string = urlencode(params)
        return hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
