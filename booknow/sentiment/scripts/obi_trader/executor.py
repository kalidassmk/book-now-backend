import logging
import time
import requests
import hmac
import hashlib
import urllib3
from urllib.parse import urlencode

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

log = logging.getLogger("obi_trader.executor")

class ExecutionManager:
    """
    Handles Binance orders and account balance.
    Supports Live and Paper trading modes.
    """
    def __init__(self, api_key="", api_secret="", live=False, trade_amount_usdt=12.0):
        self.api_key = api_key
        self.api_secret = api_secret
        self.live = live
        self.trade_amount_usdt = trade_amount_usdt
        self.position = None
        self.pnl_history = []

    def open_position(self, symbol, price, side, walls=None):
        qty = self.trade_amount_usdt / price
        
        if self.live:
            # Place real market order
            order = self._place_order(symbol, side, qty)
            if order:
                self.position = {"symbol": symbol, "entry_price": float(order.get('fills', [{}])[0].get('price', price)), "qty": qty}
        else:
            # Paper trade
            self.position = {"symbol": symbol, "entry_price": price, "qty": qty, "time": time.time()}
            log.info(f"📝 [PAPER BUY] {symbol} @ {price} | Walls nearby: {walls}")

    def close_position(self, symbol, price, reason):
        if not self.position:
            return

        entry_price = self.position['entry_price']
        pnl_pct = (price - entry_price) / entry_price * 100
        
        if self.live:
            side = "SELL"
            self._place_order(symbol, side, self.position['qty'])
        
        emoji = "🟢" if pnl_pct > 0 else "🔴"
        log.info(f"{emoji} [TRADE CLOSED] {symbol} @ {price} | PnL: {pnl_pct:.2f}% | Reason: {reason}")
        
        self.pnl_history.append(pnl_pct)
        self.position = None

    def _place_order(self, symbol, side, qty):
        # Basic Binance API order placement logic (simplified)
        # In a real scenario, we'd need signature and timestamp handling
        log.warning(f"LIVE TRADING ORDER: {side} {qty} {symbol}")
        # Implementation similar to previous executors...
        return None

    def get_stats(self):
        if not self.pnl_history:
            return "No trades yet."
        win_rate = len([p for p in self.pnl_history if p > 0]) / len(self.pnl_history) * 100
        total_pnl = sum(self.pnl_history)
        return f"Trades: {len(self.pnl_history)} | Win Rate: {win_rate:.1f}% | Total PnL: {total_pnl:.2f}%"
