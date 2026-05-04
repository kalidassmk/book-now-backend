import logging
import json
import bisect

log = logging.getLogger("obi_trader.book")

class OrderBookManager:
    """
    Maintains a local copy of the Binance Order Book for a symbol.
    Supports incremental updates and snapshots.
    """
    def __init__(self, symbol, depth=20):
        self.symbol = symbol.upper()
        self.depth_limit = depth
        self.bids = {} # price: volume
        self.asks = {} # price: volume
        self.last_update_id = 0
        self._is_initialized = False
        self._update_buffer = []

    def handle_snapshot(self, snapshot):
        """Initializes the book with a REST snapshot."""
        self.last_update_id = snapshot['lastUpdateId']
        self.bids = {float(price): float(qty) for price, qty in snapshot['bids']}
        self.asks = {float(price): float(qty) for price, qty in snapshot['asks']}
        self._is_initialized = True
        log.info(f"OrderBook for {self.symbol} initialized with snapshot ID {self.last_update_id}")
        
        # Process buffered updates
        for update in self._update_buffer:
            self.handle_update(update)
        self._update_buffer = []

    def handle_update(self, update):
        """Processes an incremental update (depthUpdate)."""
        if not self._is_initialized:
            self._update_buffer.append(update)
            return

        final_id = update['u']
        first_id = update['U']

        if final_id <= self.last_update_id:
            return # Old update

        # Update bid/ask prices
        for price, qty in update['b']:
            p, q = float(price), float(qty)
            if q == 0:
                self.bids.pop(p, None)
            else:
                self.bids[p] = q

        for price, qty in update['a']:
            p, q = float(price), float(qty)
            if q == 0:
                self.asks.pop(p, None)
            else:
                self.asks[p] = q

        self.last_update_id = final_id

    def get_top_levels(self):
        """Returns sorted top N bids and asks."""
        sorted_bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:self.depth_limit]
        sorted_asks = sorted(self.asks.items(), key=lambda x: x[0])[:self.depth_limit]
        return sorted_bids, sorted_asks

    def get_mid_price(self):
        if not self.bids or not self.asks:
            return 0.0
        best_bid = max(self.bids.keys())
        best_ask = min(self.asks.keys())
        return (best_bid + best_ask) / 2.0
