import logging

log = logging.getLogger("obi_trader.metrics")

class ImbalanceCalculator:
    """
    Computes imbalance metrics: Bid/Ask Ratio, Pressure, Weighted Imbalance, and Walls.
    """
    def __init__(self, wall_threshold_multiplier=5.0):
        self.wall_threshold_multiplier = wall_threshold_multiplier

    def calculate(self, bids, asks):
        """
        bids, asks: List of (price, qty) tuples, sorted.
        """
        if not bids or not asks:
            return None

        total_bid_vol = sum(q for p, q in bids)
        total_ask_vol = sum(q for p, q in asks)

        # 1. Bid/Ask Ratio
        ratio = total_bid_vol / total_ask_vol if total_ask_vol > 0 else 0.0

        # 2. Order Book Pressure
        pressure = (total_bid_vol - total_ask_vol) / (total_bid_vol + total_ask_vol) if (total_bid_vol + total_ask_vol) > 0 else 0.0

        # 3. Weighted Imbalance (closer to mid-price = higher weight)
        weighted_bid_vol = sum(q / (i + 1) for i, (p, q) in enumerate(bids))
        weighted_ask_vol = sum(q / (i + 1) for i, (p, q) in enumerate(asks))
        weighted_imbalance = (weighted_bid_vol - weighted_ask_vol) / (weighted_bid_vol + weighted_ask_vol) if (weighted_bid_vol + weighted_ask_vol) > 0 else 0.0

        # 4. Liquidity Walls detection
        avg_bid_size = total_bid_vol / len(bids)
        avg_ask_size = total_ask_vol / len(asks)

        bid_walls = [p for p, q in bids if q > avg_bid_size * self.wall_threshold_multiplier]
        ask_walls = [p for p, q in asks if q > avg_ask_size * self.wall_threshold_multiplier]

        return {
            "ratio": round(ratio, 4),
            "pressure": round(pressure, 4),
            "weighted_imbalance": round(weighted_imbalance, 4),
            "bid_walls": bid_walls,
            "ask_walls": ask_walls,
            "total_bid_vol": round(total_bid_vol, 2),
            "total_ask_vol": round(total_ask_vol, 2)
        }
