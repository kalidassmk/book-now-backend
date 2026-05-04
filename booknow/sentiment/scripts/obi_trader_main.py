#!/usr/bin/env python3
import asyncio
import argparse
import logging
import os
import sys

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("obi_trader")

async def main():
    parser = argparse.ArgumentParser(description="OBI Trader - Real-time Order Book Imbalance Bot")
    parser.add_argument("--symbol", default="BTCUSDT", help="Trading pair (default: BTCUSDT)")
    parser.add_argument("--amount", type=float, default=12.0, help="Trade amount in USDT")
    parser.add_argument("--live", action="store_true", help="Enable live trading")
    
    args = parser.parse_args()

    from obi_trader import OBITraderEngine
    
    engine = OBITraderEngine(
        symbol=args.symbol,
        live=args.live,
        trade_amount=args.amount
    )

    try:
        await engine.start()
    except KeyboardInterrupt:
        log.info("Shutting down...")
        engine.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
