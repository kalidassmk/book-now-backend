import asyncio
import aiohttp
import logging
import json
import redis
import pandas as pd
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from symbols_config import ACTIVE_SYMBOLS
from data_fetcher import DataFetcher
from indicators import RiskIndicators
from portfolio_manager import PortfolioManager
from risk_engine import RiskEngine

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
log = logging.getLogger("risk.main")

class RiskManagementBot:
    def __init__(self, symbols=None, interval_sec=60):
        if symbols is None:
            symbols = ACTIVE_SYMBOLS
        self.symbols = symbols
        self.interval_sec = interval_sec
        self.redis_client = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
        self.portfolio = PortfolioManager(self.redis_client)
        self.engine = RiskEngine()

    async def run(self):
        log.info("🛡️  [INITIALIZING] Risk Management Engine (Resilient CCXT Mode)...")
        fetcher = DataFetcher()
        try:
            while True:
                portfolio_state = self.portfolio.get_state()
                log.info(f"Equity: {portfolio_state['equity']} | Drawdown: {portfolio_state['drawdown']:.2%}")
                
                tasks = [self.process_symbol(fetcher, symbol, portfolio_state) for symbol in self.symbols]
                await asyncio.gather(*tasks)
                
                log.info(f"Risk cycle complete. Waiting {self.interval_sec}s...")
                await asyncio.sleep(self.interval_sec)
        finally:
            await fetcher.close()

    async def process_symbol(self, fetcher, symbol, portfolio_state):
        try:
            # 1. Fetch Data
            klines = await fetcher.fetch_klines(symbol)
            if not klines: return

            df = pd.DataFrame(klines, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
            df[['high','low','close']] = df[['high','low','close']].astype(float)

            # 2. Calculate Indicators (ATR)
            atr = RiskIndicators.calculate_atr(df)
            current_price = df['close'].iloc[-1]

            # 3. Calculate Risk Parameters for a hypothetical BUY trade
            risk_params = self.engine.calculate_trade_params(
                symbol=symbol,
                entry_price=current_price,
                side="BUY",
                atr=atr,
                portfolio_state=portfolio_state
            )

            # 4. Store in Redis
            self.redis_client.hset("RISK_ENGINE_PARAMS", symbol, json.dumps(risk_params))
            
            if risk_params["trade_allowed"]:
                log.info(f"✅ [{symbol}] Size: {risk_params['position_size']} | SL: {risk_params['stop_loss']} | TP: {risk_params['take_profit']}")
            else:
                log.warning(f"❌ [{symbol}] Trade Rejected: {risk_params['reason']}")

        except Exception as e:
            log.error(f"Error processing risk for {symbol}: {e}", exc_info=True)

if __name__ == "__main__":
    bot = RiskManagementBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        log.info("Risk engine stopped.")
