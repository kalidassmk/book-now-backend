import asyncio
import json
import logging
import time
import redis
import redis
import ccxt
import websockets
import ssl

# Internal Imports
from book_manager import OrderBookManager
from metrics import ImbalanceCalculator
from executor import ExecutionManager
from strategy import StrategyManager, SignalGenerator

# Configure Logging with a more descriptive format
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("OBITrader")

class OBITraderEngine:
    def __init__(self, symbol="BTCUSDT", live=False, trade_amount=100.0, redis_host="127.0.0.1"):
        self.symbol = symbol.upper()
        self.book = OrderBookManager(self.symbol)
        self.metrics_calc = ImbalanceCalculator()
        self.signal_gen = SignalGenerator()
        self.executor = ExecutionManager(live=live, trade_amount_usdt=trade_amount)
        self.strategy = StrategyManager(self.executor)
        
        # Initialize CCXT
        self.ccxt_client = ccxt.binance({
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })
        
        # Redis Connection with descriptive error handling
        try:
            self._redis = redis.Redis(host=redis_host, port=6379, decode_responses=True, socket_connect_timeout=5)
            self._redis.ping()
            log.info("🔗 [SYSTEM] OBI Trader connected to Redis at %s", redis_host)
        except redis.ConnectionError:
            log.error("❌ [CRITICAL] OBI Trader cannot find Redis. Signals will not be broadcast.")
        except Exception as e:
            log.error("⚠️  [SYSTEM] Unexpected OBI Redis error: %s", e)
            
        self._running = False

    async def start(self):
        self._running = True
        log.info(f"🚀 Starting OBI Trader for {self.symbol}")
        
        # Start WebSocket and Snapshot tasks
        await asyncio.gather(
            self._ws_loop(),
            self._snapshot_loop()
        )

    async def _snapshot_loop(self):
        """Fetches initial order book snapshot using CCXT."""
        try:
            # CCXT expects symbol with slash
            ccxt_symbol = self.symbol if "/" in self.symbol else f"{self.symbol[:-4]}/{self.symbol[-4:]}"
            log.info(f"📡 Fetching depth snapshot for {self.symbol} using CCXT...")
            
            snapshot = await self.ccxt_client.fetch_order_book(ccxt_symbol, limit=1000)
            
            # Convert CCXT format to the format handle_snapshot expects
            # CCXT: {'bids': [[price, qty], ...], 'asks': ...}
            # Binance REST: {'bids': [['price', 'qty'], ...], 'asks': ...}
            formatted_snapshot = {
                "lastUpdateId": snapshot.get("nonce", 0),
                "bids": [[str(p), str(q)] for p, q in snapshot['bids']],
                "asks": [[str(p), str(q)] for p, q in snapshot['asks']]
            }
            self.book.handle_snapshot(formatted_snapshot)
            log.info(f"✅ Depth snapshot loaded for {self.symbol}")
        except Exception as e:
            log.error(f"❌ [SNAPSHOT ERROR] Failed to fetch depth for {self.symbol}: {e}")

    async def _ws_loop(self):
        uri = f"wss://stream.binance.com:9443/ws/{self.symbol.lower()}@depth"
        while self._running:
            try:
                ssl_context = ssl._create_unverified_context()
                async with websockets.connect(uri, ssl=ssl_context) as websocket:
                    log.info(f"Connected to Binance WebSocket for {self.symbol}")
                    async for message in websocket:
                        data = json.loads(message)
                        self.book.handle_update(data)
                        
                        # After update, run logic
                        await self._tick()
            except websockets.exceptions.InvalidURI:
                log.error(f"❌ [CONFIG ERROR] Invalid WebSocket URI for {self.symbol}. Check symbol format.")
                break
            except websockets.exceptions.ConnectionClosed:
                log.warning(f"⚠️  [NETWORK] WebSocket connection closed for {self.symbol}. Reconnecting...")
            except Exception as e:
                log.error(f"❌ [WS ERROR] Unexpected error in OBI stream: {e}")
                await asyncio.sleep(5)

    async def _tick(self):
        bids, asks = self.book.get_top_levels()
        if not bids or not asks:
            return

        # 1. Calculate Metrics
        m = self.metrics_calc.calculate(bids, asks)
        if not m:
            return

        mid_price = self.book.get_mid_price()

        # 2. Generate Signal
        signal = self.signal_gen.generate(m)

        # 3. Strategy Decision
        self.strategy.on_signal(self.symbol, signal, mid_price, m)

        # 4. Logging & Dashboard
        self._publish_state(mid_price, m, signal)

    def _publish_state(self, price, m, signal):
        state = {
            "symbol": self.symbol,
            "price": price,
            "pressure": m['pressure'],
            "weighted_imbalance": m['weighted_imbalance'],
            "ratio": m['ratio'],
            "bid_walls": len(m['bid_walls']),
            "ask_walls": len(m['ask_walls']),
            "signal": signal,
            "in_position": self.strategy.in_position,
            "stats": self.executor.get_stats(),
            "timestamp": time.time()
        }
        # Publish to Redis for Dashboard
        self._redis.hset("OBI_STATE", self.symbol, json.dumps(state))
        
        if int(time.time()) % 10 == 0: # Log every 10 seconds
            log.info(f"🕐 [{self.symbol}] Task: COMPLETE | Price: {price:.2f} | Pressure: {m['pressure']:.2f} | Imb: {m['weighted_imbalance']:.2f} | Sig: {signal}")

    def stop(self):
        self._running = False

if __name__ == "__main__":
    import os
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from symbols_config import OBI_SYMBOLS

    LIVE = os.getenv("LIVE_TRADING", "false").lower() == "true"

    # Allow override via env var (single symbol) or run all OBI_SYMBOLS
    env_symbol = os.getenv("SYMBOL")
    symbols_to_run = [env_symbol] if env_symbol else OBI_SYMBOLS

    log.info(f"🚀 OBI Trader starting for {len(symbols_to_run)} symbols: {symbols_to_run}")

    async def run_all():
        engines = [OBITraderEngine(symbol=sym, live=LIVE) for sym in symbols_to_run]
        await asyncio.gather(*[engine.start() for engine in engines])

    try:
        asyncio.run(run_all())
    except KeyboardInterrupt:
        log.info("👋 Shutdown requested via KeyboardInterrupt.")
    except Exception as e:
        log.critical("🔥 [CRITICAL] OBI Engine crashed during startup: %s", e)
