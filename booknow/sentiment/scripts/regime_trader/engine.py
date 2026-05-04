"""
Regime Trading Engine — Main orchestrator.

Combines:
  1. Binance REST API for historical data seeding
  2. Binance WebSocket for real-time kline updates
  3. Indicator Engine → Regime Detector → Strategy Router → Executor
  4. Redis integration for dashboard visibility
"""

import json
import time
import asyncio
import logging
import ssl
import websockets
import redis
import ccxt
import os
import requests
import urllib3

def manual_load_dotenv(path):
    if not os.path.exists(path): return
    with open(path, 'r') as f:
        for line in f:
            if '=' in line and not line.startswith('#'):
                k, v = line.strip().split('=', 1)
                os.environ[k] = v.strip('"').strip("'")

# Load keys from the shared dashboard .env
dotenv_path = os.path.join(os.path.dirname(__file__), "..", "dashboard", ".env")
manual_load_dotenv(dotenv_path)

# Internal Imports
from indicators import IndicatorEngine, OHLCV
from regime_detector import RegimeDetector
from executor import BinanceExecutor
from strategies.base import Signal
from strategies.trend import TrendStrategy
from strategies.range import RangeStrategy
from strategies.volatility import VolatilityStrategy

# Disable SSL warnings for unverified requests
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("RegimeTrader")

BINANCE_REST = "https://api.binance.com"
BINANCE_WS = "wss://stream.binance.com:9443/ws"


class RegimeTraderEngine:
    """
    Main engine that:
      1. Seeds indicators from REST API history
      2. Connects to WebSocket for live kline updates
      3. Routes through: Indicators → Regime → Strategy → Executor
      4. Publishes state to Redis for dashboard integration
    """

    def __init__(
        self,
        symbol: str = "BTCUSDT",
        interval: str = "1m",
        indicator_period: int = 9,
        api_key: str = None,
        api_secret: str = None,
        live_trading: bool = False,
        trade_amount_usdt: float = 12.0,
        redis_host: str = "127.0.0.1",
        redis_port: int = 6379,
    ):
        self.symbol = symbol
        self.interval = interval

        # Components
        self.indicators = IndicatorEngine(period=indicator_period)
        self.regime_detector = RegimeDetector()
        # Initialize CCXT
        self.ccxt_client = ccxt.binance({
            'apiKey': api_key or os.getenv("BINANCE_API_KEY"),
            'secret': api_secret or os.getenv("BINANCE_SECRET_KEY"),
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })

        self.executor = BinanceExecutor(
            api_key=self.ccxt_client.apiKey,
            api_secret=self.ccxt_client.secret,
            live=live_trading,
            trade_amount_usdt=trade_amount_usdt,
        )

        # Strategies — one per regime
        self.strategies = {
            "TRENDING": TrendStrategy(),
            "RANGING": RangeStrategy(),
            "VOLATILE": VolatilityStrategy(),
        }
        self._active_strategy = None
        self._prev_regime = None

        # Redis Connection with descriptive error handling
        try:
            self._redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=True, socket_connect_timeout=5)
            self._redis.ping()
            self._redis_ok = True
            log.info("🔗 [SYSTEM] Successfully connected to Redis data bus at %s:%d", redis_host, redis_port)
        except redis.ConnectionError:
            log.error("❌ [CRITICAL] Redis is not reachable. Please ensure Redis is running on %s:%d", redis_host, redis_port)
            self._redis_ok = False
            self._redis = None
        except Exception as e:
            log.warning("⚠️  [SYSTEM] Unexpected Redis error: %s. Continuing in offline mode.", e)
            self._redis = None
            self._redis_ok = False

        # State
        self._running = False
        self._last_price = 0.0
        self._candle_count = 0

    # ── Public API ────────────────────────────────────────────────────

    def start(self):
        """Seed from history, then connect WebSocket for live updates."""
        log.info("═" * 60)
        log.info("  🚀 Regime Trader Engine")
        log.info("  Symbol:   %s", self.symbol)
        log.info("  Interval: %s", self.interval)
        log.info("  Mode:     %s", "🔴 LIVE" if self.executor.live else "📝 PAPER")
        log.info("═" * 60)

        # Step 1: Seed with historical data
        self._seed_history()

        # Step 2: Start WebSocket via asyncio
        self._running = True
        asyncio.run(self._ws_loop())

    def stop(self):
        """Gracefully shut down."""
        self._running = False
        log.info("Engine stopped. Final stats: %s", self.executor.get_stats())

    # ── History Seeding ───────────────────────────────────────────────

    def _seed_history(self):
        try:
            log.info("📡 Fetching historical candles for %s using CCXT...", self.symbol)

            # CCXT expects symbol with slash
            ccxt_symbol = self.symbol if "/" in self.symbol else f"{self.symbol[:-4]}/{self.symbol[-4:]}"
            
            klines = self.ccxt_client.fetch_ohlcv(ccxt_symbol, timeframe=self.interval, limit=200)

            for k in klines:
                candle = OHLCV(
                    timestamp=int(k[0]),
                    open=float(k[1]),
                    high=float(k[2]),
                    low=float(k[3]),
                    close=float(k[4]),
                    volume=float(k[5]),
                )
                self.indicators.update(candle)
                self._candle_count += 1

            state = self.indicators.state
            log.info("✅ Seeded %d candles. Indicators ready: %s", len(klines), state.ready)
            log.info("   ATR=%.6f ADX=%.2f +DI=%.2f -DI=%.2f RSI=%.1f",
                     state.atr, state.adx, state.plus_di, state.minus_di, state.rsi)

            # Initial regime classification
            regime = self.regime_detector.update(state)
            log.info("   Initial Regime: %s (confidence=%.1f%%)", regime.regime.value, regime.confidence)

        except requests.exceptions.ConnectionError:
            log.error("❌ [NETWORK ERROR] Unable to reach Binance API. Check your internet connection.")
        except requests.exceptions.HTTPError as e:
            log.error("❌ [API ERROR] Binance returned an error: %s", e)
        except Exception as e:
            log.error("❌ [UNKNOWN ERROR] An unexpected error occurred during history seeding: %s", e)
            log.info("💡 Suggestion: Check if the symbol '%s' is correct and supported by Binance.", self.symbol)

    # ── WebSocket (async) ─────────────────────────────────────────────

    async def _ws_loop(self):
        """Connect to Binance kline WebSocket stream with auto-reconnect."""
        stream = f"{self.symbol.lower()}@kline_{self.interval}"
        ws_url = f"{BINANCE_WS}/{stream}"

        while self._running:
            try:
                ssl_context = ssl._create_unverified_context()
                async with websockets.connect(ws_url, ping_interval=30, ping_timeout=10, ssl=ssl_context) as ws:
                    log.info("✅ WebSocket connected — streaming %s@kline_%s",
                             self.symbol, self.interval)

                    async for message in ws:
                        if not self._running:
                            break
                        self._handle_message(message)

            except websockets.exceptions.ConnectionClosed as e:
                log.warning("WebSocket closed: %s", e)
            except Exception as e:
                log.error("WebSocket error: %s", e)

            if self._running:
                log.info("🔄 Reconnecting WebSocket in 5s...")
                await asyncio.sleep(5)

    def _handle_message(self, message: str):
        """Process incoming kline WebSocket message."""
        try:
            data = json.loads(message)
            kline = data.get("k", {})

            if not kline:
                return

            is_closed = kline.get("x", False)
            current_price = float(kline.get("c", 0))
            self._last_price = current_price

            # Check stops on every tick
            trade = self.executor.check_stops(current_price)
            if trade:
                self._publish_trade(trade)

            # Only process completed candles for indicators/strategy
            if not is_closed:
                return

            candle = OHLCV(
                timestamp=int(kline.get("t", 0)),
                open=float(kline.get("o", 0)),
                high=float(kline.get("h", 0)),
                low=float(kline.get("l", 0)),
                close=float(kline.get("c", 0)),
                volume=float(kline.get("v", 0)),
            )

            self._candle_count += 1
            self._process_candle(candle)

        except Exception as e:
            log.error("Error processing WS message: %s", e)

    # ── Core Processing ───────────────────────────────────────────────

    def _process_candle(self, candle: OHLCV):
        """Full pipeline: Indicators → Regime → Strategy → Execution."""
        price = candle.close

        # 1. Update indicators
        ind_state = self.indicators.update(candle)

        if not ind_state.ready:
            return

        # 2. Detect regime
        regime_state = self.regime_detector.update(ind_state)
        current_regime = regime_state.regime

        # 3. Handle regime change
        if current_regime != self._prev_regime and self._prev_regime is not None:
            log.info("🔄 REGIME CHANGE: %s → %s",
                     self._prev_regime.value if self._prev_regime else "NONE",
                     current_regime.value)
            # Reset the new strategy's state
            strategy = self.strategies.get(current_regime.value)
            if strategy:
                strategy.reset()
            # Close position on regime change (safety)
            if self.executor.position:
                trade = self.executor.close_position(
                    price, reason=f"REGIME_CHANGE_{current_regime.value}")
                if trade:
                    self._publish_trade(trade)

        self._prev_regime = current_regime
        self._active_strategy = self.strategies.get(current_regime.value)

        # 4. Evaluate active strategy
        if self._active_strategy:
            signal = self._active_strategy.evaluate(price, ind_state, regime_state)

            if signal.signal == Signal.BUY and self.executor.position is None:
                pos = self.executor.open_position(
                    symbol=self.symbol,
                    price=price,
                    stop_loss=signal.stop_loss,
                    take_profit=signal.take_profit,
                    strategy=signal.strategy_name,
                    position_size_pct=signal.position_size_pct,
                )
                if pos:
                    log.info("📈 %s | %s (confidence=%.0f%%)",
                             signal.strategy_name, signal.reason, signal.confidence)

            elif signal.signal == Signal.SELL and self.executor.position is not None:
                trade = self.executor.close_position(price, reason=signal.reason)
                if trade:
                    self._publish_trade(trade)

        # 5. Log status
        self._log_status(price, ind_state, regime_state)

        # 6. Publish to Redis
        self._publish_state(price, ind_state, regime_state)

    # ── Logging ───────────────────────────────────────────────────────

    def _log_status(self, price: float, ind, regime):
        """Print compact status line."""
        pos_info = ""
        if self.executor.position:
            pos = self.executor.position
            pnl = ((price - pos.entry_price) / pos.entry_price) * 100
            pos_info = f" | 📊 PnL: {pnl:+.2f}%"

        strategy_name = self._active_strategy.name if self._active_strategy else "None"

        log.info(
            "🕐 [%s] Task: COMPLETE | Price: %.2f | Regime: %s | ADX=%.1f RSI=%.1f%s",
            self.symbol, price,
            regime.regime.value,
            regime.adx, ind.rsi, pos_info
        )

    def _publish_trade(self, trade):
        """Log and publish a completed trade."""
        emoji = "🟢" if trade.pnl_pct > 0 else "🔴"
        stats = self.executor.get_stats()
        log.info(
            "%s TRADE CLOSED: %s %.2f%% ($%.4f) | W/L: %d/%d (%.1f%%) | Total PnL: $%.4f | %s",
            emoji, trade.symbol, trade.pnl_pct, trade.pnl_usdt,
            stats["wins"], stats["losses"], stats["win_rate"],
            stats["total_pnl_usdt"], trade.reason
        )

    # ── Redis Integration ─────────────────────────────────────────────

    def _publish_state(self, price: float, ind, regime):
        """Publish current state to Redis for dashboard consumption."""
        if not self._redis_ok:
            return

        try:
            state = {
                "symbol": self.symbol,
                "price": price,
                "regime": regime.regime.value,
                "regime_confidence": regime.confidence,
                "trend_direction": regime.trend_direction,
                "adx": regime.adx,
                "atr": regime.atr,
                "atr_ratio": regime.atr_ratio,
                "plus_di": regime.plus_di,
                "minus_di": regime.minus_di,
                "rsi": round(ind.rsi, 1),
                "bb_upper": round(ind.bb_upper, 8),
                "bb_middle": round(ind.bb_middle, 8),
                "bb_lower": round(ind.bb_lower, 8),
                "sma_fast": round(ind.sma_fast, 8),
                "sma_slow": round(ind.sma_slow, 8),
                "active_strategy": self._active_strategy.name if self._active_strategy else "None",
                "position": None,
                "stats": self.executor.get_stats(),
                "candle_count": self._candle_count,
                "timestamp": time.time(),
            }

            if self.executor.position:
                pos = self.executor.position
                state["position"] = {
                    "symbol": pos.symbol,
                    "entry_price": pos.entry_price,
                    "stop_loss": pos.stop_loss,
                    "take_profit": pos.take_profit,
                    "pnl_pct": round(pos.pnl_pct, 2),
                    "strategy": pos.strategy,
                }

            # Store detailed state
            key = f"regime:trader:{self.symbol}"
            self._redis.set(key, json.dumps(state), ex=300)

            # Store regime in hash for quick lookup
            self._redis.hset("REGIME_STATE", self.symbol, json.dumps({
                "regime": regime.regime.value,
                "confidence": regime.confidence,
                "adx": regime.adx,
                "trend": regime.trend_direction,
                "strategy": self._active_strategy.name if self._active_strategy else "None",
                "price": price,
                "timestamp": time.time(),
            }))

        except Exception as e:
            log.error("Redis publish error: %s", e)

if __name__ == "__main__":
    import os
    import sys
    import threading
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from symbols_config import ACTIVE_SYMBOLS

    INTERVAL = os.getenv("INTERVAL", "5m")
    LIVE = os.getenv("LIVE_TRADING", "false").lower() == "true"

    # Allow override via env var (single symbol) or run all ACTIVE_SYMBOLS
    env_symbol = os.getenv("SYMBOL")
    symbols_to_run = [env_symbol] if env_symbol else ACTIVE_SYMBOLS

    log.info(f"🚀 Regime Trader starting for {len(symbols_to_run)} symbols")

    threads = []
    for sym in symbols_to_run:
        def _run(symbol=sym):
            try:
                engine = RegimeTraderEngine(
                    symbol=symbol,
                    interval=INTERVAL,
                    live_trading=LIVE,
                )
                engine.start()
            except Exception as exc:
                log.critical("🔥 [CRITICAL] Regime Engine crashed for %s: %s", symbol, exc)

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        threads.append(t)

    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        log.info("👋 Shutdown requested via KeyboardInterrupt.")
