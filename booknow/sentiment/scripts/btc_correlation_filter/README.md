# BTC Correlation + Trend Filter Engine

This system acts as a "Global Market Safety Switch." It prevents altcoin trading when Bitcoin conditions are unfavorable or when high correlation poses a risk.

## 🚀 How it Works

The engine runs a real-time analysis of the relationship between **Bitcoin (BTC)** and your target altcoins.

### 1. Synchronized Data Fetching
Uses `asyncio.gather` to fetch klines for the Altcoin and BTCUSDT simultaneously, ensuring perfectly aligned timestamps for correlation calculation.

### 2. Rolling Correlation (20-period)
Calculates the statistical correlation between BTC returns and Altcoin returns. 
- **High Correlation (>0.6)**: Altcoin is heavily dependent on BTC movement.
- **Low Correlation (<0.3)**: Altcoin is "decoupled" and moving on its own strength.

### 3. BTC Strength Scoring
Combines EMA trend (9/21), momentum, and volatility into a unified **BTC Strength Score** (-1.0 to +1.0).

## 🧠 Filtering Rules

- **BLOCK LONG** if: BTC is Bearish AND Correlation is High.
- **BLOCK LONG** if: BTC Strength Score is critically low (<-0.3).
- **ALLOW LONG** if: Correlation is Low (even if BTC is weak).
- **ALLOW LONG** if: BTC is Bullish and Strength is positive.

## 🏗️ Architecture

- `data_fetcher.py`: Parallel async API requests.
- `correlation.py`: Statistical return correlation.
- `btc_filter.py`: Macro market bias detection.
- `strategy_filter.py`: Core rule engine.
- `main.py`: Real-time orchestrator.

## 🏃 Setup & Run

1. **Install Dependencies**:
   ```bash
   pip install aiohttp pandas numpy redis
   ```

2. **Run the Filter**:
   ```bash
   cd binance-sentiment-engine/btc_correlation_filter
   python main.py
   ```

The results are published to the `BTC_CORRELATION_FILTERS` Redis hash, where they can be consumed by the execution engines to "Green-light" or "Block" trades.
