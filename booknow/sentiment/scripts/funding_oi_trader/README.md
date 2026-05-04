# Funding Rate & Open Interest Divergence Bot

This system detects trading signals on Binance Futures by analyzing the relationship between price action, Open Interest (OI), and Funding Rates.

## 🚀 How it Works

The bot monitors three key metrics to identify market sentiment:

1.  **Price Trend**: Uses EMA 9 and EMA 21 crossover to determine the short-term trend.
2.  **Open Interest (OI)**: Tracks whether new positions are being opened (OI rising) or closed (OI falling).
3.  **Funding Rate**: Identifies overcrowded trades (extreme positive/negative funding) that are prone to reversals or liquidations.

## 🛠 Features

- **Real-time Monitoring**: Runs every 60 seconds with async `aiohttp` calls.
- **Divergence Analysis**: Detects "Fake Pimps" (Price Up, OI Down) and "Short Build-ups" (Price Down, OI Up).
- **Reversal Detection**: Identifies potential short squeezes when funding is extremely negative.
- **Confidence Scoring**: Each signal comes with a weighted confidence score (0.0 to 1.0).
- **Redis Integration**: Results are stored in Redis (`FUNDING_OI_SIGNALS` hash) for easy dashboard integration.

## 📦 Requirements

- Python 3.10+
- Redis (running on 127.0.0.1:6379)
- Packages: `aiohttp`, `pandas`, `numpy`, `redis`

## 🏃 Run the Project

1. **Navigate to the directory**:
   ```bash
   cd binance-sentiment-engine/funding_oi_trader
   ```

2. **Install dependencies** (if not already in venv):
   ```bash
   pip install aiohttp pandas numpy redis
   ```

3. **Start the bot**:
   ```bash
   python main.py
   ```

## 📊 Signal Definitions

- **STRONG BUY**: Price rising + OI rising (New longs entering).
- **STRONG SELL**: Price falling + OI rising (New shorts entering).
- **WEAK BUY**: Price rising + OI falling (Shorts covering, no new demand).
- **SHORT SQUEEZE**: Extreme negative funding + Price stability/rise.
