# Volume Profile (POC/VAH/VAL) Trading System

This system implements a price-based volume distribution analysis (Volume Profile) to identify high-liquidity zones and generate trading signals.

## 🚀 Key Concepts

- **Volume Profile**: Aggregates traded volume at specific price levels rather than over time.
- **POC (Point of Control)**: The price level with the highest traded volume in the period.
- **Value Area (70%)**: The price range where 70% of the total volume was traded.
  - **VAH (Value Area High)**: The upper boundary of the Value Area.
  - **VAL (Value Area Low)**: The lower boundary of the Value Area.

## 🧠 Trading Signals

- **BUY**: Price near **VAL** (Potential Support).
- **SELL**: Price near **VAH** (Potential Resistance).
- **STRONG BUY**: Price breaks and holds above **VAH** with high volume.
- **STRONG SELL**: Price breaks and holds below **VAL** with high volume.
- **MEAN REVERSION**: Price extended far from **POC** often acts as a magnet.

## 🛠 Features

- **Dynamic Bining**: Automatically adjusts price buckets based on market range.
- **Weighted Confidence**: Signals are scored based on proximity and volume density.
- **Visual Analytics**: Generates real-time histograms for each symbol in the `plots/` directory.
- **Redis Integration**: Publishes signals to the `VOLUME_PROFILE_SIGNALS` hash.

## 🏃 Setup & Run

1. **Install Dependencies**:
   ```bash
   pip install aiohttp pandas numpy matplotlib redis
   ```

2. **Run the Bot**:
   ```bash
   cd binance-sentiment-engine/volume_profile_trader
   python main.py
   ```

3. **Check Plots**:
   Visual profiles are saved as PNG files in the `plots/` directory (e.g., `BTCUSDT_profile.png`).
