# Fakeout & Liquidity Sweep Detector

This system identifies when the market traps traders by breaking a key support or resistance level and then immediately reversing.

## 🚀 Detection Logic

The system uses a 4-step validation process:

1.  **Breakout Detection**: Identifies when a candle closes outside a 20-period swing high/low.
2.  **Volume Validation**: Compares breakout volume with a 20-period moving average. Low volume breakouts are flagged as high-risk fakeouts.
3.  **Rejection Wick Analysis**: Detects long wicks (at least 2x the body size) that indicate aggressive supply or demand at the breakout point.
4.  **Follow-Through Check**: Monitors if the subsequent candle fails to hold the breakout and re-enters the prior range.

## 🧠 Trading Signals

- **STRONG SELL (Fake Breakout)**: Price breaks above resistance, shows volume weakness or rejection wicks, and closes back below resistance.
- **STRONG BUY (Liquidity Sweep)**: Price breaks below support, shows volume weakness or rejection wicks, and closes back above support.

## 🛠 Features

- **Real-time Monitoring**: Analyzes multiple symbols every 30-60 seconds.
- **Confidence Scoring**: Each signal is weighted by the presence of volume weakness and wick intensity.
- **Redis Integration**: Real-time signals are published to the `FAKEOUT_SIGNALS` hash for consumption by executors or dashboards.

## 🏃 Setup & Run

1. **Install Dependencies**:
   ```bash
   pip install aiohttp pandas numpy redis
   ```

2. **Run the Detector**:
   ```bash
   cd binance-sentiment-engine/fakeout_detector_system
   python main.py
   ```
