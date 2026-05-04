# Multi-Timeframe Trend Alignment Engine

This system reduces false signals by requiring consensus across multiple timeframes before generating a trading signal.

## 🚀 How it Works

The engine analyzes 6 different timeframes simultaneously and calculates a **Weighted Trend Agreement Score**.

### Timeframes & Weights:
- **5m, 15m**: Weight 1 (Entry/Momentum)
- **1h**: Weight 2 (Short-term trend)
- **4h**: Weight 3 (Medium-term trend)
- **1d**: Weight 4 (Long-term trend)
- **1w**: Weight 5 (Macro trend)

## 🧠 Core Logic

1.  **Trend Detection**: For each timeframe, it uses EMA 9/21 crossover, Price Structure (Higher Highs/Lower Lows), and momentum.
2.  **Weighted Consensus**: High-timeframe trends have more impact on the final score than low-timeframe trends.
3.  **Alignment Threshold**: A signal is only generated if at least **70%** of the weighted trend aligns.

## 📊 Signal Definitions

- **STRONG BUY**: ≥ 70% alignment in the bullish direction.
- **STRONG SELL**: ≥ 70% alignment in the bearish direction.
- **EARLY REVERSAL**: Occurs when low timeframes are bullish while high timeframes are bearish (or vice-versa).
- **NO TRADE**: When market is fragmented and alignment is below 70%.

## 🛠 Features

- **Parallel Processing**: Uses `asyncio` and `aiohttp` to fetch all 6 timeframes for all symbols concurrently.
- **Redis Integration**: Real-time signals are published to the `TREND_ALIGNMENT_SIGNALS` hash.
- **Performance**: Uses vectorized `pandas` operations for rapid trend calculation.

## 🏃 Setup & Run

1. **Install Dependencies**:
   ```bash
   pip install aiohttp pandas numpy redis
   ```

2. **Run the Scanner**:
   ```bash
   cd binance-sentiment-engine/trend_alignment_engine
   python main.py
   ```
