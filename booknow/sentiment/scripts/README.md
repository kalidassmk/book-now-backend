# 🛸 Binance Adaptive Market-Behavior Sentiment Engine

A production-grade algorithmic trading suite that combines 16 specialized signals (7 Java + 9 Python) into a unified master consensus.

👉 **[View the Detailed Algorithm & Logic Guide (ALGORITHMS_GUIDE.md)](./ALGORITHMS_GUIDE.md)**

## 🧠 Integrated Algorithms

| Algorithm | Folder/Service | Description |
| :--- | :--- | :--- |
| **1. Regime Trader** | `regime_trader/` | Detects Market State: Trending (Bull/Bear), Ranging, or Volatile. |
| **2. OBI Trader** | `obi_trader/` | Real-time Order Book Imbalance and Buy/Sell Wall detection. |
| **3. Funding/OI Trader**| `funding_oi_trader/` | Bullish/Bearish Divergence between Price and Derivatives data. |
| **4. Volume Profile** | `volume_profile_trader/`| Identifies High-Liquidity zones (POC, VAH, VAL) for support/resistance. |
| **5. MTF Alignment** | `trend_alignment_engine/`| Trend verification across 6 timeframes (5m, 15m, 1h, 4h, 1d, 1w). |
| **6. Fakeout Detector** | `fakeout_detector_system/`| Identifies Liquidity Sweeps and Traps using volume confirmation. |
| **7. Meta-Model (ML)** | `meta_model_system/` | XGBoost model predicting the probability of trade success (0 to 1). |
| **8. Risk Engine** | `risk_management_engine/` | Dynamic position sizing and drawdown protection. |
| **9. BTC Filter** | `btc_correlation_filter/` | Global safety switch based on BTC trend and correlation. |
| **10. Consensus Engine**| `./` | **Master Bridge**: Unifies 16 algorithms (Original + New). |

---

## 🏗️ Architecture Overview

The system follows a **Decoupled Consensus Architecture**:
1.  **Python Intelligence Layer**: 10 engines process raw exchange data and publish advanced metrics to **Redis**.
2.  **Java Execution Layer (Spring Boot)**: Receives the "Unified Consensus" and executes orders with sub-millisecond precision.
3.  **Dashboard (Node.js)**: Provides real-time visualization of the 16 algorithms and the current trade decisions.

---

## 🚀 How to Run the System

### 1. Requirements
Ensure **Redis** is running on `127.0.0.1:6379`.

### 2. Sync Live Symbols (Binance API)
Before starting the engines, fetch the latest top-volume coins from Binance:
```bash
cd binance-sentiment-engine

/Users/bogoai/Book-Now/venv313/bin/python3 sync_symbols.py
```

### 3. Start the Python Intelligence Stack
Launch all 10 specialized engines (Regime, OBI, Volume, Meta-Model, etc.):
```bash
cd /Users/bogoai/Book-Now/binance-sentiment-engine
/Users/bogoai/Book-Now/venv313/bin/python3 start_all.py
```

### 4. Start the Spring Boot Bot (Integrated)
Start the execution layer:
```bash
cd book-now-v3
mvn spring-boot:run
```

### 5. Start the Dashboard (Optional)
To visualize the 16-algorithm consensus:
```bash
cd dashboard
node server.js
```

### 6. Running Engines Individually
If you need to debug a specific algorithm, you can run it separately. You must `cd` into the algorithm's directory first:

| Algorithm | Command |
| :--- | :--- |
| **Regime Trader** | `cd /Users/bogoai/Book-Now/binance-sentiment-engine/regime_trader && /Users/bogoai/Book-Now/venv313/bin/python3 engine.py` |
| **OBI Trader** | `cd /Users/bogoai/Book-Now/binance-sentiment-engine/obi_trader && /Users/bogoai/Book-Now/venv313/bin/python3 engine.py` |
| **Funding/OI** | `cd /Users/bogoai/Book-Now/binance-sentiment-engine/funding_oi_trader && /Users/bogoai/Book-Now/venv313/bin/python3 main.py` |
| **Volume Profile** | `cd /Users/bogoai/Book-Now/binance-sentiment-engine/volume_profile_trader && /Users/bogoai/Book-Now/venv313/bin/python3 main.py` |
| **MTF Alignment** | `cd /Users/bogoai/Book-Now/binance-sentiment-engine/trend_alignment_engine && /Users/bogoai/Book-Now/venv313/bin/python3 main.py` |
| **Fakeout Detector** | `cd /Users/bogoai/Book-Now/binance-sentiment-engine/fakeout_detector_system && /Users/bogoai/Book-Now/venv313/bin/python3 main.py` |
| **Meta-Model** | `cd /Users/bogoai/Book-Now/binance-sentiment-engine/meta_model_system && /Users/bogoai/Book-Now/venv313/bin/python3 main.py` |
| **Risk Engine** | `cd /Users/bogoai/Book-Now/binance-sentiment-engine/risk_management_engine && /Users/bogoai/Book-Now/venv313/bin/python3 main.py` |
| **BTC Correlation** | `cd /Users/bogoai/Book-Now/binance-sentiment-engine/btc_correlation_filter && /Users/bogoai/Book-Now/venv313/bin/python3 main.py` |
| **Consensus Master**| `cd /Users/bogoai/Book-Now/binance-sentiment-engine && /Users/bogoai/Book-Now/venv313/bin/python3 consensus_engine.py` |

*Note: Use your specific python path if necessary (e.g., `/Users/bogoai/Book-Now/venv313/bin/python3`).*

### 7. Running Utility & Scanning Scripts
These scripts are used for broad market discovery and deep analysis. You can run them all at once using the Utility Master:

| Script | Command | Purpose |
| :--- | :--- | :--- |
| **Utility Master** | `cd /Users/bogoai/Book-Now/binance-sentiment-engine
 && /Users/bogoai/Book-Now/venv313/bin/python3 start_utilities.py` | **One-Stop Hub**: Runs Sync, Scanner, and Fast-Move Scan in a smart loop. |
| **Market Scanner** | `cd binance-sentiment-engine && ../venv313/bin/python3 market_sentiment_engine.py` | Scans **ALL USDT pairs** to find best setups. |
| **Deep Analyzer** | `cd binance-sentiment-engine && ../venv313/bin/python3 volume_price_analyzer.py --symbol BTC/USDT` | Deep-dive 12-timeframe analysis for one coin. |
| **Fast Move Scan** | `cd binance-sentiment-engine && ../venv313/bin/python3 volume_price_analyzer.py --scan` | Scans current trending coins in Redis. |
| **Pattern Recorder** | `cd binance-sentiment-engine && ../venv313/bin/python3 success_pattern_recorder.py` | **Success DNA**: Stores winning patterns in AnalyseDB. |
| **Pattern Matcher** | `cd binance-sentiment-engine && ../venv313/bin/python3 pattern_matching_engine.py` | **Smart Hunter**: Finds coins mimicking past success patterns. |
| **Profit Trend** | `cd binance-sentiment-engine && ../venv313/bin/python3 profit_020_trend_analyzer.py` | **Growth Tracker**: Analyzes volume/price trends for successful coins. |
| **Symbol Sync** | `cd binance-sentiment-engine && ../venv313/bin/python3 sync_symbols.py` | **Live Binance API Sync**: Fetches Top 200 coins and updates Redis. |

### 🛠️ Utility Master (`start_utilities.py`)
One script to rule them all. Manages:
- `sync_symbols.py`: Keeps your Top USDT list fresh.
- `market_sentiment_engine.py`: Continuous behavioral analysis.
- `volume_price_analyzer.py --scan`: Periodically finds momentum breakouts.
- `ultra_fast_scalper.py`: Rapid multi-symbol scalping.
- `profit_reached_analyzer.py`: High-frequency profit milestone tracking.
- `profit_020_trend_analyzer.py`: Tracks and visualizes post-success volume and price trends.
- `success_pattern_recorder.py`: Records successful trade "DNA" into AnalyseDB.
- `pattern_matching_engine.py`: Scans live market for historical success repeats.
- `fee_calculator_util.py`: (Startup Only) Calculates net profit targets after 0.1% fees.
- `virtual_scalp_executor.py`: Manages paper trades with 3m patience logic and net PnL reporting.

```bash
/Users/bogoai/Book-Now/venv313/bin/python3 start_utilities.py
```

---

## 📈 Monitoring Consensus
You can check the final combined decision in real-time via Redis:
```bash
# Get the unified score and decision for SOLUSDT
redis-cli hget FINAL_CONSENSUS_STATE SOLUSDT
```

## 🛠️ Configuration
- **Risk Limits**: Adjusted in `risk_management_engine/config.json`.
- **Dynamic Symbols**: The system is fully dynamic. Run `/Users/bogoai/Book-Now/venv313/bin/python3 sync_symbols.py` to automatically update the Top 200 coins across all engines from the Binance API.
- **Manual Symbols**: Edit `symbols_config.py` only if you want to override the dynamic discovery with a hardcoded list.
- **Success Radar**: Access the new `.20 Profit Analysis` dashboard at `http://localhost:3000/profit_analysis.html` or via the "Success Radar" link in the Pro Terminal sidebar.
