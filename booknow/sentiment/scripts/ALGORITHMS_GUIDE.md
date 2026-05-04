# 📘 Complete Algorithm Documentation & Execution Guide

This document provides a detailed breakdown of all **16+ specialized algorithms** implemented in the Binance Adaptive Sentiment Engine. It covers how to run them, what they do, and their critical logic.

---

## 🐍 Python Intelligence Stack (10 Algorithms)

These engines process raw data and publish metrics to Redis.

### 1. Master Consensus Bridge
*   **Script**: `consensus_engine.py`
*   **Run Command**: `../venv313/bin/python3 consensus_engine.py`
*   **Purpose**: The "Grand Orchestrator". It pulls signals from all other 15 algorithms and calculates a final **Decision Score (-1 to +1)**.
*   **Critical Logic**: 
    *   Uses a **Weighted Voting System**.
    *   If `BTC_Filter` is Bearish, it overrides all Long signals to `NEUTRAL`.
    *   Applies the `Risk_Engine` multipliers before outputting the final state.

### 2. Regime Trader
*   **Script**: `regime_trader/engine.py`
*   **Run Command**: `cd regime_trader && ../../venv313/bin/python3 engine.py`
*   **Purpose**: Identifies the "Market State".
*   **Critical Logic**: Uses ADX (Trend Strength) and ATR (Volatility) to classify into:
    *   `TRENDING`: Prioritizes Momentum.
    *   `RANGING`: Prioritizes Mean Reversion.
    *   `VOLATILE`: Prioritizes Liquidity & Spreads.

### 3. OBI (Order Book Imbalance)
*   **Script**: `obi_trader/engine.py`
*   **Run Command**: `cd obi_trader && ../../venv313/bin/python3 engine.py`
*   **Purpose**: Detects massive Buy/Sell walls and volume pressure.
*   **Critical Logic**: Calculates the **Imbalance Ratio** between the top 20 levels of Bids vs Asks. 
    *   `Ratio > 0.7` = High Buying Pressure.
    *   `Ratio < 0.3` = High Selling Pressure.

### 4. Meta-Model (ML Engine)
*   **Script**: `meta_model_system/main.py`
*   **Run Command**: `cd meta_model_system && ../../venv313/bin/python3 main.py`
*   **Purpose**: Probability-based prediction using XGBoost.
*   **Critical Logic**: 
    *   Inputs: OBI, RSI, Volume, and Funding Rate.
    *   Output: A probability score (0.0 to 1.0) predicting if the price will hit **Target (+1.5%)** before **Stop Loss (-1.0%)**.

### 5. Funding & Open Interest (OI)
*   **Script**: `funding_oi_trader/main.py`
*   **Run Command**: `cd funding_oi_trader && ../../venv313/bin/python3 main.py`
*   **Purpose**: Detects Smart Money positioning.
*   **Critical Logic**: Looks for **Bullish Divergence**: Price is flat or dropping, but Open Interest is rising and Funding is negative (indicates shorts are trapped).

### 6. Volume Profile (POC)
*   **Script**: `volume_profile_trader/main.py`
*   **Run Command**: `cd volume_profile_trader && ../../venv313/bin/python3 main.py`
*   **Purpose**: Finds "Points of Control" (POC) where most volume occurred.
*   **Critical Logic**: Identifies if the current price is above or below the **Value Area**. 
    *   If Price > POC = Support confirmed.
    *   If Price < POC = Resistance confirmed.

### 7. Fakeout Detector
*   **Script**: `fakeout_detector_system/main.py`
*   **Run Command**: `cd fakeout_detector_system && ../../venv313/bin/python3 main.py`
*   **Purpose**: Filters out false breakouts.
*   **Critical Logic**: Checks if a price move is backed by **Volume Confirmation**. If Price breaks a level but Volume stays low, it flags a `FAKEOUT` warning.

---

## 🛠️ Utility & Scanning Scripts

### 8. Broad Market Scanner
*   **Script**: `market_sentiment_engine.py`
*   **Run Command**: `../venv313/bin/python3 market_sentiment_engine.py`
*   **Purpose**: Scans all 400+ USDT pairs to find coins with the highest consensus scores.
*   **Critical Logic**: Rapid iteration over Binance Spot API with a 30s cooldown between full market cycles.

### 9. Multi-Timeframe Analyzer
*   **Script**: `volume_price_analyzer.py`
*   **Run Command**: `../venv313/bin/python3 volume_price_analyzer.py --symbol SOL/USDT`
*   **Purpose**: Deep-dive analysis of 12 timeframes (5m to 1 Month).
*   **Critical Logic**: Uses a **Time-Weight Decay** model where 1h and 4h moves carry more weight than 5m noise.

### 10. Profit Trend Analyzer
*   **Script**: `profit_020_trend_analyzer.py`
*   **Run Command**: `../venv313/bin/python3 profit_020_trend_analyzer.py`
*   **Purpose**: Post-success performance tracking.
*   **Critical Logic**: 
    *   Calculates **Volume Bias**: Detects if volume growth is sustained after hitting the $0.20 profit target.
    *   Generates a **Price Trend History** stored in Redis for visualization on the "Success Radar" dashboard.

---

## ☕ Java Execution Stack (7 Algorithms)

Located in `book-now-v3`. These run automatically when you start the Spring Boot app.

| Algorithm | File | Logic |
| :--- | :--- | :--- |
| **1. FastMoveFilter** | `FastMoveFilter.java` | Blocks entries if price has moved >3% in the last 60 seconds (Anti-Slippage). |
| **2. TimeAnalyser** | `TimeAnalyser.java` | Restricts trading during low-liquidity hours (e.g., Sunday night). |
| **3. ULF (Liquidity Flow)** | `ULF0To3.java` | Tracks "Universal Liquidity Flow" between USDT and BTC. |
| **4. RuleOne** | `RuleOne.java` | **Base Entry Rule**: Price must be above the 200 EMA on the 15m chart. |
| **5. RuleTwo** | `RuleTwo.java` | **RSI Filter**: Prevents buying when RSI > 80 (Overbought). |
| **6. RuleThree** | `RuleThree.java` | **Volume Rule**: Volume must be 2x higher than the 24h average. |
| **7. TrailingStop** | `TrailingStopLossProcessor.java` | Dynamically moves Stop Loss to lock in profits as price rises. |

---

## 🔄 Interaction Map (How they call each other)

1.  **Exchange Data** → Individual Python Engines.
2.  **Python Engines** → Publish scores to **Redis** (e.g., `OBI_SCORE`, `REGIME_TYPE`).
3.  **Consensus Bridge** → Reads all Redis keys, aggregates them, and writes to `FINAL_CONSENSUS_STATE`.
4.  **Java Bot** → Listens to `FINAL_CONSENSUS_STATE` + Applies its own **7 Java Rules**.
5.  **Execution** → Order sent to Binance if **ALL** 16+ signals agree.
