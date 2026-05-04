# 🏛️ Technical Specification: Binance Sentiment Engine Algorithms

This document outlines the **16 algorithms** developed for the Binance Sentiment Engine, focusing on their purpose, implementation details, and interaction logic.

## 1. Consensus Engine (`consensus_engine.py`)
- **Purpose**: The "Supreme Court" that unifies all signals.
- **Implementation**: 
  - **Logic**: Weighted aggregation of signals from Python and Java layers.
  - **Threshold**: Decision = BUY if Weighted Score >= 65 and no Vetoes.
  - **Inputs**: Meta-Model (30%), Trend Alignment (25%), OBI (25%), Sentiment (10%), Dashboard (10%).

## 2. Meta-Model ML Engine (`meta_model_system/`)
- **Purpose**: Probability-based prediction using Machine Learning.
- **Implementation**: 
  - **Model**: XGBoost Classifier.
  - **Target**: Probability of price hitting +1.5% TP before -1.0% SL.
  - **Features**: Real-time Order Book Imbalance, RSI, Funding Rates, and Volume Growth.

## 3. OBI (Order Book Imbalance) (`obi_trader/`)
- **Purpose**: Detects buy/sell wall pressure.
- **Implementation**: 
  - **Metric**: `(Sum(Bids[0:20]) - Sum(Asks[0:20])) / TotalVolume`.
  - **Signals**: High Buy Pressure (>0.7), High Sell Pressure (<0.3).

## 4. Regime Trader (`regime_trader/`)
- **Purpose**: Determines the "Market State".
- **Implementation**: 
  - **Tools**: ADX (Trend Strength) and ATR (Volatility).
  - **States**: Trending, Ranging, or Volatile.

## 5. Ultra-Fast Scalper (`ultra_fast_scalper.py`)
- **Purpose**: Captures micro-momentum in high-volume pairs.
- **Implementation**: 
  - **Strategy**: Micro-breakouts on 1m charts.
  - **Filters**: EMA 9/21/50 stack, RSI > 60, Volume > 1.5x average.

## 6. Funding & Open Interest (OI) (`funding_oi_trader/`)
- **Purpose**: Identifies liquidations and squeezes.
- **Implementation**: 
  - **Signal**: Price divergence from OI. Rising OI + Negative Funding + Flat Price = Long Squeeze Potential.

## 7. Volume Profile POC (`volume_profile_trader/`)
- **Purpose**: Finds the most liquid price levels.
- **Implementation**: 
  - **Logic**: Horizontal volume distribution (POC - Point of Control).

## 8. Fakeout Detector (`fakeout_detector_system/`)
- **Purpose**: Filters false breakouts.
- **Implementation**: 
  - **Logic**: Requires Relative Volume (RVOL) > 1.2x on price breakout.

## 9. BTC Correlation Filter (`btc_correlation_filter/`)
- **Purpose**: Market health kill-switch.
- **Implementation**: 
  - **Logic**: Vetoes trades if BTC/USDT is bearish or extremely volatile.

## 10. Symbol Discovery Engine (`symbol_discovery_engine.py`)
- **Purpose**: Dynamic symbol selection.
- **Implementation**: 
  - **Logic**: Scans 400+ pairs for volume spikes and news-driven moves.

## 11. Volume Price Analyzer (`volume_price_analyzer.py`)
- **Purpose**: Multi-timeframe trend analysis.
- **Implementation**: 
  - **Logic**: Time-weighted decay model across 12 timeframes.

## 12. Profit 020 Trend Analyzer (`profit_020_trend_analyzer.py`)
- **Purpose**: Performance tracking for the 0.20 USDT scalp strategy.
- **Implementation**: 
  - **Logic**: Monitors post-exit price action to optimize targets.

## 13. Trend Alignment Engine (`trend_alignment_engine/`)
- **Purpose**: Technical indicator convergence.
- **Implementation**: 
  - **Logic**: Scores the alignment of EMA, MACD, and RSI across timeframes.

## 14. Risk Management Engine (`risk_management_engine/`)
- **Purpose**: Capital preservation.
- **Implementation**: 
  - **Logic**: Dynamic position sizing and global drawdown limits.

## 15. Success Pattern Recorder (`success_pattern_recorder.py`)
- **Purpose**: Data gathering for ML retraining.
- **Implementation**: 
  - **Logic**: Snapshots indicator states on every profitable trade.

## 16. Market Sentiment Engine (`market_sentiment_engine.py`)
- **Purpose**: Macro sentiment overview.
- **Implementation**: 
  - **Logic**: Aggregates Fear & Greed and Binance social sentiment.
