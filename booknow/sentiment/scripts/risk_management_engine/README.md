# Production-Grade Risk Management Engine

This engine ensures capital preservation and controlled risk by calculating precise position sizes and exit levels for every trade.

## 🚀 Key Risk Modules

### 1. Volatility-Based Sizing (ATR)
Instead of using a fixed amount of crypto, the engine calculates position size based on the **Average True Range (ATR)**. 
- Higher Volatility → Wider Stop Loss → Smaller Position Size.
- Lower Volatility → Tighter Stop Loss → Larger Position Size.
- **Result**: The dollar amount at risk remains constant regardless of market conditions.

### 2. ATR-Based Stop-Loss
Exit levels are dynamically set using a multiplier (default 2.0x) of the ATR. This ensures stops are placed outside the "market noise" while still protecting capital.

### 3. Drawdown Protection (Equity Guard)
The system tracks the "Peak Equity" and automatically adjusts behavior based on current drawdown:
- **Drawdown > 10%**: Risk per trade is automatically reduced by 50%.
- **Drawdown > 20%**: A "Trading Halt" is triggered, rejecting all new trades until manual intervention.

### 4. Trade Validation Layer
Every trade request passes through a validation gate that checks:
- Current account equity.
- Number of active positions (Max 3).
- Risk-Reward ratio (Min 1:2).

## 🛠 Features

- **Real-Time Integration**: Calculates risk parameters every 60 seconds for active watchlists.
- **Redis State Management**: Stores equity curve, peak equity, and drawdown history in `RISK_PORTFOLIO_STATE`.
- **Async Execution**: Parallel calculation for multiple symbols.

## 🏃 Setup & Run

1. **Install Dependencies**:
   ```bash
   pip install aiohttp pandas numpy redis
   ```

2. **Run the Engine**:
   ```bash
   cd binance-sentiment-engine/risk_management_engine
   python main.py
   ```
