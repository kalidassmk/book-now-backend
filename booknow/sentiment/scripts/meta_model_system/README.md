# Meta-Model Prediction System

This system uses Machine Learning (XGBoost) to predict the probability of a profitable trade by synthesizing signals from multiple sub-systems.

## 🚀 How it Works

Instead of relying on rigid rules, the **Meta-Model** acts as a "consensus layer" that learns which combinations of features lead to successful trades.

### Integrated Features:
1.  **Momentum**: RSI, Price Velocity.
2.  **Derivatives**: Funding Rate, Open Interest Change.
3.  **Market Context**: Volatility (ATR), Volume Spikes.
4.  **Interactions**: e.g., (OI Change × Funding Rate) to detect aggressive positioning.

## 🧠 Model Pipeline

1.  **Data Collection**: Gathers raw metrics from Binance and internal engines.
2.  **Feature Engineering**: Normalizes data and creates non-linear interaction features.
3.  **Labeling**: Historical trades are labeled as Profit (1) or Loss (0) based on future price movement (TP/SL).
4.  **Training**: Uses **XGBoost** with a **Time-Series Split** to ensure no data leakage from the future.
5.  **Inference**: Real-time feature calculation followed by a probability output (0 to 1).

## 📊 Signal Logic

- **PROBABILITY > 0.7**: STRONG BUY (High Confidence)
- **PROBABILITY < 0.3**: STRONG SELL (High Confidence)
- **0.4 – 0.6**: NEUTRAL / NO TRADE

## 🏃 Setup & Run

1. **Install Dependencies**:
   ```bash
   pip install xgboost scikit-learn pandas numpy redis joblib aiohttp
   ```

2. **Initialize & Train (Bootstrap)**:
   The first run will automatically generate a synthetic dataset to train the initial model.
   ```bash
   cd binance-sentiment-engine/meta_model_system
   python main.py --bootstrap
   ```

3. **Monitor Predictions**:
   Results are published to the `META_MODEL_PREDICTIONS` Redis hash every 60 seconds.

## 🏗️ Project Structure

- `data_collector.py`: API integration and raw feature gathering.
- `feature_engineering.py`: Normalization and interaction logic.
- `model_trainer.py`: XGBoost training pipeline.
- `predictor.py`: Real-time inference engine.
- `main.py`: Orchestrator and loop logic.
