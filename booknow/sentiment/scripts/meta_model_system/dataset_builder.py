import pandas as pd
import numpy as np
import time
import logging
from data_collector import DataCollector
from feature_engineering import FeatureEngineer

log = logging.getLogger("meta_model.dataset")

class DatasetBuilder:
    """
    Builds a training dataset by collecting historical features and labeling 
    them based on future price movement (TP=1.5%, SL=1.0%).
    """
    def __init__(self, collector: DataCollector, engineer: FeatureEngineer):
        self.collector = collector
        self.engineer = engineer

    async def build_historical_dataset(self, symbol, klines):
        """
        Processes historical klines to create a labeled DataFrame.
        Note: In a production scenario, this would pull historical Redis snapshots.
        For training, we simulate the feature state from kline history.
        """
        log.info(f"🔨 Building dataset for {symbol} using {len(klines)} candles...")
        
        records = []
        
        # We need a look-ahead window, so we stop before the end
        # Labeling rule: 1 if +1.5% hit before -1.0%
        TP_PCT = 0.015
        SL_PCT = 0.010
        
        for i in range(100, len(klines) - 50): # Start after 100 for indicators, end 50 early for labeling
            current_kline = klines[i]
            entry_price = float(current_kline[4]) # Close price
            
            # 1. Extract Features (Simulated for training)
            # In production, this would be collector.collect_all(symbol)
            features = self._simulate_features_at_index(klines, i)
            engineered = self.engineer.transform(features)
            
            # 2. Look Ahead for Label
            label = self._calculate_label(klines, i + 1, entry_price, TP_PCT, SL_PCT)
            
            if label is not None:
                engineered['target'] = label
                records.append(engineered)

        df = pd.DataFrame(records)
        log.info(f"✅ Generated {len(df)} labeled samples for {symbol}")
        return df

    def _calculate_label(self, klines, start_idx, entry_price, tp, sl):
        """Looks ahead to see which target is hit first."""
        tp_price = entry_price * (1 + tp)
        sl_price = entry_price * (1 - sl)
        
        for j in range(start_idx, len(klines)):
            high = float(klines[j][2])
            low = float(klines[j][3])
            
            if high >= tp_price: return 1 # Win
            if low <= sl_price: return 0  # Loss
            
        return None # Not hit within window

    def _simulate_features_at_index(self, klines, idx):
        """
        Aligns simulation with DataCollector.fetch_all_features output.
        """
        price = float(klines[idx][4])
        prev_price = float(klines[idx-1][4]) if idx > 0 else price
        
        return {
            "symbol": "BTCUSDT",
            "price": price,
            "rsi": 50 + np.random.normal(0, 10),
            "price_change_5m": (price - prev_price) / prev_price,
            "funding_rate": 0.0001 + np.random.normal(0, 0.00005),
            "oi_change": np.random.normal(0, 0.02),
            "volatility": np.random.normal(0.01, 0.005),
            "volume_spike": 1.0 + np.random.normal(0, 0.5)
        }
