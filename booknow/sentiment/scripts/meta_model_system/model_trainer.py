try:
    import xgboost as xgb
    HAS_XGBOOST = True
except Exception:
    HAS_XGBOOST = False
import pandas as pd
import numpy as np
import pickle
import os
import logging
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import accuracy_score, precision_score, roc_auc_score

log = logging.getLogger("meta_model.trainer")

class MetaModelTrainer:
    """
    Trains an XGBoost Classifier to predict trade probability.
    Uses TimeSeriesSplit to respect chronological data order.
    """
    def __init__(self, model_path="models/meta_model.pkl"):
        self.model_path = model_path
        os.makedirs("models", exist_ok=True)
        
        self.params = {
            'objective': 'binary:logistic',
            'max_depth': 4,
            'learning_rate': 0.05,
            'n_estimators': 100,
            'eval_metric': 'logloss',
            'tree_method': 'hist', # Faster training
            'random_state': 42
        }

    def train(self, df):
        """Trains the model on the provided DataFrame."""
        if not HAS_XGBOOST:
            log.error("Cannot train: xgboost module not found.")
            return False
            
        if df.empty or len(df) < 50:
            log.warning("Insufficient data for training. Need at least 50 samples.")
            return False

        # Prepare X and y
        X = df.drop(columns=['target'])
        y = df['target']

        # Ensure only numeric columns are passed to XGBoost
        X = X.select_dtypes(include=[np.number])

        # TimeSeries Cross-Validation
        tscv = TimeSeriesSplit(n_splits=3)
        scores = []

        log.info("Starting TimeSeries Cross-Validation...")
        for train_idx, val_idx in tscv.split(X):
            X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
            y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]

            model = xgb.XGBClassifier(**self.params)
            model.fit(X_train, y_train)
            
            y_pred = model.predict(X_val)
            acc = accuracy_score(y_val, y_pred)
            scores.append(acc)
            log.info(f"Fold Accuracy: {acc:.4f}")

        # Final training on all data
        log.info("Training final model on full dataset...")
        final_model = xgb.XGBClassifier(**self.params)
        final_model.fit(X, y)

        # Save model
        with open(self.model_path, 'wb') as f:
            pickle.dump(final_model, f)
        
        log.info(f"✅ Model saved to {self.model_path} | Avg CV Acc: {np.mean(scores):.4f}")
        return True

    def load_model(self):
        """Loads the trained model from disk."""
        if not os.path.exists(self.model_path):
            return None
        with open(self.model_path, 'rb') as f:
            return pickle.dump(f)
