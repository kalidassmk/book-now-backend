try:
    import xgboost as xgb
    HAS_XGBOOST = True
except Exception:
    HAS_XGBOOST = False
import pickle
import os
import pandas as pd
import logging

log = logging.getLogger("meta_model.predictor")

class MetaPredictor:
    """
    Handles live inference for the Meta-Model.
    Loads the XGBoost model and produces 0.0 to 1.0 probability scores.
    """
    def __init__(self, model_path="models/meta_model.pkl"):
        self.model_path = model_path
        self.model = self._load_model()

    def _load_model(self):
        """Loads the pre-trained model."""
        if not os.path.exists(self.model_path):
            log.warning(f"⚠️  Model file not found at {self.model_path}. Predictor will return 0.5 default.")
            return None
        try:
            with open(self.model_path, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            log.error(f"Failed to load meta-model: {e}")
            return None

    def predict_probability(self, engineered_features: dict) -> float:
        """
        Takes a flat dictionary of engineered features and returns win probability.
        """
        if not HAS_XGBOOST or self.model is None:
            return 0.5 # Default neutral probability

        try:
            # Convert dict to DataFrame for XGBoost
            df = pd.DataFrame([engineered_features])
            
            # Predict probability of class 1 (Win)
            # predict_proba returns [prob_0, prob_1]
            probs = self.model.predict_proba(df)[0]
            win_prob = float(probs[1])
            
            return round(win_prob, 4)

        except Exception as e:
            log.error(f"Inference error: {e}")
            return 0.5
