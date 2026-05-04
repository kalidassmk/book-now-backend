import pandas as pd
import numpy as np

class FeatureEngineer:
    """
    Transforms raw features into model-ready inputs.
    """
    def transform(self, data):
        """
        Transforms raw features into model-ready inputs.
        Accepts either a single feature dictionary or a list of dictionaries.
        Returns a dictionary (if single input) or a DataFrame (if list input).
        """
        if isinstance(data, dict):
            # Flatten if it's a nested dict
            flat_data = self._flatten_dict(data)
            df = pd.DataFrame([flat_data])
            is_single = True
        else:
            # List of dicts - flatten each
            flat_list = [self._flatten_dict(d) for d in data]
            df = pd.DataFrame(flat_list)
            is_single = False
        
        # Drop non-numeric columns like 'symbol'
        if 'symbol' in df.columns:
            df = df.drop(columns=['symbol'])
        
        # 1. Handle Missing Values
        df = df.ffill().fillna(0)

        # 2. Interaction Features (Ensure columns exist first)
        for col in ['oi_change', 'funding_rate', 'rsi', 'price_change_5m']:
            if col not in df.columns: df[col] = 0.0

        df['oi_funding_interaction'] = df['oi_change'] * df['funding_rate']
        df['momentum_interaction'] = df['rsi'] * df['price_change_5m']

        # 3. Normalization (Simplified Z-score)
        cols_to_norm = ['rsi', 'volatility', 'volume_spike']
        for col in cols_to_norm:
            if col in df.columns:
                mean = df[col].mean()
                std = df[col].std()
                if std > 0:
                    df[col] = (df[col] - mean) / std
                else:
                    df[col] = 0.0
            else:
                df[col] = 0.0

        # 4. Enforce Strict Column Set & Order
        required_cols = [
            'price', 'rsi', 'price_change_5m', 'funding_rate', 
            'oi_change', 'volatility', 'volume_spike', 
            'oi_funding_interaction', 'momentum_interaction'
        ]
        
        # Ensure all required columns exist (fill with 0 if missing)
        for col in required_cols:
            if col not in df.columns:
                df[col] = 0.0
        
        # Reorder to match the required set exactly
        df = df[required_cols]

        if is_single:
            return df.iloc[0].to_dict()
        return df

        if is_single:
            return df.iloc[0].to_dict()
        return df

    def _flatten_dict(self, d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    @staticmethod
    def create_labels(df, tp=0.015, sl=-0.01):
        """
        Creates target labels y=1 (profit) or y=0 (loss) based on future price.
        Note: This requires 'future_price' to be present (only for training).
        """
        if 'future_max' not in df.columns:
            return df
            
        df['target'] = 0
        # Logic: if future max hits TP before future min hits SL
        # For simplicity in this demo:
        df.loc[df['future_pct_change'] > tp, 'target'] = 1
        return df
