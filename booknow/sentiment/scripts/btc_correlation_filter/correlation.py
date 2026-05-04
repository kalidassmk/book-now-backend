import pandas as pd
import numpy as np

class CorrelationEngine:
    """
    Computes rolling correlation between altcoin and BTC returns.
    """
    @staticmethod
    def calculate(target_klines, btc_klines, window=20):
        if not target_klines or not btc_klines:
            return 0.0

        # Convert to DataFrames
        num_cols_t = len(target_klines[0])
        num_cols_b = len(btc_klines[0])
        
        target_df = pd.DataFrame(target_klines, columns=['t', 'o', 'h', 'l', 'c', 'v', 'ct', 'qv', 'tr', 'tbv', 'tbq', 'i'][:num_cols_t])
        btc_df = pd.DataFrame(btc_klines, columns=['t', 'o', 'h', 'l', 'c', 'v', 'ct', 'qv', 'tr', 'tbv', 'tbq', 'i'][:num_cols_b])

        # Align on timestamp 't'
        target_df['t'] = pd.to_numeric(target_df['t'])
        btc_df['t'] = pd.to_numeric(btc_df['t'])
        
        merged = pd.merge(
            target_df[['t', 'c']], 
            btc_df[['t', 'c']], 
            on='t', 
            suffixes=('_alt', '_btc')
        )
        
        merged[['c_alt', 'c_btc']] = merged[['c_alt', 'c_btc']].astype(float)

        # Calculate Returns
        merged['ret_alt'] = merged['c_alt'].pct_change()
        merged['ret_btc'] = merged['c_btc'].pct_change()

        # Rolling Correlation
        rolling_corr = merged['ret_alt'].rolling(window=window).corr(merged['ret_btc'])
        
        if rolling_corr.empty or len(rolling_corr) < 1:
            return 0.0
            
        current_corr = rolling_corr.iloc[-1]
        
        return 0.0 if np.isnan(current_corr) else round(current_corr, 4)
