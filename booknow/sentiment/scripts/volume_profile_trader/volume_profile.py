import numpy as np
import pandas as pd
import logging

log = logging.getLogger("volume_profile.engine")

class VolumeProfileEngine:
    """
    Core logic to build Volume Profile, POC, VAH, and VAL.
    """
    def __init__(self, num_bins=100):
        self.num_bins = num_bins

    def calculate(self, klines):
        """
        klines: list of kline lists
        """
        if not klines:
            return None

        # Prepare DataFrame
        num_cols = len(klines[0])
        if num_cols >= 12:
            cols = ['time', 'open', 'high', 'low', 'close', 'volume', 
                    'close_time', 'quote_vol', 'trades', 't_buy_vol', 't_buy_q_vol', 'ignore']
        else:
            cols = ['time', 'open', 'high', 'low', 'close', 'volume']
            
        df = pd.DataFrame(klines, columns=cols[:num_cols])
        
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)

        min_price = df['low'].min()
        max_price = df['high'].max()
        
        if min_price == max_price:
            return None

        # Define bins
        bin_size = (max_price - min_price) / self.num_bins
        bins = np.linspace(min_price, max_price, self.num_bins + 1)
        volume_profile = np.zeros(self.num_bins)

        # Distribute volume (Method B: Even distribution across High-Low range)
        for _, row in df.iterrows():
            h, l, v = row['high'], row['low'], row['volume']
            
            # Find bins covered by the candle range
            start_bin = int((l - min_price) / bin_size)
            end_bin = int((h - min_price) / bin_size)
            
            # Clamp bins to range
            start_bin = max(0, min(start_bin, self.num_bins - 1))
            end_bin = max(0, min(end_bin, self.num_bins - 1))
            
            num_covered_bins = (end_bin - start_bin) + 1
            vol_per_bin = v / num_covered_bins
            
            volume_profile[start_bin : end_bin + 1] += vol_per_bin

        # 1. Point of Control (POC)
        poc_idx = np.argmax(volume_profile)
        poc_price = bins[poc_idx] + (bin_size / 2)

        # 2. Value Area (70% Rule)
        total_volume = np.sum(volume_profile)
        target_volume = total_volume * 0.70
        
        # Sort bins by volume descending
        sorted_indices = np.argsort(volume_profile)[::-1]
        
        accumulated_vol = 0
        va_indices = []
        for idx in sorted_indices:
            accumulated_vol += volume_profile[idx]
            va_indices.append(idx)
            if accumulated_vol >= target_volume:
                break
        
        # Value Area High and Low
        va_prices = [bins[idx] for idx in va_indices]
        vah = max(va_prices)
        val = min(va_prices)

        return {
            "min_price": min_price,
            "max_price": max_price,
            "bins": bins.tolist(),
            "profile": volume_profile.tolist(),
            "poc": round(poc_price, 4),
            "vah": round(vah, 4),
            "val": round(val, 4),
            "current_price": df['close'].iloc[-1],
            "total_volume": total_volume
        }
