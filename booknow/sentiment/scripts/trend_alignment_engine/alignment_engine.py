import logging

log = logging.getLogger("trend_alignment.engine")

class AlignmentEngine:
    """
    Calculates alignment score across multi-timeframe trends.
    """
    WEIGHTS = {
        "5m": 1,
        "15m": 1,
        "1h": 2,
        "4h": 3,
        "1d": 4,
        "1w": 5
    }

    def __init__(self, alignment_threshold=70.0):
        self.alignment_threshold = alignment_threshold
        self.max_possible_score = sum(self.WEIGHTS.values())

    def calculate(self, trends):
        """
        trends: dict of {interval: trend_score}
        """
        weighted_score = 0
        trend_summary = {}

        for tf, score in trends.items():
            weight = self.WEIGHTS.get(tf, 0)
            weighted_score += score * weight
            trend_summary[tf] = "bullish" if score == 1 else "bearish" if score == -1 else "neutral"

        alignment_percentage = (abs(weighted_score) / self.max_possible_score) * 100
        
        return {
            "weighted_score": weighted_score,
            "alignment_percentage": round(alignment_percentage, 2),
            "trends": trend_summary,
            "is_aligned": alignment_percentage >= self.alignment_threshold
        }
