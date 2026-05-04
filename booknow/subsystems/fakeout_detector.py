"""
fakeout_detector.py
─────────────────────────────────────────────────────────────────────────────
Phase-13 port of ``fakeout_detector_system/data_fetcher.py``.

Fakeout detection pulls 5m candles, ~200 deep, so it can compute
swing highs/lows with enough history.
"""

from booknow.subsystems.base_fetcher import KlinesFetcher


class FakeoutDetectorFetcher(KlinesFetcher):
    default_interval = "5m"
    default_limit = 200

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("log_name", "booknow.subsystems.fakeout")
        super().__init__(*args, **kwargs)
