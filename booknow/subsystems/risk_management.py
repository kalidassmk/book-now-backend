"""
risk_management.py
─────────────────────────────────────────────────────────────────────────────
Phase-13 port of ``risk_management_engine/data_fetcher.py``.

Risk-management analysis pulls 15m candles, ~100 deep. Defaults
match the legacy fetcher exactly so the strategy code (indicators,
risk_engine, portfolio_manager) keeps working with no diff.
"""

from booknow.subsystems.base_fetcher import KlinesFetcher


class RiskManagementFetcher(KlinesFetcher):
    default_interval = "15m"
    default_limit = 100

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("log_name", "booknow.subsystems.risk")
        super().__init__(*args, **kwargs)
