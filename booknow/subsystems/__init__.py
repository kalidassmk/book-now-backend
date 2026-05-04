"""
booknow.subsystems
─────────────────────────────────────────────────────────────────────────────
Phase-13 home for the analytical subsystems the legacy
``binance-sentiment-engine`` shipped as standalone packages
(risk_management_engine, fakeout_detector_system, volume_profile_trader,
trend_alignment_engine, meta_model_system).

Phase 13 ports their **data-fetcher** layer only. The strategy / scoring
modules keep their own files for now and take a fetcher in their
constructor — the legacy CCXT dependency is gone.

Public surface:

    from booknow.subsystems import SubsystemRegistry

    registry = SubsystemRegistry(rest_client=rest)
    await registry.start()

    klines = await registry.risk_management.fetch_klines("BTCUSDT")
    multi  = await registry.trend_alignment.fetch_multi_timeframe("BTCUSDT")
    feats  = await registry.meta_model.fetch_all_features("BTCUSDT")

    await registry.stop()
"""

from booknow.subsystems.registry import SubsystemRegistry

__all__ = ["SubsystemRegistry"]
