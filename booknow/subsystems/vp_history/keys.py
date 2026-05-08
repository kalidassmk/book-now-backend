"""Redis Cloud key namespace for vp_history."""

VP_HISTORY_PREFIX = "vp_history:"          # ZSET per symbol; member=JSON, score=ts_ms
VP_BASE_VOL_KEY   = "vp_base_vol"          # HASH symbol → first-seen volume float
VP_STOPPED_KEY    = "vp_stopped"           # SET of symbols whose price fell below base


def history_key(symbol: str) -> str:
    return f"{VP_HISTORY_PREFIX}{symbol.upper()}"
