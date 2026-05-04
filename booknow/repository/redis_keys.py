"""
redis_keys.py
─────────────────────────────────────────────────────────────────────────────
Authoritative list of every Redis key/hash/field name used across the
BookNow stack. Ported verbatim from the Java `Constant.java` so the
dashboard, the legacy sentiment engine, and the new Python engine all
agree on the schema during migration.

DO NOT rename a value here without auditing every reader. The Node
dashboard and Python sentiment engine both interpolate these names by
hand; a typo anywhere silently splits the world in two.
"""

from __future__ import annotations

# ── Core price tracking ──────────────────────────────────────────────────
CURRENT_PRICE  = "CURRENT_PRICE"
RW_BASE_PRICE  = "RW_BASE_PRICE"

# ── Percentage-gain buckets (base → current) ────────────────────────────
BUCKET_G0L1  = ">0<1"
BUCKET_G1L2  = ">1<2"
BUCKET_G2L3  = ">2<3"
BUCKET_G3L5  = ">3<5"
BUCKET_G5L7  = ">5<7"
BUCKET_G7L10 = ">7<10"
BUCKET_G10   = ">10"

# ── Watch-list keys ──────────────────────────────────────────────────────
WATCH_ALL    = "BASE_CURRENT_INC_%"
WATCH_PREFIX = "BS_TO_"      # + bucket + WATCH_SUFFIX
WATCH_SUFFIX = "_INC_%"

# ── Fast-move / momentum ─────────────────────────────────────────────────
FAST_MOVE      = "FAST_MOVE"
FAST_MOVE_TOP5 = "FM-5"

# ── Transition-time store keys (outer = group, inner = label) ────────────
ST0 = "ST0"   # transitions from base (0%)
ST1 = "ST1"   # transitions from 1%
ST2 = "ST2"   # transitions from 2%
ST3 = "ST3"   # transitions from 3%

# ── Speed labels ─────────────────────────────────────────────────────────
SUPER_FAST_2_3        = "SUPER_FAST>2<3"
ULTRA_FAST_3_5        = "ULTRA_FAST>3<5"
ULTRA_SUPER_FAST_5_7  = "ULTRA_SUPER_FAST>5<7"
LT2MIN_0_TO_3         = "LT2MIN_0>3"
ULTRA_FAST_0_TO_2     = "ULTRA_FAST0>2"
ULTRA_FAST_2_TO_3     = "ULTRA_FAST2>3"
ULTRA_FAST_0_TO_3     = "ULTRA_FAST0>3"

# ── Rule result keys ─────────────────────────────────────────────────────
RULE_1     = "R1"
RULE_1_HIT = "R1P3"
RULE_2     = "R2"
RULE_2_HIT = "R2P4"
RULE_3     = "R3"
RULE_3_HIT = "R3P4"

# ── Buy / Sell ────────────────────────────────────────────────────────────
BUY_KEY  = "BUY"
SELL_KEY = "SELL"   # Hash: symbol → SellRecord (JSON)

# ── Trading configuration ────────────────────────────────────────────────
TRADING_CONFIG = "TRADING_CONFIG"

# ── Account / wallet (written by user-data-stream) ──────────────────────
BALANCE_PREFIX = "BINANCE:BALANCE:"   # + asset
DUST_PREFIX    = "BINANCE:DUST:"      # + asset
SYMBOL_PREFIX  = "BINANCE:SYMBOL:"    # + symbol — exchangeInfo cache
DELIST_PREFIX  = "BINANCE:DELIST:"    # + symbol — true if delisted

# ── Sentiment-engine writers (consumed by dashboard + consensus) ────────
VOLUME_SCORE                = "VOLUME_SCORE"                # hash
SCALPER_SIGNAL_PREFIX       = "SCALPER:SIGNAL:"             # + symbol
SCALPER_POSITIONS           = "SCALPER:POSITIONS"
PROFIT_REACHED_020          = "PROFIT_REACHED_020"          # hash
ANALYSIS_020_TIMELINE       = "ANALYSIS_020_TIMELINE"       # hash
SYMBOLS_ACTIVE              = "SYMBOLS:ACTIVE"
SYMBOLS_OBI                 = "SYMBOLS:OBI"
SYMBOLS_BTC_FILTER          = "SYMBOLS:BTC_FILTER"

# ── Sentiment / consensus / dashboard scores ────────────────────────────
SENTIMENT_VOLUME_PREFIX     = "sentiment:market:volume:"    # + symbol (JSON value)
SENTIMENT_BEHAVIOURAL_PREFIX = "sentiment:market:adaptive:" # + symbol
DASHBOARD_SCORE             = "DASHBOARD_SCORE"             # hash
FINAL_CONSENSUS_STATE       = "FINAL_CONSENSUS_STATE"       # hash
TRADING_FEE_INTELLIGENCE    = "TRADING_FEE_INTELLIGENCE"    # JSON value

# ── Defaults (also held in TradingConfig in Redis; these are fallbacks) ─
BUY_AMOUNT_USDT_DEFAULT     = 12.0
SELL_PCT_RULE_1_FULL        = 5.0
SELL_PCT_RULE_1_FAST        = 3.5
SELL_PCT_RULE_2             = 7.0
SELL_PCT_RULE_3             = 9.0
