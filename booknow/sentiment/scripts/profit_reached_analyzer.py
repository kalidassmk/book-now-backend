import os
import redis
import json
import time
import logging
from datetime import datetime, timezone

# ==========================================
# 1. LOGGING & CONFIG
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ProfitReachedAnalyzer")

# ── Redis Keys ──────────────────────────────────────────────────────────
WATCH_ALL          = "BASE_CURRENT_INC_%"             # input: per-coin base/current price
PROFIT_REACHED_KEY = "PROFIT_REACHED_020"             # legacy: currently-above set (still maintained for downstream)
EVENTS_KEY         = "PROFIT_REACHED_020_EVENTS"      # NEW: append-only edge-triggered crossings
STATE_KEY          = "PROFIT_020_STATE"               # NEW: durable per-symbol "above" marker for edge detection

# ── Tunables ────────────────────────────────────────────────────────────
# Aligned with operator's $30 small-scalp config (was $100 / $0.20).
# PROFIT_THRESHOLD scaled proportionally: $0.06 on $30 = same 0.2 % move
# the analyzer used to flag, just sized for the smaller trade.
EVENTS_CAP        = 20_000
BUY_AMOUNT_USDT   = 30.0
PROFIT_THRESHOLD  = 0.06
LOOP_SLEEP_SEC    = 2


class ProfitReachedAnalyzer:
    """Detects when a coin's price (relative to its base price) yields the
    configured USDT profit threshold.

    Edge-triggered: every (below → above) transition emits one immutable
    record into ``PROFIT_REACHED_020_EVENTS`` (a list, capped). The legacy
    ``PROFIT_REACHED_020`` hash is kept as a "currently-above-or-recently-was"
    set so existing downstream consumers (trend analyzer) keep working.
    """

    def __init__(self):
        self.r = redis.Redis(
            host=os.getenv("REDIS_HOST", "127.0.0.1"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            db=0, decode_responses=True,
        )

    def run(self):
        logger.info(
            f"🚀 Profit Reached Analyzer started — investment=${BUY_AMOUNT_USDT}  "
            f"threshold=${PROFIT_THRESHOLD}  events_cap={EVENTS_CAP}"
        )
        while True:
            try:
                # Snapshot edge-detection state once per loop (1 redis call) instead
                # of per-symbol (N+1 calls). For ~500 symbols this saves real load.
                state = self.r.hgetall(STATE_KEY) or {}

                for symbol, data_json in self.r.hscan_iter(WATCH_ALL):
                    try:
                        data = json.loads(data_json)
                        base_price = float(data.get('basePrice', 0))
                        curr_price = float(data.get('currentPrice', 0))

                        if base_price <= 0:
                            continue

                        profit = (curr_price - base_price) * (BUY_AMOUNT_USDT / base_price)
                        is_above  = profit >= PROFIT_THRESHOLD
                        was_above = state.get(symbol) == "above"

                        if is_above and not was_above:
                            # Rising edge — emit a new crossing event.
                            self._emit_rising_edge(symbol, base_price, curr_price, profit)
                            state[symbol] = "above"
                        elif (not is_above) and was_above:
                            # Falling edge — clear the edge marker only. Leave
                            # PROFIT_REACHED_020 alone so the trend analyzer's
                            # 4-hour inactivity pruning handles cleanup.
                            self._clear_edge(symbol)
                            state.pop(symbol, None)
                        elif is_above:
                            # Still above (no edge) — refresh the legacy
                            # "currently above" record so the snapshot stays fresh.
                            self._refresh_current_above(symbol, base_price, curr_price, profit)

                    except Exception as e:
                        logger.error(f"Error processing {symbol}: {e}")

                time.sleep(LOOP_SLEEP_SEC)

            except Exception as e:
                logger.error(f"Loop error: {e}")
                time.sleep(LOOP_SLEEP_SEC * 2)

    def _emit_rising_edge(self, symbol, base_price, curr_price, profit):
        """A coin just crossed the threshold from below — append an
        immutable event and refresh the universe set."""
        ts = time.time()
        event_id = f"{symbol}-{int(ts * 1000)}"
        event = {
            "event_id":         event_id,
            "symbol":           symbol,
            "basePrice":        base_price,
            "crossingPrice":    curr_price,
            "currentPrice":     curr_price,        # alias kept for legacy consumers
            "profit":           round(profit, 4),
            "investmentUsdt":   BUY_AMOUNT_USDT,
            "thresholdUsdt":    PROFIT_THRESHOLD,
            "reachedAt":        datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
            "reachedAtTs":      ts,
            "hms":              datetime.fromtimestamp(ts).strftime("%H:%M:%S"),
        }
        try:
            with self.r.pipeline(transaction=False) as pipe:
                pipe.lpush(EVENTS_KEY, json.dumps(event))
                pipe.ltrim(EVENTS_KEY, 0, EVENTS_CAP - 1)
                pipe.hset(PROFIT_REACHED_KEY, symbol, json.dumps(event))
                pipe.hset(STATE_KEY, symbol, "above")
                pipe.execute()
            logger.info(
                f"🎯 [CROSSING] {symbol} | +${profit:.4f} | base={base_price} → curr={curr_price} | event={event_id}"
            )
        except Exception as e:
            logger.error(f"Failed to emit crossing event for {symbol}: {e}")

    def _clear_edge(self, symbol):
        """Coin dropped back below threshold. Clear its edge marker so the
        next rising edge will fire a fresh event."""
        try:
            self.r.hdel(STATE_KEY, symbol)
        except Exception as e:
            logger.warning(f"Failed to clear edge state for {symbol}: {e}")

    def _refresh_current_above(self, symbol, base_price, curr_price, profit):
        """No edge transition; refresh the legacy "currently above" snapshot."""
        record = {
            "symbol":      symbol,
            "basePrice":   base_price,
            "currentPrice": curr_price,
            "profit":      round(profit, 4),
            "reachedAt":   datetime.now(tz=timezone.utc).isoformat(),
        }
        try:
            self.r.hset(PROFIT_REACHED_KEY, symbol, json.dumps(record))
        except Exception:
            pass


if __name__ == "__main__":
    ProfitReachedAnalyzer().run()
