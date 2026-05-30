"""
success_pattern_recorder.py
─────────────────────────────────────────────────────────────────────────────
For every edge-triggered crossing event published by
``profit_reached_analyzer`` (rising edge of +$0.20 USDT vs. base price),
this recorder captures a rich pattern record and stores it in cloud Redis.

The point of capturing this is *prediction*: in the future, when a new
coin shows a similar pre-cross trajectory, we want to be able to score it
against historical winners.

Schema versions
---------------
V2 (primary, written here):
    A trajectory-shape-aware record stored under ``PATTERNS:V2`` (HASH,
    keyed by event_id). Each record carries:

      - the crossing snapshot (base, crossing price, profit, timestamps),
      - the ``trajectory`` — every ANALYSIS_020_TIMELINE snapshot in the
        last TRAJECTORY_WINDOW_SEC seconds *before* the cross,
      - ``market_dna_at_cross`` — consensus / regime / volume / BTC,
      - ``context_at_cross`` — derived stats (scalp signal lead-time,
        max drawdown in window, overheated flag),
      - legacy multi-timeframe ``time_series_dna`` (1s/1m/1h klines via
        the kline router, zero-REST when WS cache is warm),
      - ``performance_after_1h`` — filled by the followup loop.

V1 / Legacy (still written for backward compat):
    A subset of the V2 fields written to ``ANALYSE_DB`` so the existing
    ``pattern_matching_engine.py`` keeps producing PATTERN_MATCH_SIGNALS
    until it can be upgraded to read V2 directly.

Cursor-based consumption
------------------------
Events are read from the local ``PROFIT_REACHED_020_EVENTS`` list using a
durable cursor stored in cloud Redis (``PATTERNS:V2:CURSOR``). On
restart we resume from where we left off; on extended downtime, evicted
events are tolerated by an idempotency check (``HEXISTS PATTERNS:V2``).
"""

import os
import redis
import json
import time
import logging
import ccxt
from datetime import datetime, timezone

# Multi-source kline fetcher (Binance WS cache → REST rotation → CryptoCompare).
from klines_router import get_default_router as _get_klines_router

# ──────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-7s | %(message)s')
log = logging.getLogger("PatternRecorder")

# ──────────────────────────────────────────────────────────────────────────
# Redis configuration
# ──────────────────────────────────────────────────────────────────────────
LOCAL_REDIS = {
    'host': os.getenv("REDIS_HOST", "127.0.0.1"),
    'port': int(os.getenv("REDIS_PORT", "6379")),
    'db': 0,
    'decode_responses': True,
}
REMOTE_REDIS = {
    'host': os.getenv("REDIS_ANALYSE_HOST", "redis-analyse"),
    'port': int(os.getenv("REDIS_ANALYSE_PORT", "6379")),
    'password': os.getenv("REDIS_ANALYSE_PASS") or None,
    'decode_responses': True,
}

# ──────────────────────────────────────────────────────────────────────────
# Source keys (local Redis, written by analyzers)
# ──────────────────────────────────────────────────────────────────────────
EVENTS_KEY            = "PROFIT_REACHED_020_EVENTS"   # edge-triggered crossings (input)
ANALYSIS_TIMELINE_KEY = "ANALYSIS_020_TIMELINE"       # rolling per-tick snapshots
CONSENSUS_KEY         = "FINAL_CONSENSUS_STATE"
REGIME_KEY            = "REGIME_STATE"
VOLUME_KEY            = "VOLUME_SCORE"
BTC_KEY               = "BTC_CORRELATION_FILTERS"

# ──────────────────────────────────────────────────────────────────────────
# Destination keys (cloud Redis — AnalyseDB)
# ──────────────────────────────────────────────────────────────────────────
# V2 — trajectory-aware schema
PATTERNS_V2_KEY          = "PATTERNS:V2"                  # HASH event_id -> JSON
PATTERNS_V2_RECENT_KEY   = "PATTERNS:V2:RECENT"           # LIST event_ids newest-first (cap)
PATTERNS_V2_SYM_PREFIX   = "PATTERNS:V2:SYM"              # per-symbol LIST
PATTERNS_V2_FOLLOWUP_KEY = "PATTERNS:V2:FOLLOWUP_PENDING" # HASH event_id -> {ts, price}
PATTERNS_V2_CURSOR_KEY   = "PATTERNS:V2:CURSOR"           # STRING last processed event_id

# Legacy V1 — kept until pattern_matching_engine.py is upgraded
ANALYSE_DB_KEY           = "ANALYSE_DB"

# ──────────────────────────────────────────────────────────────────────────
# Tunables
# ──────────────────────────────────────────────────────────────────────────
TRAJECTORY_WINDOW_SEC = 600        # 10 min of pre-cross snapshots
RECENT_LIST_CAP       = 1000       # global "recent crossings" feed
PER_SYMBOL_LIST_CAP   = 100        # per-symbol crossing history
LOOP_SLEEP_SEC        = 10
FOLLOWUP_DELAY_SEC    = 3600       # 1 hour
EVENTS_FETCH_BATCH    = 1000       # newest-N events scanned per loop
EVENT_DEFER_SEC       = 30         # wait this long after a crossing before
                                    # capturing — gives profit_020_trend_analyzer
                                    # time to populate ANALYSIS_020_TIMELINE
                                    # via fetch_historical_context (10m of
                                    # pre-cross 1m klines).

# Multi-timeframe kline samples (for the legacy ANALYSE_DB record).
INTERVALS_SEC  = [1, 2, 5, 10, 20, 30, 45, 50]
INTERVALS_MIN  = [1, 2, 3, 5, 10, 15, 20, 30, 45]
INTERVALS_HOUR = [1, 2, 3, 5]


class SuccessPatternRecorder:
    def __init__(self):
        self.r_source  = redis.Redis(**LOCAL_REDIS)
        self.r_analyse = redis.Redis(**REMOTE_REDIS)
        # CCXT kept only for the rare bare-spot fallback in followups; klines
        # go through the WS-cached router.
        self.ccxt = ccxt.binance({
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'},
        })
        self.klines = _get_klines_router()

    # ── Main loop ─────────────────────────────────────────────────────────

    def run(self):
        log.info(
            "🧠 Pattern Recorder online — V2 schema | trajectory_window=%ds | "
            "events_batch=%d | followup_delay=%ds",
            TRAJECTORY_WINDOW_SEC, EVENTS_FETCH_BATCH, FOLLOWUP_DELAY_SEC,
        )
        while True:
            try:
                self._process_new_events()
                self._process_followups()
                time.sleep(LOOP_SLEEP_SEC)
            except Exception as e:
                log.error(f"Loop error: {e}")
                time.sleep(max(LOOP_SLEEP_SEC // 2, 1))

    # ── Event processing ──────────────────────────────────────────────────

    def _process_new_events(self):
        events = self._fetch_new_events()
        if not events:
            return

        # Defer capture for very recent events. ANALYSIS_020_TIMELINE is
        # populated by profit_020_trend_analyzer *after* a coin enters
        # PROFIT_REACHED_020, so the timeline is empty at the moment of
        # crossing. Waiting EVENT_DEFER_SEC lets the trend analyzer seed
        # the timeline (via fetch_historical_context — 10m of 1m klines)
        # before we snapshot it. Newer events stay in the events list and
        # get picked up on a subsequent loop.
        cutoff_ts = time.time() - EVENT_DEFER_SEC
        ready    = [e for e in events if float(e.get("reachedAtTs", 0) or 0) <= cutoff_ts]
        deferred = len(events) - len(ready)
        if not ready:
            if deferred:
                log.info("⏳ [EVENTS] %d crossing(s) too recent — deferring to next loop", deferred)
            return

        if deferred:
            log.info("📥 [EVENTS] %d ready, %d still deferred (<%ds old)",
                     len(ready), deferred, EVENT_DEFER_SEC)
        else:
            log.info("📥 [EVENTS] %d new crossing(s) to record", len(ready))

        recorded_any = False
        for event in ready:
            if self._capture_pattern(event):
                recorded_any = True

        # Advance the cursor to the newest READY event. Deferred events
        # stay newer-than-cursor so the next _fetch_new_events sees them.
        if recorded_any:
            try:
                self.r_analyse.set(PATTERNS_V2_CURSOR_KEY, ready[-1]["event_id"])
            except Exception as e:
                log.warning(f"Failed to update V2 cursor: {e}")

    def _fetch_new_events(self):
        """Read events from local EVENTS_KEY (LPUSH'd newest-first) and
        return the ones added since the last cursor, in oldest-newest order."""
        try:
            cursor = self.r_analyse.get(PATTERNS_V2_CURSOR_KEY)
        except Exception:
            cursor = None
        try:
            raw_list = self.r_source.lrange(EVENTS_KEY, 0, EVENTS_FETCH_BATCH - 1)
        except Exception as e:
            log.error(f"Failed to read events list: {e}")
            return []

        new_events = []
        for raw in raw_list:
            try:
                ev = json.loads(raw)
            except Exception:
                continue
            if cursor and ev.get("event_id") == cursor:
                break
            new_events.append(ev)

        return list(reversed(new_events))  # oldest-newest

    def _capture_pattern(self, event):
        event_id = event.get("event_id")
        symbol   = event.get("symbol")
        if not event_id or not symbol:
            return False

        # Idempotency: don't re-record if this event_id is already in V2.
        try:
            if self.r_analyse.hexists(PATTERNS_V2_KEY, event_id):
                return True
        except Exception:
            pass  # If we can't even check, fall through and try to write — write will surface failure.

        try:
            crossed_at_ts = float(event.get("reachedAtTs", time.time()))
            trajectory = self._capture_trajectory(symbol, crossed_at_ts)
            market_dna = self._capture_market_dna(symbol)
            context    = self._compute_context(trajectory)
            time_series_dna = self._capture_legacy_time_series(symbol)

            base_price     = event.get("basePrice")
            crossing_price = event.get("crossingPrice")
            gain_pct = (
                round(((crossing_price / base_price) - 1) * 100, 4)
                if base_price and crossing_price else 0.0
            )

            record = {
                "schema_version": 2,
                "event_id":       event_id,
                "symbol":         symbol,
                "crossed_at":     event.get("reachedAt"),
                "crossed_at_ts":  crossed_at_ts,

                "crossing": {
                    "base_price":      base_price,
                    "crossing_price":  crossing_price,
                    "profit_usdt":     event.get("profit"),
                    "investment_usdt": event.get("investmentUsdt"),
                    "threshold_usdt":  event.get("thresholdUsdt"),
                    "price_gain_pct":  gain_pct,
                },

                "trajectory":              trajectory,
                "trajectory_seconds_covered": (
                    trajectory[-1]["t_offset_sec"] - trajectory[0]["t_offset_sec"]
                    if len(trajectory) >= 2 else 0
                ),
                "trajectory_points": len(trajectory),

                "market_dna_at_cross": market_dna,
                "context_at_cross":    context,
                "time_series_dna":     time_series_dna,
                "performance_after_1h": None,
            }

            self._write_v2(record)
            self._write_legacy(symbol, event, record, time_series_dna, market_dna)
            self._schedule_followup(record)

            log.info(
                "💾 [V2] %s | event=%s | trajectory=%d pts (%ds) | scalp_signal_lead=%s",
                symbol, event_id, len(trajectory),
                record["trajectory_seconds_covered"],
                context.get("scalp_signal_offset_sec"),
            )
            return True

        except Exception as e:
            log.error(f"Capture failed for {event_id}: {e}")
            return False

    # ── Per-event field builders ─────────────────────────────────────────

    def _capture_trajectory(self, symbol, crossed_at_ts):
        """Snapshot the last TRAJECTORY_WINDOW_SEC seconds of
        ANALYSIS_020_TIMELINE up to (but not after) the crossing moment."""
        try:
            raw = self.r_source.hget(ANALYSIS_TIMELINE_KEY, symbol)
        except Exception:
            return []
        if not raw:
            return []
        try:
            timeline = json.loads(raw)
        except Exception:
            return []

        cutoff = crossed_at_ts - TRAJECTORY_WINDOW_SEC
        out = []
        for snap in timeline:
            try:
                ts = float(snap.get("timestamp", 0))
            except (TypeError, ValueError):
                continue
            if ts > crossed_at_ts:
                continue  # post-cross — exclude
            if ts < cutoff:
                continue  # before window
            out.append({
                "t_offset_sec":          int(ts - crossed_at_ts),
                "timestamp":             ts,
                "price":                 snap.get("price"),
                "volume":                snap.get("volume"),
                "status":                snap.get("status"),
                "micro_signal":          snap.get("micro_signal"),
                "is_overheated":         snap.get("is_overheated"),
                "sequence_report":       snap.get("sequence_report"),
                "prediction_confidence": snap.get("prediction_confidence"),
            })
        return out

    def _capture_market_dna(self, symbol):
        """Read consensus / regime / volume / BTC context for this symbol."""
        consensus, regime, btc = {}, {}, {}
        volume_score = 0.0

        try:
            raw = self.r_source.hget(CONSENSUS_KEY, symbol)
            consensus = json.loads(raw) if raw else {}
        except Exception:
            pass
        try:
            raw = self.r_source.hget(REGIME_KEY, symbol)
            regime = json.loads(raw) if raw else {}
        except Exception:
            pass
        try:
            raw = self.r_source.hget(VOLUME_KEY, symbol)
            if raw:
                try:
                    volume_score = float(json.loads(raw).get("score", 0))
                except Exception:
                    try:
                        volume_score = float(raw)
                    except Exception:
                        pass
        except Exception:
            pass
        try:
            raw = self.r_source.hget(BTC_KEY, "BTCUSDT")
            btc = json.loads(raw) if raw else {}
        except Exception:
            pass

        return {
            "consensus_score":   consensus.get("score", 0),
            "layers":            consensus.get("signals", {}),
            "decision_at_hit":   consensus.get("decision", "HOLD"),
            "regime":            regime.get("regime", "UNKNOWN"),
            "regime_confidence": regime.get("confidence", 0),
            "trend_direction":   regime.get("trend", "NEUTRAL"),
            "volume_strength":   volume_score,
            "btc_condition":     btc.get("condition", "STABLE"),
            "is_btc_bullish":    btc.get("trade_allowed", True),
        }

    @staticmethod
    def _compute_context(trajectory):
        """Derive summary stats from the captured trajectory."""
        ctx = {
            "scalp_signal_fired_before":  False,
            "scalp_signal_offset_sec":    None,
            "any_overheated_flag":        False,
            "max_drawdown_pct_in_window": 0.0,
            "trajectory_points":          len(trajectory),
        }
        if not trajectory:
            return ctx

        try:
            first_price = float(trajectory[0].get("price") or 0)
        except (TypeError, ValueError):
            first_price = 0.0

        if first_price > 0:
            try:
                low_price = min(
                    float(s.get("price") or first_price) for s in trajectory
                )
                ctx["max_drawdown_pct_in_window"] = round(
                    ((low_price / first_price) - 1) * 100, 4
                )
            except Exception:
                pass

        for s in trajectory:
            if s.get("micro_signal") == "SCALP_BUY_SIGNAL":
                ctx["scalp_signal_fired_before"] = True
                ctx["scalp_signal_offset_sec"]   = s.get("t_offset_sec")
                break

        ctx["any_overheated_flag"] = any(s.get("is_overheated") for s in trajectory)
        return ctx

    def _capture_legacy_time_series(self, symbol):
        """Multi-timeframe klines for the legacy ANALYSE_DB record.
        Routes through the WS-cached kline router (zero-REST when warm)."""
        data = {"seconds": {}, "minutes": {}, "hours": {}}
        ccxt_symbol = symbol if "/" in symbol else f"{symbol[:-4]}/{symbol[-4:]}"

        sec_kl = self.klines.fetch_ohlcv(ccxt_symbol, timeframe="1s", limit=60)
        if sec_kl:
            for s in INTERVALS_SEC:
                if len(sec_kl) >= s:
                    k = sec_kl[-s]
                    data["seconds"][f"{s}s"] = {"price": float(k[4]), "volume": float(k[5])}

        min_kl = self.klines.fetch_ohlcv(ccxt_symbol, timeframe="1m", limit=60)
        if min_kl:
            for m in INTERVALS_MIN:
                if len(min_kl) >= m:
                    k = min_kl[-m]
                    data["minutes"][f"{m}m"] = {"price": float(k[4]), "volume": float(k[5])}

        hr_kl = self.klines.fetch_ohlcv(ccxt_symbol, timeframe="1h", limit=10)
        if hr_kl:
            for h in INTERVALS_HOUR:
                if len(hr_kl) >= h:
                    k = hr_kl[-h]
                    data["hours"][f"{h}h"] = {"price": float(k[4]), "volume": float(k[5])}

        return data

    # ── Persistence ───────────────────────────────────────────────────────

    def _write_v2(self, record):
        event_id = record["event_id"]
        symbol   = record["symbol"]
        sym_key  = f"{PATTERNS_V2_SYM_PREFIX}:{symbol}"
        try:
            with self.r_analyse.pipeline(transaction=False) as pipe:
                pipe.hset(PATTERNS_V2_KEY, event_id, json.dumps(record))
                pipe.lpush(PATTERNS_V2_RECENT_KEY, event_id)
                pipe.ltrim(PATTERNS_V2_RECENT_KEY, 0, RECENT_LIST_CAP - 1)
                pipe.lpush(sym_key, event_id)
                pipe.ltrim(sym_key, 0, PER_SYMBOL_LIST_CAP - 1)
                pipe.execute()
        except Exception as e:
            log.error(f"V2 write failed for {event_id}: {e}")

    def _write_legacy(self, symbol, event, v2_record, time_series_dna, market_dna):
        """Write a legacy ANALYSE_DB record so pattern_matching_engine keeps
        producing PATTERN_MATCH_SIGNALS until it can be upgraded."""
        hit_id = f"{symbol}_{event.get('reachedAt')}"
        legacy = {
            "identity": {
                "symbol":          symbol,
                "recorded_at":     v2_record["crossed_at"],
                "profit_achieved": event.get("profit"),
                "price_performance": {
                    "base":     event.get("basePrice"),
                    "current":  event.get("crossingPrice"),
                    "gain_pct": v2_record["crossing"]["price_gain_pct"],
                },
            },
            "market_dna": {
                "consensus_score":   market_dna.get("consensus_score", 0),
                "layers":            market_dna.get("layers", {}),
                "regime":            market_dna.get("regime", "UNKNOWN"),
                "regime_confidence": market_dna.get("regime_confidence", 0),
                "volume_strength":   market_dna.get("volume_strength", 0.0),
                "btc_condition":     market_dna.get("btc_condition", "STABLE"),
                "is_btc_bullish":    market_dna.get("is_btc_bullish", True),
            },
            "time_series_dna": time_series_dna,
            "meta_criteria": {
                "decision_at_hit": market_dna.get("decision_at_hit", "HOLD"),
                "trend_direction": market_dna.get("trend_direction", "NEUTRAL"),
            },
        }
        try:
            self.r_analyse.hset(ANALYSE_DB_KEY, hit_id, json.dumps(legacy))
        except Exception as e:
            log.warning(f"Legacy ANALYSE_DB write failed for {hit_id}: {e}")

    def _schedule_followup(self, record):
        try:
            self.r_analyse.hset(
                PATTERNS_V2_FOLLOWUP_KEY,
                record["event_id"],
                json.dumps({
                    "symbol":           record["symbol"],
                    "scheduled_at_ts":  time.time(),
                    "price_at_cross":   record["crossing"].get("crossing_price"),
                }),
            )
        except Exception as e:
            log.warning(f"Followup schedule failed for {record['event_id']}: {e}")

    # ── 1-hour follow-up ──────────────────────────────────────────────────

    def _process_followups(self):
        try:
            pending = self.r_analyse.hgetall(PATTERNS_V2_FOLLOWUP_KEY) or {}
        except Exception as e:
            log.error(f"Followup read failed: {e}")
            return

        now = time.time()
        for event_id, raw in pending.items():
            try:
                f = json.loads(raw)
            except Exception:
                # Bad payload — drop it so we stop hitting it forever.
                self._discard_followup(event_id)
                continue
            if now - float(f.get("scheduled_at_ts", 0)) < FOLLOWUP_DELAY_SEC:
                continue
            self._complete_followup(event_id, f)

    def _complete_followup(self, event_id, f):
        symbol = f.get("symbol")
        ccxt_symbol = symbol if "/" in symbol else f"{symbol[:-4]}/{symbol[-4:]}"

        curr_price, curr_vol = None, None
        try:
            ohlcv = self.klines.fetch_ohlcv(ccxt_symbol, timeframe="1m", limit=1)
            if ohlcv:
                curr_price = float(ohlcv[-1][4])
                curr_vol   = float(ohlcv[-1][5])
        except Exception as e:
            log.warning(f"Followup price fetch failed for {symbol}: {e}")

        try:
            v2_raw = self.r_analyse.hget(PATTERNS_V2_KEY, event_id)
            v2 = json.loads(v2_raw) if v2_raw else None
        except Exception:
            v2 = None

        if v2 is not None and curr_price is not None:
            entry_price = f.get("price_at_cross") or v2["crossing"].get("crossing_price")
            gain_pct = (
                round(((curr_price / entry_price) - 1) * 100, 4)
                if entry_price else 0.0
            )
            v2["performance_after_1h"] = {
                "price":              curr_price,
                "volume":             curr_vol,
                "gain_since_hit_pct": gain_pct,
                "captured_at":        datetime.now(tz=timezone.utc).isoformat(),
            }
            try:
                self.r_analyse.hset(PATTERNS_V2_KEY, event_id, json.dumps(v2))
                log.info(
                    "✅ [FOLLOW-UP] %s | event=%s | gain_after_1h=%.3f%%",
                    symbol, event_id, gain_pct,
                )
            except Exception as e:
                log.warning(f"V2 followup update failed for {event_id}: {e}")

        self._discard_followup(event_id)

    def _discard_followup(self, event_id):
        try:
            self.r_analyse.hdel(PATTERNS_V2_FOLLOWUP_KEY, event_id)
        except Exception:
            pass


if __name__ == "__main__":
    SuccessPatternRecorder().run()
