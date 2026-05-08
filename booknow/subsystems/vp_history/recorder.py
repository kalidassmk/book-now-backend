"""
vp_history recorder
─────────────────────────────────────────────────────────────────────────────
Polls the local WATCH_ALL hash once per ``POLL_INTERVAL_SEC``. For every
fast-scanner-flagged symbol, captures (price, volume, vol_pct, ts) into
the Redis Cloud sorted set ``vp_history:{SYMBOL}``.

Three rules govern what lands in the cloud:

  1. STOP — when ``current_price < base_price`` we delete the symbol's
     history and add it to the stopped set. base_vol is dropped too so a
     later resume rebases volume from a fresh first sighting.

  2. RESUME — if a stopped symbol later climbs back above base_price, the
     stop marker is cleared and recording starts again from a new base.

  3. COMPRESSION — when |new_vol_pct - last_vol_pct| < VOL_PCT_EPSILON the
     last sorted-set member is removed and the new one is added with a
     fresher timestamp. Net effect: flat-volume runs collapse to a single
     point that keeps moving forward in time.

Local Redis stays read-only here. All writes target Redis Cloud.
"""

from __future__ import annotations

import json
import logging
import signal
import sys
import time
from typing import Optional

import redis

from booknow.binance.tickers_cache import get_default_cache
from booknow.repository import redis_keys

from .cloud_redis import get_cloud_redis
from .config import (
    LOCAL_REDIS_DB,
    LOCAL_REDIS_HOST,
    LOCAL_REDIS_PORT,
    MAX_POINTS_PER_SYMBOL,
    POLL_INTERVAL_SEC,
    RETENTION_SEC,
    VOL_PCT_EPSILON,
)
from .keys import VP_BASE_VOL_KEY, VP_STOPPED_KEY, history_key


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("VPHistoryRecorder")


class VPHistoryRecorder:
    def __init__(self) -> None:
        self.r_local = redis.Redis(
            host=LOCAL_REDIS_HOST,
            port=LOCAL_REDIS_PORT,
            db=LOCAL_REDIS_DB,
            decode_responses=True,
        )
        self.r_cloud = get_cloud_redis()
        self.tickers = get_default_cache()
        self._running = True
        # Rate-limit timestamps for noisy error paths.
        self._last_pipe_err_at = 0.0
        self._last_tick_err_at = 0.0

    # ── Lifecycle ────────────────────────────────────────────────────────
    def stop(self, *_: object) -> None:
        logger.info("Stop signal received — exiting after current iteration.")
        self._running = False

    def run(self) -> None:
        # Health-check both Redis endpoints up front so misconfiguration
        # surfaces in the supervisor log instead of silently looping.
        self.r_local.ping()
        self.r_cloud.ping()
        logger.info(
            "🚀 vp_history recorder started — poll=%.2fs epsilon=%.3f%% retention=%ss max_points=%d",
            POLL_INTERVAL_SEC, VOL_PCT_EPSILON, RETENTION_SEC, MAX_POINTS_PER_SYMBOL,
        )

        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)

        while self._running:
            iter_start = time.monotonic()
            try:
                self._tick()
            except Exception as e:
                now = time.monotonic()
                if now - self._last_tick_err_at >= 60.0:
                    logger.warning("vp_history tick failed: %s", str(e)[:200])
                    self._last_tick_err_at = now

            # Steady cadence regardless of tick duration.
            elapsed = time.monotonic() - iter_start
            time.sleep(max(0.0, POLL_INTERVAL_SEC - elapsed))

    # ── One iteration ────────────────────────────────────────────────────
    def _tick(self) -> None:
        watch_all = self.r_local.hgetall(redis_keys.WATCH_ALL) or {}
        if not watch_all:
            return

        ts_ms = int(time.time() * 1000)
        cutoff_ms = ts_ms - RETENTION_SEC * 1000

        # Pipeline cloud writes — saves roundtrips when many symbols update.
        pipe = self.r_cloud.pipeline(transaction=False)

        for symbol, payload in watch_all.items():
            try:
                row = json.loads(payload)
            except (TypeError, ValueError):
                continue

            base_price = _to_float(row.get("basePrice"))
            curr_price = _to_float(row.get("currentPrice"))
            if base_price <= 0 or curr_price <= 0:
                continue

            ticker = self.tickers.get_ticker(symbol)
            curr_vol = _to_float(ticker.get("quoteVolume")) if ticker else 0.0
            if curr_vol <= 0:
                # Tickers cache hasn't seen this symbol yet — skip silently.
                continue

            self._handle_symbol(pipe, symbol, base_price, curr_price, curr_vol, ts_ms, cutoff_ms)

        try:
            pipe.execute()
        except Exception as e:
            # Rate-limited: one short line per 60 s so a full Redis Cloud
            # (the most common cause) doesn't drown the supervisor logs.
            now = time.monotonic()
            if now - self._last_pipe_err_at >= 60.0:
                logger.warning("cloud pipeline execute failed: %s", str(e)[:200])
                self._last_pipe_err_at = now

    # ── Per-symbol logic ─────────────────────────────────────────────────
    def _handle_symbol(
        self,
        pipe,
        symbol: str,
        base_price: float,
        curr_price: float,
        curr_vol: float,
        ts_ms: int,
        cutoff_ms: int,
    ) -> None:
        hkey = history_key(symbol)
        is_stopped = bool(self.r_cloud.sismember(VP_STOPPED_KEY, symbol))

        # Rule 1 — STOP when price falls below base.
        if curr_price < base_price:
            if not is_stopped:
                pipe.delete(hkey)
                pipe.hdel(VP_BASE_VOL_KEY, symbol)
                pipe.sadd(VP_STOPPED_KEY, symbol)
            return

        # Rule 2 — RESUME if previously stopped and price has recovered.
        if is_stopped:
            pipe.srem(VP_STOPPED_KEY, symbol)
            # base_vol intentionally re-seeded below as a fresh first sighting.

        # Establish base_vol on first sight (or after a resume).
        base_vol_raw = self.r_cloud.hget(VP_BASE_VOL_KEY, symbol)
        if base_vol_raw is None:
            pipe.hset(VP_BASE_VOL_KEY, symbol, curr_vol)
            base_vol = curr_vol
        else:
            base_vol = _to_float(base_vol_raw) or curr_vol

        if base_vol <= 0:
            return
        vol_pct = (curr_vol - base_vol) / base_vol * 100.0

        # Rule 3 — COMPRESSION: overwrite last entry when vol_pct unchanged.
        last = self.r_cloud.zrevrange(hkey, 0, 0)
        if last:
            try:
                last_obj = json.loads(last[0])
                if abs(vol_pct - float(last_obj.get("vol_pct", 0.0))) < VOL_PCT_EPSILON:
                    pipe.zrem(hkey, last[0])
            except (TypeError, ValueError):
                pass

        member = json.dumps({
            "p": curr_price,
            "v": curr_vol,
            "vol_pct": round(vol_pct, 4),
            "ts": ts_ms,
        }, separators=(",", ":"))
        pipe.zadd(hkey, {member: ts_ms})

        # Retention: drop entries beyond the time window, then enforce
        # the per-symbol point cap as a safety net.
        pipe.zremrangebyscore(hkey, 0, cutoff_ms)
        pipe.zremrangebyrank(hkey, 0, -(MAX_POINTS_PER_SYMBOL + 1))


def _to_float(value) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def main() -> None:
    VPHistoryRecorder().run()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
