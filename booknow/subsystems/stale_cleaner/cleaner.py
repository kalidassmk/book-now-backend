"""
stale_cleaner.cleaner
─────────────────────────────────────────────────────────────────────────────
Sweeps WATCH_ALL every ``POLL_INTERVAL_SEC``. For any row whose
``timestamp`` field is older than ``STALE_THRESHOLD_SEC``, removes the
symbol from every market-state hash on the operational Redis:

  - WATCH_ALL                      (BASE_CURRENT_INC_%)
  - RW_BASE_PRICE
  - FAST_MOVE
  - All percentage buckets         (>0<1, >1<2, >2<3, >3<5, >5<7, >7<10, >10)
  - All speed-label hashes         (SUPER_FAST>2<3, ULTRA_FAST*, etc.)

Trading state and configs are intentionally untouched.

Why "stale" matters:
    base_price is set on first ticker sight and never updated. If a
    symbol stops ticking (delisted, low-liquidity drop-off), its row
    sits in WATCH_ALL forever with a frozen baseline that pollutes
    bucket scans and dashboard rendering. This sweep evicts those
    zombie rows without disturbing actively-tracked symbols.
"""

from __future__ import annotations

import json
import logging
import signal
import sys
import time

import redis

from booknow.repository import redis_keys

from .config import (
    LOCAL_REDIS_DB,
    LOCAL_REDIS_HOST,
    LOCAL_REDIS_PORT,
    POLL_INTERVAL_SEC,
    STALE_THRESHOLD_SEC,
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("StaleCleaner")


# Hashes that are keyed by SYMBOL and need an HDEL when a symbol is
# evicted. Pulled from redis_keys so a rename there is caught here.
_BUCKET_KEYS = (
    redis_keys.BUCKET_G0L1,
    redis_keys.BUCKET_G1L2,
    redis_keys.BUCKET_G2L3,
    redis_keys.BUCKET_G3L5,
    redis_keys.BUCKET_G5L7,
    redis_keys.BUCKET_G7L10,
    redis_keys.BUCKET_G10,
)
_SPEED_KEYS = (
    redis_keys.SUPER_FAST_2_3,
    redis_keys.ULTRA_FAST_3_5,
    redis_keys.ULTRA_SUPER_FAST_5_7,
    redis_keys.LT2MIN_0_TO_3,
    redis_keys.ULTRA_FAST_0_TO_2,
    redis_keys.ULTRA_FAST_2_TO_3,
)
_PER_SYMBOL_HASHES = (
    redis_keys.WATCH_ALL,
    redis_keys.RW_BASE_PRICE,
    redis_keys.FAST_MOVE,
    *_BUCKET_KEYS,
    *_SPEED_KEYS,
)


class StaleCleaner:
    def __init__(self) -> None:
        self.r = redis.Redis(
            host=LOCAL_REDIS_HOST,
            port=LOCAL_REDIS_PORT,
            db=LOCAL_REDIS_DB,
            decode_responses=True,
        )
        self._running = True

    def stop(self, *_: object) -> None:
        logger.info("Stop signal received — exiting after current iteration.")
        self._running = False

    def run(self) -> None:
        self.r.ping()
        logger.info(
            "🧹 stale_cleaner started — poll=%.1fs threshold=%ds",
            POLL_INTERVAL_SEC, STALE_THRESHOLD_SEC,
        )
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)

        while self._running:
            iter_start = time.monotonic()
            try:
                self._sweep()
            except Exception:
                logger.exception("stale_cleaner sweep failed (continuing)")

            elapsed = time.monotonic() - iter_start
            time.sleep(max(0.0, POLL_INTERVAL_SEC - elapsed))

    def _sweep(self) -> None:
        rows = self.r.hgetall(redis_keys.WATCH_ALL) or {}
        if not rows:
            return

        now_ms = int(time.time() * 1000)
        threshold_ms = STALE_THRESHOLD_SEC * 1000
        stale: list[str] = []

        for symbol, payload in rows.items():
            try:
                row = json.loads(payload)
            except (TypeError, ValueError):
                # Corrupt JSON — treat as stale, evict.
                stale.append(symbol)
                continue
            ts = int(row.get("timestamp") or 0)
            if ts == 0 or (now_ms - ts) > threshold_ms:
                stale.append(symbol)

        if not stale:
            return

        # Single pipeline: O(stale × hashes) HDELs in one round-trip.
        pipe = self.r.pipeline(transaction=False)
        for symbol in stale:
            for hkey in _PER_SYMBOL_HASHES:
                pipe.hdel(hkey, symbol)
        pipe.execute()

        logger.info("evicted %d stale symbol(s)", len(stale))


def main() -> None:
    StaleCleaner().run()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
