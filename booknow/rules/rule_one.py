"""
rule_one.py
─────────────────────────────────────────────────────────────────────────────
Rule 1 — Rapid 0%→1%→2%→3% Ladder. Direct port of RuleOne.java.

Patterns:

  R1-FULL    : 0→1 (≤60s) AND 1→2 (≤60s) AND 2→3 (≤180s)
               → BUY with +5% target
  R1-PARTIAL : 1→2 (≤60s) AND 2→3 (≤180s)   (missed 0→1)
               → BUY with +5% target
  R1-ULTRA   : 0→1 (≤20s) AND 2→3 (≤20s)    (regardless of 1→2)
               → BUY with +3.5% target (tighter / faster)

R1-FULL and R1-PARTIAL are emitted together (same Redis row, type
``"R1-FULL"``) because the sell-side behaviour is identical — the
distinction is just for log readability. The Java code does the same.
"""

from __future__ import annotations

from typing import Any, Dict

from booknow.repository import redis_keys
from booknow.rules.base import RuleBase
from booknow.util.momentum import get_hms


# Sell-pct targets — match Java Constant defaults.
SELL_PCT_RULE_1_FULL = 5.0
SELL_PCT_RULE_1_FAST = 3.5

# Timing thresholds (seconds).
MAX_0T1_ULTRA = 20.0
MAX_0T1_FULL  = 60.0
MAX_1T2_FULL  = 60.0
MAX_2T3_ULTRA = 20.0
MAX_2T3_FULL  = 180.0


class RuleOne(RuleBase):
    name = "rule_one"

    async def _tick(self) -> None:
        # Pull the three timings R1 cares about.
        map_0t1 = await self._read_timing(redis_keys.ST0, "0T1")
        map_1t2 = await self._read_timing(redis_keys.ST1, "1T2")
        map_2t3 = await self._read_timing(redis_keys.ST2, "2T3")
        prices  = await self._read_current_prices()

        # ── R1-FULL / R1-PARTIAL ─────────────────────────────────────
        for symbol, t2t3 in map_2t3.items():
            if symbol in self._triggered:
                continue

            t1t2 = map_1t2.get(symbol, 0.0)
            t0t1 = map_0t1.get(symbol, 0.0)

            has_0t1 = 0 < t0t1 <= MAX_0T1_FULL
            has_1t2 = 0 < t1t2 <= MAX_1T2_FULL
            has_2t3 = 0 < t2t3 <= MAX_2T3_FULL

            if (has_0t1 and has_1t2 and has_2t3) or (has_1t2 and has_2t3):
                self._triggered.add(symbol)
                payload = _build_payload(symbol, t0t1, t1t2, t2t3, "R1-FULL")
                await self._save_rule_result(redis_keys.RULE_1, symbol, payload)
                await self._save_rule_result(redis_keys.RULE_1_HIT, symbol, payload)
                self.log.info(
                    "R1-FULL: %s | 0T1=%.1fs 1T2=%.1fs 2T3=%.1fs",
                    symbol, t0t1, t1t2, t2t3,
                )
                await self._fire(symbol, prices, SELL_PCT_RULE_1_FULL, "R1-FULL")

        # ── R1-ULTRA: 0→1 and 2→3 both ≤20s ──────────────────────────
        for symbol, t0t1 in map_0t1.items():
            if symbol in self._triggered:
                continue
            t2t3 = map_2t3.get(symbol)
            if t2t3 is None:
                continue
            if t0t1 <= MAX_0T1_ULTRA and t2t3 <= MAX_2T3_ULTRA:
                self._triggered.add(symbol)
                t1t2 = map_1t2.get(symbol, 0.0)
                payload = _build_payload(symbol, t0t1, t1t2, t2t3, "R1-ULTRA")
                await self._save_rule_result(redis_keys.RULE_1, symbol, payload)
                self.log.info(
                    "R1-ULTRA: %s | 0T1=%.1fs 2T3=%.1fs", symbol, t0t1, t2t3,
                )
                await self._fire(symbol, prices, SELL_PCT_RULE_1_FAST, "R1-ULTRA")


def _build_payload(
    symbol: str, t0t1: float, t1t2: float, t2t3: float, type_: str,
) -> Dict[str, Any]:
    return {
        "symbol": symbol,
        "type": type_,
        "zeroToOne": t0t1,
        "oneToTwo": t1t2,
        "twoToThree": t2t3,
        "hms": get_hms(),
    }
