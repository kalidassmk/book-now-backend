"""
rule_two.py
─────────────────────────────────────────────────────────────────────────────
Rule 2 — Sustained 1%→2%→3%→5% Ladder. Direct port of RuleTwo.java.

Patterns:

  R2-FULL    : 1→2 (≤150s) AND 2→3 (≤150s) AND 3→5 (≤1500s)
               → BUY with +7% target
  R2-PARTIAL : 2→3 (≤150s) AND 3→5 (≤1500s)   (entered at 2% band)
               → BUY with +7% target

Higher sell target than R1 because the sustained, multi-step
confirmation indicates stronger underlying momentum.
"""

from __future__ import annotations

from typing import Any, Dict

from booknow.repository import redis_keys
from booknow.rules.base import RuleBase
from booknow.util.momentum import get_hms


SELL_PCT_RULE_2 = 7.0

MAX_1T2 = 150.0
MAX_2T3 = 150.0
MAX_3T5 = 1500.0


class RuleTwo(RuleBase):
    name = "rule_two"

    async def _tick(self) -> None:
        map_1t2 = await self._read_timing(redis_keys.ST1, "1T2")
        map_2t3 = await self._read_timing(redis_keys.ST2, "2T3")
        map_3t5 = await self._read_timing(redis_keys.ST3, "3T5")
        prices  = await self._read_current_prices()

        # 3T5 is the final-confirmation step — anchor iteration there.
        for symbol, t3t5 in map_3t5.items():
            if symbol in self._triggered or symbol not in prices:
                continue

            has_3t5 = 0 < t3t5 <= MAX_3T5
            if not has_3t5:
                continue

            t2t3 = map_2t3.get(symbol, 0.0)
            t1t2 = map_1t2.get(symbol, 0.0)
            has_2t3 = 0 < t2t3 <= MAX_2T3
            has_1t2 = 0 < t1t2 <= MAX_1T2

            pattern = None
            if has_1t2 and has_2t3:
                pattern = "R2-FULL"
            elif has_2t3:
                pattern = "R2-PARTIAL"

            if pattern:
                self._triggered.add(symbol)
                payload = {
                    "symbol": symbol,
                    "type": pattern,
                    "oneToTwo": t1t2,
                    "twoToThree": t2t3,
                    "threeToFive": t3t5,
                    "hms": get_hms(),
                }
                await self._save_rule_result(redis_keys.RULE_2, symbol, payload)
                await self._save_rule_result(redis_keys.RULE_2_HIT, symbol, payload)
                self.log.info(
                    "%s: %s | 1T2=%.1fs 2T3=%.1fs 3T5=%.1fs",
                    pattern, symbol, t1t2, t2t3, t3t5,
                )
                await self._fire(symbol, prices, SELL_PCT_RULE_2, pattern)
