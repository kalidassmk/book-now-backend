"""
rule_three.py
─────────────────────────────────────────────────────────────────────────────
Rule 3 — Multi-path Convergence to 5%. Direct port of RuleThree.java.

Highest-confidence signal. Requires that the move to 5% is confirmed
by more than one timing path:

  R3-STRONGEST : 3→5 (≤1500s) AND 2→5 (≤1500s)
                 AND 1→5 (≤1500s) AND 0→5 (≤2000s)
  R3-STRONG    : 3→5 (≤1500s) AND 2→5 (≤1500s)

Both fire BUY with +9% target — highest of the three rules.
"""

from __future__ import annotations

from typing import Any, Dict

from booknow.repository import redis_keys
from booknow.rules.base import RuleBase
from booknow.util.momentum import get_hms


SELL_PCT_RULE_3 = 9.0

MAX_3T5 = 1500.0
MAX_2T5 = 1500.0
MAX_1T5 = 1500.0
MAX_0T5 = 2000.0


class RuleThree(RuleBase):
    name = "rule_three"

    async def _tick(self) -> None:
        map_3t5 = await self._read_timing(redis_keys.ST3, "3T5")
        map_2t5 = await self._read_timing(redis_keys.ST2, "2T5")
        map_1t5 = await self._read_timing(redis_keys.ST1, "1T5")
        map_0t5 = await self._read_timing(redis_keys.ST0, "0T5")
        prices  = await self._read_current_prices()

        # 3T5 is the minimum requirement.
        for symbol, t3t5 in map_3t5.items():
            if symbol in self._triggered or symbol not in prices:
                continue
            if not (0 < t3t5 <= MAX_3T5):
                continue

            t2t5 = map_2t5.get(symbol, 0.0)
            t1t5 = map_1t5.get(symbol, 0.0)
            t0t5 = map_0t5.get(symbol, 0.0)

            has_2t5 = 0 < t2t5 <= MAX_2T5
            has_1t5 = 0 < t1t5 <= MAX_1T5
            has_0t5 = 0 < t0t5 <= MAX_0T5

            pattern = None
            if has_2t5 and has_1t5 and has_0t5:
                pattern = "R3-STRONGEST"
            elif has_2t5:
                pattern = "R3-STRONG"

            if pattern:
                self._triggered.add(symbol)
                payload = {
                    "symbol": symbol,
                    "type": pattern,
                    "threeToFive": t3t5,
                    "twoToFive": t2t5,
                    "oneToFive": t1t5,
                    "zeroToFive": t0t5,
                    "hms": get_hms(),
                }
                await self._save_rule_result(redis_keys.RULE_3, symbol, payload)
                await self._save_rule_result(redis_keys.RULE_3_HIT, symbol, payload)
                self.log.info(
                    "%s: %s | 3T5=%.1fs 2T5=%.1fs 1T5=%.1fs 0T5=%.1fs",
                    pattern, symbol, t3t5, t2t5, t1t5, t0t5,
                )
                await self._fire(symbol, prices, SELL_PCT_RULE_3, pattern)
