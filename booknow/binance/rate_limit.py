"""
rate_limit.py
─────────────────────────────────────────────────────────────────────────────
Process-wide guard for Binance IP bans / rate-limit cool-downs.

Direct port of `BinanceRateLimitGuard.java`. Every async task that talks
to Binance REST or WS calls `is_banned()` before firing, and on exception
calls `report_if_banned(exc)` so the cool-down propagates across every
caller.

Without this, each scheduled task ignores the others' failures and the
combined retry storm extends the ban indefinitely.
"""

from __future__ import annotations

import logging
import re
import threading
import time

logger = logging.getLogger("rate_limit")

# Matches "banned until <epoch_ms>" inside Binance's -1003 error string,
# e.g. "Way too much request weight used; IP banned until 1777814809681".
_BAN_UNTIL_RE = re.compile(r"banned\s+until\s+(\d{12,})", re.IGNORECASE)

# Default cool-down when Binance gives a 418/429 with no explicit timestamp.
_DEFAULT_COOLDOWN_S = 120

# Substrings that flag a Binance rate-limit response even if the structured
# "banned until N" timestamp is absent. Lowercase comparison.
_BAN_HINTS = (
    "ip banned",
    "ip auto banned",
    "too much request weight",
    "418 i'm a teapot",
    "teapot",
    "way too much request weight",
)


class RateLimitGuard:
    """Thread-safe singleton-style guard.

    Public API mirrors the Java class: `is_banned`, `ban_remaining_seconds`,
    `record_ban_until(epoch_ms, context)`, `report_if_banned(exc)`, `clear()`.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ban_until_ms: int = 0

    # ── Public reads ─────────────────────────────────────────────────────

    def is_banned(self) -> bool:
        return self._ban_until_ms > _now_ms()

    def ban_remaining_seconds(self) -> int:
        remaining_ms = self._ban_until_ms - _now_ms()
        return max(0, remaining_ms // 1000)

    # ── Public writes ────────────────────────────────────────────────────

    def record_ban_until(self, epoch_ms: int, context: str = "") -> None:
        """Mark a ban explicitly. Only extends; never shortens."""
        with self._lock:
            if epoch_ms > self._ban_until_ms:
                self._ban_until_ms = epoch_ms
                secs = (epoch_ms - _now_ms()) // 1000
                logger.error(
                    "[RateLimitGuard] Binance IP ban recorded — sleeping %ds (%s). "
                    "All scheduled callers will skip until then.",
                    secs, _truncate(context),
                )

    def report_if_banned(self, exc: BaseException) -> bool:
        """Inspect an exception's message chain for ban indicators.

        Returns True iff a ban was detected; the guard is updated in
        place. Callers can `if guard.report_if_banned(e): return` to bail
        out cleanly.
        """
        cur: BaseException | None = exc
        while cur is not None:
            msg = str(cur) or ""
            if msg:
                m = _BAN_UNTIL_RE.search(msg)
                if m:
                    try:
                        until = int(m.group(1))
                        self.record_ban_until(until, f"from Binance error: {_truncate(msg)}")
                        return True
                    except ValueError:
                        pass

                lower = msg.lower()
                if any(hint in lower for hint in _BAN_HINTS):
                    self.record_ban_until(
                        _now_ms() + _DEFAULT_COOLDOWN_S * 1000,
                        f"default cooldown from message: {_truncate(msg)}",
                    )
                    return True

            cur = cur.__cause__ or cur.__context__
        return False

    def clear(self) -> None:
        """Manual override (e.g. after a successful probe). Use sparingly."""
        with self._lock:
            self._ban_until_ms = 0
        logger.info("[RateLimitGuard] Cooldown cleared manually.")


# ── Helpers ──────────────────────────────────────────────────────────────


def _now_ms() -> int:
    return int(time.time() * 1000)


def _truncate(s: str, limit: int = 160) -> str:
    if not s:
        return ""
    return s if len(s) <= limit else s[:limit] + "…"


# Module-level singleton — created lazily so tests can substitute.
_default: RateLimitGuard | None = None


def get_default() -> RateLimitGuard:
    """Process-wide guard. Use this everywhere unless you have a reason."""
    global _default
    if _default is None:
        _default = RateLimitGuard()
    return _default
