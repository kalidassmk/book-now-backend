"""
futures_rest.py
─────────────────────────────────────────────────────────────────────────────
Tiny async helper for the two Binance Futures (USD-M, ``fapi``) public
endpoints the meta-model feature collector needs:

  GET /fapi/v1/premiumIndex      — current premium / funding rate
  GET /fapi/v1/openInterest      — current open interest

These don't have native WebSocket equivalents in the form the
meta-model wants (the WS feeds stream forced-liquidations and
mark-price ticks; we want the latest *value* on demand).

Both calls are unsigned and weight-cheap, so a single httpx client
with the engine-wide :class:`RateLimitGuard` is enough. We keep the
client out of :mod:`booknow.binance.rest_api` because that one is
spot-only by design.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import httpx

from booknow.binance.rate_limit import get_default as _get_rate_limit_guard


logger = logging.getLogger("booknow.subsystems.futures")


_FAPI = "https://fapi.binance.com"
_TIMEOUT_S = 10.0


class FuturesRestClient:
    """Minimal async ``fapi`` client. One per registry."""

    def __init__(self, http_client: Optional[httpx.AsyncClient] = None):
        self._http = http_client or httpx.AsyncClient(
            timeout=_TIMEOUT_S,
            verify=False,  # match other Binance modules' SSL convention
            headers={"User-Agent": "booknow-engine/0.1"},
        )
        self._owns_http = http_client is None
        self._guard = _get_rate_limit_guard()

    async def aclose(self) -> None:
        if self._owns_http:
            try:
                await self._http.aclose()
            except Exception:
                pass

    async def get_funding_rate(self, symbol: str) -> Optional[float]:
        """Latest funding rate for a perpetual.

        ``GET /fapi/v1/premiumIndex`` returns ``lastFundingRate`` on
        the per-symbol response.
        """
        data = await self._get("/fapi/v1/premiumIndex", {"symbol": symbol.upper()})
        if not data:
            return None
        try:
            return float(data.get("lastFundingRate", 0.0))
        except (TypeError, ValueError):
            return None

    async def get_open_interest(self, symbol: str) -> Optional[float]:
        """Current OI for a perpetual.

        ``GET /fapi/v1/openInterest`` → ``{"openInterest": "...", ...}``.
        """
        data = await self._get("/fapi/v1/openInterest", {"symbol": symbol.upper()})
        if not data:
            return None
        try:
            return float(data.get("openInterest", 0.0))
        except (TypeError, ValueError):
            return None

    async def _get(self, path: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if self._guard.is_banned():
            logger.debug(
                "fapi GET %s skipped — Binance ban active for %ds",
                path, self._guard.ban_remaining_seconds(),
            )
            return None
        try:
            r = await self._http.get(f"{_FAPI}{path}", params=params)
            if r.status_code in (418, 429) or (
                r.status_code >= 400 and "banned" in r.text.lower()
            ):
                self._guard.report_if_banned(RuntimeError(r.text))
                return None
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if self._guard.report_if_banned(e):
                return None
            # OI on a freshly-listed pair often 400s — log at debug.
            logger.debug("fapi GET %s %s failed: %s", path, params, e)
            return None
