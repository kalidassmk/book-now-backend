"""
rest_api.py
─────────────────────────────────────────────────────────────────────────────
Async HTTP client for the few Binance endpoints that have NO websocket
equivalent and have to stay on REST:

  Public (no signing)
    GET /api/v3/exchangeInfo                 — symbol filters / tickSize
    GET https://www.binance.com/bapi/...     — announcements (delist scraper)

  Signed (HMAC-SHA256)
    POST /sapi/v1/asset/transfer             — universal transfer (dust)
    POST /sapi/v1/asset/dust                 — convert dust to BNB

The signed-call signing is identical to the WS-API client in ws_api.py
— sorted query string, HMAC-SHA256 hex digest, ``signature`` param.

Everything routes through :class:`RateLimitGuard` so a Binance ban
recorded by any other module pauses these calls too. 418/429 responses
are detected on the way out and the guard is armed automatically.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import re
import time
from typing import Any, Dict, Iterable, Optional
from urllib.parse import urlencode

import httpx

from booknow.binance.rate_limit import get_default as _get_rate_limit_guard


logger = logging.getLogger("booknow.rest_api")

API_BASE = "https://api.binance.com"
ANNOUNCEMENTS_BASE = "https://www.binance.com"

DEFAULT_TIMEOUT_S = 10.0
DEFAULT_BAN_COOLDOWN_S = 120

# Match other Binance modules' SSL convention on this host.
# Production deployments should use httpx default with certifi instead.
_VERIFY_SSL = False

# Regex for "banned until <epoch_ms>" inside Binance error bodies. We rely
# on the shared guard's parser too but checking here lets us fail-fast
# before forwarding bad data on.
_BAN_RE = re.compile(r"banned\s+until\s+(\d{12,})", re.IGNORECASE)


class BinanceIpBannedException(RuntimeError):
    """Mirror of the type in ws_api.py — same shape, separate import path
    so callers don't have to know which transport raised it.
    """

    def __init__(self, http_code: int, retry_after_seconds: int, message: str = ""):
        self.http_code = http_code
        self.retry_after_seconds = retry_after_seconds
        super().__init__(
            f"HTTP {http_code} from Binance — IP temporarily banned. "
            f"Retry-After: {retry_after_seconds}s. {message}"
        )


class RestApiClient:
    """Thin async wrapper around ``httpx.AsyncClient``.

    One instance per engine — share it across services. Reuses a
    persistent HTTP/2 connection pool so the few REST calls we still
    make are cheap.

    Construct with ``api_key`` + ``secret_key`` only when you intend to
    call signed endpoints; public endpoints work with no credentials.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        timeout_seconds: float = DEFAULT_TIMEOUT_S,
    ):
        self.api_key = api_key or ""
        self.secret_key = secret_key or ""
        self.timeout = timeout_seconds
        self._guard = _get_rate_limit_guard()
        self._client = httpx.AsyncClient(
            timeout=timeout_seconds,
            verify=_VERIFY_SSL,
            http2=False,  # Binance doesn't gain from h2 here; keep simpler.
            headers={"User-Agent": "booknow-engine/0.1"},
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    # ── Public endpoints ─────────────────────────────────────────────────

    async def get_exchange_info(self) -> Dict[str, Any]:
        """Fetch full exchange info (symbols, filters)."""
        return await self._public_get("/api/v3/exchangeInfo")

    async def get_announcements_list(
        self, catalog_id: int = 161, page_size: int = 20,
    ) -> Dict[str, Any]:
        """List recent announcements (used by the delist scraper)."""
        url = (
            f"{ANNOUNCEMENTS_BASE}/bapi/composite/v1/public/cms/article/list/query"
            f"?type=1&catalogId={catalog_id}&pageNo=1&pageSize={page_size}"
        )
        return await self._raw_get(url)

    async def get_announcement_detail(self, code: str) -> Dict[str, Any]:
        """Fetch a single announcement by code."""
        url = (
            f"{ANNOUNCEMENTS_BASE}/bapi/composite/v1/public/cms/article/detail/query"
            f"?articleCode={code}"
        )
        return await self._raw_get(url)

    # ── Signed (SAPI) endpoints ─────────────────────────────────────────

    async def universal_transfer(
        self, asset: str, amount: str, transfer_type: str = "FUNDING_MAIN",
    ) -> Dict[str, Any]:
        """``POST /sapi/v1/asset/transfer`` — moves funds between wallets.

        Default type ``FUNDING_MAIN`` matches Java's dust auto-transfer
        (Funding → Spot). Returns Binance's ``{tranId: ...}`` response.
        """
        params = {
            "type": transfer_type,
            "asset": asset.upper(),
            "amount": amount,
        }
        return await self._signed_post("/sapi/v1/asset/transfer", params)

    async def dust_transfer(self, assets: Iterable[str]) -> Dict[str, Any]:
        """``POST /sapi/v1/asset/dust`` — convert dust assets to BNB."""
        params = {"asset": ",".join(a.upper() for a in assets)}
        return await self._signed_post("/sapi/v1/asset/dust", params)

    # ── Internals ────────────────────────────────────────────────────────

    async def _public_get(self, path: str) -> Any:
        return await self._raw_get(f"{API_BASE}{path}")

    async def _raw_get(self, url: str) -> Any:
        if self._guard.is_banned():
            raise BinanceIpBannedException(
                418, self._guard.ban_remaining_seconds(),
                f"deferred GET {url}: cool-down active",
            )
        try:
            r = await self._client.get(url)
        except Exception as e:
            if self._guard.report_if_banned(e):
                raise BinanceIpBannedException(
                    418, self._guard.ban_remaining_seconds(), f"GET {url}: {e}",
                ) from e
            raise
        return self._handle_response(r, f"GET {url}")

    async def _signed_post(self, path: str, params: Dict[str, Any]) -> Any:
        if not self.api_key or not self.secret_key:
            raise ValueError(f"signed call POST {path} requires api_key + secret_key")
        if self._guard.is_banned():
            raise BinanceIpBannedException(
                418, self._guard.ban_remaining_seconds(),
                f"deferred POST {path}: cool-down active",
            )

        signed = dict(params)
        signed["timestamp"] = int(time.time() * 1000)
        signed["recvWindow"] = signed.get("recvWindow", 5000)
        query = urlencode(sorted(signed.items()))
        sig = hmac.new(
            self.secret_key.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        url = f"{API_BASE}{path}?{query}&signature={sig}"

        try:
            r = await self._client.post(
                url,
                headers={"X-MBX-APIKEY": self.api_key},
            )
        except Exception as e:
            if self._guard.report_if_banned(e):
                raise BinanceIpBannedException(
                    418, self._guard.ban_remaining_seconds(),
                    f"POST {path}: {e}",
                ) from e
            raise
        return self._handle_response(r, f"POST {path}")

    def _handle_response(self, r: httpx.Response, what: str) -> Any:
        """Translate HTTP/JSON errors into Python exceptions, surface bans."""
        # 418 / 429 with or without JSON body — Binance's IP-ban signal.
        if r.status_code in (418, 429):
            retry = _parse_retry_after(r.headers.get("Retry-After"))
            self._guard.record_ban_until(
                int(time.time() * 1000) + retry * 1000,
                f"{what} returned HTTP {r.status_code}",
            )
            raise BinanceIpBannedException(r.status_code, retry, r.text[:200])

        # Try to surface "banned until N" embedded in 4xx response bodies.
        if r.status_code >= 400:
            body = r.text or ""
            m = _BAN_RE.search(body)
            if m:
                until = int(m.group(1))
                self._guard.record_ban_until(until, f"{what} → {body[:160]}")
                retry = max(60, (until - int(time.time() * 1000)) // 1000)
                raise BinanceIpBannedException(r.status_code, retry, body[:200])
            # Other 4xx/5xx
            raise RuntimeError(f"{what} HTTP {r.status_code}: {body[:200]}")

        # Successful — parse JSON. Some endpoints return text; let it bubble.
        try:
            return r.json()
        except ValueError as e:
            raise RuntimeError(f"{what} non-JSON reply: {r.text[:200]}") from e


def _parse_retry_after(header: Optional[str]) -> int:
    if not header:
        return DEFAULT_BAN_COOLDOWN_S
    try:
        n = int(header.strip())
        return max(60, n)
    except ValueError:
        return DEFAULT_BAN_COOLDOWN_S
