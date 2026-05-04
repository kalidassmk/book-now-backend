"""
delist.py
─────────────────────────────────────────────────────────────────────────────
Async port of BinanceDelistService.java.

Scrapes Binance's announcements API every 6 hours, looks for "Notice
of Removal of Spot Trading Pairs" articles, extracts the affected
symbols, and writes ``BINANCE:DELIST:<symbol> = "true"`` to Redis so
the market consumer (:mod:`booknow.binance.ws_streams`) and trade
executor can skip them.

Static seed (``DEFAULT_DELIST_SEED`` from ``util.momentum``) is the
safety net for boot-before-first-scrape and includes BTCUSDT/ETHUSDT
which are intentionally excluded from this micro-scalping engine.
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Iterable, Set

import redis.asyncio as aioredis

from booknow.binance.rate_limit import get_default as _get_rate_limit_guard
from booknow.binance.rest_api import BinanceIpBannedException, RestApiClient
from booknow.repository import redis_keys
from booknow.util.momentum import DEFAULT_DELIST_SEED


logger = logging.getLogger("booknow.delist")

REFRESH_INTERVAL_S = 6 * 60 * 60  # 6 hours
TARGET_TITLE = "Notice of Removal of Spot Trading Pairs"

# Article codes are 32-hex-digit ids prefixed with `c`. The CMS body
# embeds them in href links; this regex pulls them out for nested
# article expansion.
_ARTICLE_CODE_RE = re.compile(r"c[a-f0-9]{32}")

# Find symbol mentions like "BTC/USDT" or "BTCUSDT" inside article body.
_SYMBOL_RE = re.compile(r"\b([A-Z0-9]{2,12})/?USDT\b")


class DelistService:
    """Background scraper + Redis cache for delisted symbols.

    Public:
        ``await service.start()``         spawn the 6-hourly task
        ``await service.is_delisted(s)``  Redis lookup (used by traders)
        ``await service.get_set()``       full delist set as Python set[str]
        ``await service.stop()``          graceful shutdown
    """

    def __init__(self, redis_client: aioredis.Redis, rest: RestApiClient):
        self._redis = redis_client
        self._rest = rest
        self._guard = _get_rate_limit_guard()
        self._task: asyncio.Task | None = None
        self._running = False
        self._processed_codes: Set[str] = set()

    # ── Lifecycle ────────────────────────────────────────────────────────

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        # Best-effort initial scrape so the cache is warm at boot. The
        # background task takes over after this.
        await self._safe_scrape()
        self._task = asyncio.create_task(self._refresh_loop(), name="delist-refresh")
        logger.info("[DelistService] task spawned (6-hour refresh)")

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

    # ── Public API ───────────────────────────────────────────────────────

    async def is_delisted(self, symbol: str) -> bool:
        """O(1) check used by the trade executor before placing an order."""
        if not symbol:
            return False
        # Static seed is a hard block irrespective of Redis state.
        if symbol in DEFAULT_DELIST_SEED:
            return True
        return await self._redis.exists(f"{redis_keys.DELIST_PREFIX}{symbol}") == 1

    async def get_set(self) -> Set[str]:
        """Return the full delist set (seed ∪ Redis-discovered)."""
        keys = await self._redis.keys(f"{redis_keys.DELIST_PREFIX}*")
        from_redis = {
            (k.split(redis_keys.DELIST_PREFIX, 1)[1] if redis_keys.DELIST_PREFIX in k else k)
            for k in keys
        }
        return set(DEFAULT_DELIST_SEED) | from_redis

    async def mark(self, symbol: str) -> None:
        await self._redis.set(f"{redis_keys.DELIST_PREFIX}{symbol}", "true")

    # ── Scraper ──────────────────────────────────────────────────────────

    async def _safe_scrape(self) -> None:
        if self._guard.is_banned():
            logger.warning(
                "[DelistService] scrape skipped — Binance ban active for %ds",
                self._guard.ban_remaining_seconds(),
            )
            return
        try:
            await self._scrape_once()
        except BinanceIpBannedException as e:
            logger.error("[DelistService] scrape blocked by ban: %s", e)
        except Exception as e:
            if self._guard.report_if_banned(e):
                return
            logger.error("[DelistService] scrape failed: %s", e)

    async def _scrape_once(self) -> None:
        logger.info("[DelistService] scanning announcements for delistings…")
        self._processed_codes.clear()

        listing = await self._rest.get_announcements_list()
        articles = (
            listing.get("data", {})
            .get("catalogs", [{}])[0]
            .get("articles", [])
        )

        marked = 0
        for art in articles:
            title = art.get("title") or ""
            code = art.get("code") or ""
            if TARGET_TITLE in title and code:
                marked += await self._process_article(code, depth=0)

        logger.info("[DelistService] scan complete — %d new symbols marked", marked)

    async def _process_article(self, code: str, depth: int) -> int:
        if depth > 1 or code in self._processed_codes:
            return 0
        self._processed_codes.add(code)

        try:
            detail = await self._rest.get_announcement_detail(code)
        except BinanceIpBannedException:
            raise
        except Exception as e:
            if self._guard.report_if_banned(e):
                return 0
            logger.error("[DelistService] fetch detail %s failed: %s", code, e)
            return 0

        body = (detail.get("data", {}) or {}).get("body") or ""

        # 1) Extract symbol mentions in this article.
        marked = 0
        for m in _SYMBOL_RE.finditer(body):
            base = m.group(1)
            symbol = f"{base}USDT"
            await self.mark(symbol)
            marked += 1
            logger.info("[DelistService] marked DELISTED: %s", symbol)

        # 2) Recurse into linked articles (one level).
        if "Removal of Spot Trading Pairs" in body:
            for cm in _ARTICLE_CODE_RE.finditer(body):
                marked += await self._process_article(cm.group(0), depth + 1)

        return marked

    # ── Background ──────────────────────────────────────────────────────

    async def _refresh_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(REFRESH_INTERVAL_S)
            except asyncio.CancelledError:
                break
            if not self._running:
                break
            await self._safe_scrape()
