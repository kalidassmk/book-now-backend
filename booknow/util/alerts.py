"""
alerts.py — iter 58 (2026-05-23)
─────────────────────────────────────────────────────────────────────────────
Push notifications to the operator.  Used so the user knows the moment
the bot buys / fills / sells, without having to sit at the terminal.

Currently supports Telegram (free, fast, easy to set up).  The module is
structured so other channels (Discord webhook, Slack, email) can be
added later without touching the call sites.

## Setup (one-time)

1. On Telegram, message ``@BotFather`` and send ``/newbot``.  Pick a
   name and a unique username ending in ``bot``.  Copy the token it
   returns (looks like ``123456789:ABCdefGHIjkl...``).

2. On Telegram, start a chat with your new bot and send any message
   (e.g. ``hi``).

3. From any browser, open::

       https://api.telegram.org/bot<TOKEN>/getUpdates

   The JSON response includes ``"chat": {"id": 12345678, ...}`` — copy
   that ``id`` (your chat_id).

4. Add to ``/opt/booknow/.env`` on EC2::

       TELEGRAM_BOT_TOKEN=123456789:ABCdef...
       TELEGRAM_CHAT_ID=12345678

5. Flip ``alertsEnabled`` to ``true`` in Redis ``TRADING_CONFIG`` (or
   the dataclass default in trading_config.py — already True).

6. Restart the backend container.  You'll get a "Bot connected" alert
   on Telegram within a few seconds.

## What you'll get

  🛒 BUY <SYM>
  Price: $X.YZ
  Qty: N
  Leg: $96 / Rule: PATTERN_BOT:bounce

  ✅ FILLED <SYM>
  Buy: $X.YZ × N = $96.00
  TP set at $X.YZ (+$0.40 NET)

  💰 SOLD <SYM>
  Buy: $X.YZ → Sell: $X.YZ
  P&L: +$0.40 NET / +0.42%
  Hold: 3m12s   Reason: TP

  🛑 STOP-OUT <SYM>
  Buy: $X.YZ → Sell: $X.YZ
  P&L: −$0.62 / −0.65%
  Hold: 12m   Reason: HARD_SL

All sends are fire-and-forget; a Telegram failure never breaks a trade.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

import httpx

logger = logging.getLogger("booknow.alerts")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()

_client: Optional[httpx.AsyncClient] = None


def is_configured() -> bool:
    """Whether the Telegram credentials are present."""
    return bool(TELEGRAM_BOT_TOKEN) and bool(TELEGRAM_CHAT_ID)


async def publish_trade_alert(*, redis_client, symbol: str, action: str,
                               price=None, realised_net: float = None,
                               rule_label: str = "", extra: dict = None) -> None:
    """iter 60 — write a dashboard-banner event to Redis.

    The frontend's /api/alerts/feed reads TRADE_ALERTS:<date> and pushes
    it to the alert-banner.js overlay on every dashboard page.  This is
    the in-dashboard replacement for Telegram (which the operator turned
    off).
    """
    import json as _json
    import time as _time
    import datetime as _dt
    try:
        ev = {
            "ts": int(_time.time() * 1000),
            "symbol": symbol,
            "action": action,
            "rule_label": rule_label,
        }
        if price is not None:
            try: ev["price"] = float(price)
            except Exception: pass
        if realised_net is not None:
            try: ev["realised_net"] = float(realised_net)
            except Exception: pass
        if extra:
            ev.update(extra)
        date = _dt.datetime.now(_dt.UTC).strftime("%Y-%m-%d")
        key = f"TRADE_ALERTS:{date}"
        await redis_client.lpush(key, _json.dumps(ev))
        await redis_client.ltrim(key, 0, 999)
        await redis_client.expire(key, 7 * 24 * 60 * 60)
    except Exception as e:
        logger.debug("publish_trade_alert failed: %s", e)


async def send_telegram(message: str, parse_mode: str = "HTML") -> bool:
    """Fire-and-forget POST to the Telegram API.

    Returns True on success, False on any error.  Never raises.
    """
    if not is_configured():
        logger.debug("Telegram not configured — skipping alert.")
        return False
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=5)
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = await _client.post(
            url,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": parse_mode,
                "disable_web_page_preview": True,
            },
        )
        if resp.status_code == 200:
            return True
        logger.warning(
            "Telegram alert HTTP %s: %s",
            resp.status_code, resp.text[:200],
        )
        return False
    except httpx.TimeoutException:
        logger.debug("Telegram send timed out")
        return False
    except Exception as e:
        logger.debug("Telegram send error: %s", e)
        return False


# ── High-level event helpers ────────────────────────────────────────────


def _fmt_price(v) -> str:
    try:
        v = float(v)
        if v >= 100:    return f"{v:,.2f}"
        if v >= 1:      return f"{v:.4f}"
        if v >= 0.01:   return f"{v:.5f}"
        return f"{v:.8f}".rstrip("0").rstrip(".")
    except Exception:
        return str(v)


def _fmt_qty(v) -> str:
    try:
        v = float(v)
        return f"{v:.4f}".rstrip("0").rstrip(".") if v < 1000 else f"{v:,.2f}"
    except Exception:
        return str(v)


async def alert_buy_placed(
    *, symbol: str, price, qty, leg_usdt, rule_label: str,
) -> None:
    """Sent when a LIMIT BUY hits Binance (status=NEW)."""
    msg = (
        f"🛒 <b>BUY</b> {symbol}\n"
        f"Price: ${_fmt_price(price)}\n"
        f"Qty: {_fmt_qty(qty)}\n"
        f"Leg: ${leg_usdt:.0f} · Rule: {rule_label}"
    )
    await send_telegram(msg)


async def alert_buy_filled(
    *, symbol: str, fill_price, qty, tp_price, profit_amount_usdt,
) -> None:
    """Sent when a BUY actually fills on Binance (status=FILLED)."""
    try:
        cost = float(fill_price) * float(qty)
    except Exception:
        cost = 0
    msg = (
        f"✅ <b>FILLED</b> {symbol}\n"
        f"Buy: ${_fmt_price(fill_price)} × {_fmt_qty(qty)} = ${cost:,.2f}\n"
        f"TP set at ${_fmt_price(tp_price)} (+${profit_amount_usdt:.2f} NET)"
    )
    await send_telegram(msg)


async def alert_sold(
    *, symbol: str, buy_price, sell_price, qty, reason: str, hold_seconds: int,
) -> None:
    """Sent on any close (TP fill, HARD-SL, TSL, MAX-HOLD, TRAIL_FLOOR)."""
    try:
        bp = float(buy_price); sp = float(sell_price); q = float(qty)
        gross = (sp - bp) * q
        fees = 2 * 0.00075 * (bp * q)
        net = gross - fees
        pct = (sp - bp) / bp * 100 if bp else 0
    except Exception:
        net = 0; pct = 0
    icon = "💰" if net >= 0 else "🛑"
    sign = "+" if net >= 0 else ""
    hold_str = (f"{hold_seconds//60}m{hold_seconds%60}s" if hold_seconds < 3600
                else f"{hold_seconds//3600}h{(hold_seconds%3600)//60}m")
    msg = (
        f"{icon} <b>SOLD</b> {symbol}\n"
        f"Buy: ${_fmt_price(buy_price)} → Sell: ${_fmt_price(sell_price)}\n"
        f"P&amp;L: {sign}${net:.2f} NET / {sign}{pct:.2f}%\n"
        f"Hold: {hold_str} · Reason: {reason}"
    )
    await send_telegram(msg)


async def alert_blocked(
    *, symbol: str, rule_label: str, blocker_reason: str,
) -> None:
    """Optional: sent when a filter blocks a would-be buy.  Off by default
    (too noisy) but available if the operator wants visibility.
    """
    msg = (
        f"🚫 <b>BLOCKED</b> {symbol}\n"
        f"Rule: {rule_label}\n"
        f"{blocker_reason}"
    )
    await send_telegram(msg)


async def alert_startup(*, version_tag: str = "") -> None:
    """Sent once when the engine boots so the user knows alerts are live."""
    msg = (
        f"🤖 <b>BookNow online</b>\n"
        f"Live mode · alerts active\n"
        f"{version_tag}".strip()
    )
    await send_telegram(msg)
