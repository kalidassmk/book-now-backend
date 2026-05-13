#!/usr/bin/env python3
"""
scripts/spot_movers.py — standalone Binance Spot movers terminal app.

Listens to Binance's public `!miniTicker@arr` WebSocket (NO API KEY
NEEDED — these are public market streams) and renders a sorted table
of USDT pairs by % change. Auto-refreshes ~3x per second.

Optional --window flag picks between 24h (live, WebSocket-driven) and
1h / 4h (REST polled every 30s from /api/v3/ticker?windowSize=…).

Run:
    pip install websocket-client rich
    python3 scripts/spot_movers.py             # default: 24h, top 30
    python3 scripts/spot_movers.py --window 1h --top 50
    python3 scripts/spot_movers.py --filter BTC,ETH,SOL
    python3 scripts/spot_movers.py --min-volume 5   # in millions USDT
    python3 scripts/spot_movers.py --quiet            # no live UI, just print latest top N
    python3 scripts/spot_movers.py --no-rich          # plain ANSI output

If `rich` isn't installed the script falls back to plain ANSI rendering
so it still works on a stripped-down box. Only `websocket-client` is
strictly required.
"""
from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
import time
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional


BINANCE_WS_URL  = "wss://stream.binance.com:9443/ws/!miniTicker@arr"
BINANCE_REST    = "https://api.binance.com/api/v3"
LEVERAGED_RE_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR", "3L", "3S")


# ─────────────────────────────────────────────────────────────────────────────
# In-memory ticker cache, populated by the WebSocket thread.
# {symbol → {price, change24h, change1h, change4h, high24h, low24h, volume24h, ts}}
TICKERS: Dict[str, Dict[str, Any]] = {}
CACHE_LOCK = threading.Lock()
STOP_EVENT = threading.Event()
CONNECTION_STATE = {"connected": False, "last_msg_ts": 0.0}


def is_usdt_pair(symbol: str) -> bool:
    if not symbol or not symbol.endswith("USDT"):
        return False
    base = symbol[:-4]
    return not any(base.endswith(s) for s in LEVERAGED_RE_SUFFIXES)


def fmt_price(p: Optional[float]) -> str:
    if p is None:
        return "—"
    if p >= 100:
        return f"{p:,.2f}"
    if p >= 1:
        return f"{p:.4f}"
    if p >= 0.01:
        return f"{p:.6f}"
    return f"{p:.4g}"


def fmt_pct(v: Optional[float]) -> str:
    if v is None:
        return "—"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.2f}%"


def fmt_vol(v: Optional[float]) -> str:
    if v is None:
        return "—"
    if v >= 1e9:
        return f"{v/1e9:.2f}B"
    if v >= 1e6:
        return f"{v/1e6:.2f}M"
    if v >= 1e3:
        return f"{v/1e3:.1f}K"
    return f"{v:.0f}"


# ─────────────────────────────────────────────────────────────────────────────
# WebSocket thread — feeds 24h live data.
def ws_worker() -> None:
    """Persistent WebSocket loop with auto-reconnect."""
    try:
        import websocket  # type: ignore
    except ImportError:
        sys.stderr.write(
            "❌ websocket-client not installed. Run:\n"
            "    pip install websocket-client rich\n"
        )
        STOP_EVENT.set()
        return

    def on_message(_ws, raw):  # type: ignore[no-untyped-def]
        try:
            arr = json.loads(raw)
        except Exception:
            return
        if not isinstance(arr, list):
            return
        now = time.time()
        with CACHE_LOCK:
            for t in arr:
                sym = t.get("s")
                if not is_usdt_pair(sym):
                    continue
                try:
                    open_p = float(t.get("o") or 0)
                    last_p = float(t.get("c") or 0)
                except (TypeError, ValueError):
                    continue
                change = ((last_p - open_p) / open_p * 100) if open_p > 0 else 0.0
                entry = TICKERS.setdefault(sym, {})
                entry.update({
                    "symbol":    sym,
                    "price":     last_p,
                    "change24h": change,
                    "high24h":   float(t.get("h") or 0),
                    "low24h":    float(t.get("l") or 0),
                    "volume24h": float(t.get("q") or 0),  # quote volume (USDT)
                    "ts":        now,
                })
            CONNECTION_STATE["last_msg_ts"] = now

    def on_open(_ws):  # type: ignore[no-untyped-def]
        CONNECTION_STATE["connected"] = True

    def on_close(_ws, *_args):  # type: ignore[no-untyped-def]
        CONNECTION_STATE["connected"] = False

    def on_error(_ws, _err):  # type: ignore[no-untyped-def]
        CONNECTION_STATE["connected"] = False

    while not STOP_EVENT.is_set():
        try:
            ws = websocket.WebSocketApp(
                BINANCE_WS_URL,
                on_message=on_message,
                on_open=on_open,
                on_close=on_close,
                on_error=on_error,
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception:
            pass
        if STOP_EVENT.is_set():
            break
        time.sleep(3)  # reconnect backoff


# ─────────────────────────────────────────────────────────────────────────────
# REST poller — feeds 1h / 4h rolling-window change %.
def rest_poll_worker(window: str) -> None:
    """Poll Binance /api/v3/ticker every 30s for the given rolling window."""
    while not STOP_EVENT.is_set():
        with CACHE_LOCK:
            symbols = [s for s in TICKERS if is_usdt_pair(s)]
        # Wait until WebSocket has seeded the cache before first REST call.
        if not symbols:
            time.sleep(2)
            continue
        # Cap at 100 symbols per call (Binance rolling-window limit). Take
        # the top by 24h volume.
        with CACHE_LOCK:
            by_vol = sorted(
                (TICKERS[s] for s in symbols),
                key=lambda t: t.get("volume24h") or 0,
                reverse=True,
            )
        top = [t["symbol"] for t in by_vol[:100]]
        try:
            qs = urllib.parse.urlencode({
                "windowSize": window,
                "symbols": json.dumps(top, separators=(",", ":")),
            })
            url = f"{BINANCE_REST}/ticker?{qs}"
            req = urllib.request.Request(url, headers={"User-Agent": "spot_movers/1.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
            if isinstance(data, list):
                key = f"change{window}"
                with CACHE_LOCK:
                    for t in data:
                        sym = t.get("symbol")
                        if not sym or not is_usdt_pair(sym):
                            continue
                        try:
                            entry = TICKERS.setdefault(sym, {"symbol": sym})
                            entry[key] = float(t.get("priceChangePercent") or 0)
                        except (TypeError, ValueError):
                            pass
        except Exception:
            # Network blip — try again next loop.
            pass
        # Sleep but wake quickly on Ctrl+C.
        for _ in range(30):
            if STOP_EVENT.is_set():
                return
            time.sleep(1)


# ─────────────────────────────────────────────────────────────────────────────
# Rendering — rich if available, plain ANSI otherwise.
def filter_and_sort(window: str, top_n: int, min_vol: float,
                    filt: Optional[List[str]]) -> List[Dict[str, Any]]:
    change_key = f"change{window}"
    min_vol_usdt = min_vol * 1e6
    filt_upper = [f.upper() for f in (filt or [])]
    with CACHE_LOCK:
        rows = list(TICKERS.values())
    out: List[Dict[str, Any]] = []
    for t in rows:
        if (t.get("volume24h") or 0) < min_vol_usdt:
            continue
        if filt_upper and not any(f in t["symbol"] for f in filt_upper):
            continue
        if t.get(change_key) is None and window != "24h":
            # 1h/4h not yet fetched for this symbol
            continue
        out.append(t)
    out.sort(key=lambda t: t.get(change_key) or float("-inf"), reverse=True)
    return out[:top_n] if top_n > 0 else out


def render_rich(window: str, top_n: int, min_vol: float, filt: Optional[List[str]]) -> None:
    from rich.console import Console
    from rich.live import Live
    from rich.table import Table
    from rich.text import Text

    console = Console()

    def build_table() -> Table:
        title = (
            f"Binance Spot Movers · {window.upper()} · top {top_n} "
            f"· min vol ${min_vol:g}M"
        )
        if not CONNECTION_STATE["connected"]:
            title += " · [red]disconnected[/red]"
        else:
            age = time.time() - (CONNECTION_STATE["last_msg_ts"] or 0)
            title += f" · last tick {age:.1f}s ago"
        tbl = Table(
            title=title,
            header_style="bold cyan",
            row_styles=["", "dim"],
            padding=(0, 1),
        )
        tbl.add_column("Rank", justify="right", width=4)
        tbl.add_column("Symbol", style="bold")
        tbl.add_column("Price", justify="right")
        tbl.add_column(f"Change {window}", justify="right")
        tbl.add_column("High 24h", justify="right", style="dim")
        tbl.add_column("Low 24h", justify="right", style="dim")
        tbl.add_column("Volume 24h", justify="right", style="dim")
        rows = filter_and_sort(window, top_n, min_vol, filt)
        change_key = f"change{window}"
        for i, t in enumerate(rows, start=1):
            chg = t.get(change_key)
            chg_text = Text(fmt_pct(chg))
            if chg is None:
                chg_text.stylize("dim")
            elif chg >= 0:
                chg_text.stylize("bold green")
            else:
                chg_text.stylize("bold red")
            tbl.add_row(
                str(i),
                t["symbol"].replace("USDT", "") + "/USDT",
                fmt_price(t.get("price")),
                chg_text,
                fmt_price(t.get("high24h")),
                fmt_price(t.get("low24h")),
                fmt_vol(t.get("volume24h")),
            )
        if not rows:
            tbl.add_row("", "[dim]waiting for data…[/dim]", "", "", "", "", "")
        return tbl

    with Live(build_table(), console=console, refresh_per_second=3, screen=True) as live:
        while not STOP_EVENT.is_set():
            live.update(build_table())
            time.sleep(0.3)


def render_plain(window: str, top_n: int, min_vol: float, filt: Optional[List[str]]) -> None:
    """ANSI escape rendering for environments without rich."""
    GREEN = "\033[32m"; RED = "\033[31m"; DIM = "\033[2m"; BOLD = "\033[1m"
    CYAN = "\033[36m"; RESET = "\033[0m"
    CLEAR = "\033[2J\033[H"
    while not STOP_EVENT.is_set():
        rows = filter_and_sort(window, top_n, min_vol, filt)
        change_key = f"change{window}"
        out: List[str] = [CLEAR]
        age = time.time() - (CONNECTION_STATE["last_msg_ts"] or 0)
        status = f"{GREEN}●{RESET} live" if CONNECTION_STATE["connected"] else f"{RED}●{RESET} disconnected"
        out.append(
            f"{BOLD}Binance Spot Movers{RESET} · {CYAN}{window.upper()}{RESET} · "
            f"top {top_n} · min vol ${min_vol:g}M · {status} "
            f"({age:.1f}s since last tick)\n"
        )
        out.append(
            f"{BOLD}{'#':>3}  {'Symbol':<14}{'Price':>12}{'Change':>10}"
            f"{'High 24h':>14}{'Low 24h':>14}{'Vol 24h':>12}{RESET}"
        )
        for i, t in enumerate(rows, start=1):
            chg = t.get(change_key)
            colour = "" if chg is None else (GREEN if chg >= 0 else RED)
            out.append(
                f"{i:>3}  {(t['symbol'].replace('USDT','') + '/USDT'):<14}"
                f"{fmt_price(t.get('price')):>12}"
                f"{colour}{fmt_pct(chg):>10}{RESET}"
                f"{DIM}{fmt_price(t.get('high24h')):>14}"
                f"{fmt_price(t.get('low24h')):>14}"
                f"{fmt_vol(t.get('volume24h')):>12}{RESET}"
            )
        if not rows:
            out.append("\n  waiting for data…")
        sys.stdout.write("\n".join(out) + "\n")
        sys.stdout.flush()
        time.sleep(0.5)


def render_quiet(window: str, top_n: int, min_vol: float, filt: Optional[List[str]]) -> None:
    """One-shot: wait until cache is populated then print the table and exit."""
    deadline = time.time() + 15
    while time.time() < deadline:
        with CACHE_LOCK:
            if len(TICKERS) >= 50:
                break
        time.sleep(0.5)
    rows = filter_and_sort(window, top_n, min_vol, filt)
    change_key = f"change{window}"
    print(f"\nBinance Spot Movers — {window.upper()} — top {top_n}")
    print(f"{'#':>3}  {'Symbol':<14}{'Price':>14}{'Change':>10}{'Volume 24h':>14}")
    for i, t in enumerate(rows, start=1):
        print(
            f"{i:>3}  {(t['symbol'].replace('USDT','') + '/USDT'):<14}"
            f"{fmt_price(t.get('price')):>14}"
            f"{fmt_pct(t.get(change_key)):>10}"
            f"{fmt_vol(t.get('volume24h')):>14}"
        )
    print()


# ─────────────────────────────────────────────────────────────────────────────
def main() -> int:
    p = argparse.ArgumentParser(description="Binance Spot movers — live USDT pairs sorted by % change.")
    p.add_argument("--window", choices=("24h", "1h", "4h"), default="24h",
                   help="Timeframe for the Change column. 24h is live via WebSocket; 1h/4h polled every 30s via REST.")
    p.add_argument("--top", type=int, default=30, help="Show top N rows (0 = unlimited).")
    p.add_argument("--min-volume", type=float, default=2.0,
                   help="Minimum 24h quote volume in millions USDT (default 2.0).")
    p.add_argument("--filter", type=str, default="",
                   help="Comma-separated substring filter, e.g. BTC,ETH.")
    p.add_argument("--no-rich", action="store_true", help="Force plain ANSI rendering (no `rich` dependency).")
    p.add_argument("--quiet", action="store_true",
                   help="Print the table once and exit (no live UI).")
    args = p.parse_args()

    filt = [f.strip() for f in args.filter.split(",") if f.strip()] or None

    # Catch Ctrl+C cleanly so the live renderer can tear down its alternate
    # screen buffer.
    def _sigint(*_):
        STOP_EVENT.set()
    signal.signal(signal.SIGINT, _sigint)
    signal.signal(signal.SIGTERM, _sigint)

    ws_thread = threading.Thread(target=ws_worker, daemon=True, name="ws")
    ws_thread.start()

    rest_thread = None
    if args.window in ("1h", "4h"):
        rest_thread = threading.Thread(
            target=rest_poll_worker, args=(args.window,),
            daemon=True, name=f"rest-{args.window}"
        )
        rest_thread.start()

    try:
        if args.quiet:
            render_quiet(args.window, args.top, args.min_volume, filt)
        else:
            use_rich = not args.no_rich
            if use_rich:
                try:
                    import rich  # noqa: F401
                except ImportError:
                    use_rich = False
            if use_rich:
                render_rich(args.window, args.top, args.min_volume, filt)
            else:
                render_plain(args.window, args.top, args.min_volume, filt)
    finally:
        STOP_EVENT.set()
        ws_thread.join(timeout=2)
        if rest_thread:
            rest_thread.join(timeout=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
