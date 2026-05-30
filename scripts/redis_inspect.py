#!/usr/bin/env python3
"""
redis_inspect.py
─────────────────────────────────────────────────────────────────────────────
Read-only inspector / analyzer for the BookNow Redis (incl. AWS ElastiCache).

Connects using the same env vars the engine uses (REDIS_HOST / REDIS_PORT /
REDIS_DB / REDIS_PASSWORD, plus REDIS_TLS for ElastiCache in-transit
encryption), or a single REDIS_URL. CLI flags override env. **Never writes** —
only SCAN / GET / HGETALL / TYPE / INFO, so it's safe to point at production.

Usage:
    # Connection comes from env (REDIS_HOST/PORT/...) or flags:
    python scripts/redis_inspect.py ping
    python scripts/redis_inspect.py --host my-cache.xxxx.cache.amazonaws.com \
        --port 6379 --tls --password "$REDIS_PASSWORD" ping

    python scripts/redis_inspect.py keys 'BINANCE:SYMBOL:*'   # scan a pattern
    python scripts/redis_inspect.py get CURRENT_PRICE          # pretty-print any key
    python scripts/redis_inspect.py field CURRENT_PRICE BTCUSDT
    python scripts/redis_inspect.py summary                    # booknow schema overview
    python scripts/redis_inspect.py movers 15                  # top gainers (analysis)

Env:
    REDIS_HOST (default 127.0.0.1)  REDIS_PORT (6379)  REDIS_DB (0)
    REDIS_PASSWORD (optional)       REDIS_TLS=true|1   REDIS_URL (overrides all)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, List, Optional

import redis

# Known booknow hashes worth summarising (label, key). Kept inline so the
# script is standalone and runnable from anywhere, including the EC2 host.
_KNOWN_HASHES = [
    ("Current prices", "CURRENT_PRICE"),
    ("Baseline prices", "RW_BASE_PRICE"),
    ("Watch-list (all)", "BASE_CURRENT_INC_%"),
    ("Fast-move momentum", "FAST_MOVE"),
    ("Volume score", "VOLUME_SCORE"),
    ("Dashboard score", "DASHBOARD_SCORE"),
    ("Final consensus", "FINAL_CONSENSUS_STATE"),
    ("Buys", "BUY"),
    ("Sells", "SELL"),
]
_BUCKETS = [">0<1", ">1<2", ">2<3", ">3<5", ">5<7", ">7<10", ">10"]


# ── Connection ───────────────────────────────────────────────────────────


def connect(args) -> redis.Redis:
    url = args.url or os.environ.get("REDIS_URL")
    if url:
        client = redis.Redis.from_url(url, decode_responses=True, socket_timeout=10)
    else:
        tls = args.tls or os.environ.get("REDIS_TLS", "").lower() in ("1", "true", "yes")
        client = redis.Redis(
            host=args.host or os.environ.get("REDIS_HOST", "127.0.0.1"),
            port=args.port or int(os.environ.get("REDIS_PORT", "6379")),
            db=args.db if args.db is not None else int(os.environ.get("REDIS_DB", "0")),
            password=args.password or os.environ.get("REDIS_PASSWORD") or None,
            ssl=tls,
            ssl_cert_reqs=None if tls else "required",
            decode_responses=True,
            socket_timeout=10,
            socket_connect_timeout=10,
        )
    return client


def _maybe_json(value: str) -> Any:
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value


def _fmt(obj: Any) -> str:
    return json.dumps(obj, indent=2, ensure_ascii=False, default=str)


# ── Commands ───────────────────────────────────────────────────────────────


def cmd_ping(client: redis.Redis, args) -> None:
    pong = client.ping()
    info = client.info()
    print(f"PING → {pong}")
    print(f"  server   : Redis {info.get('redis_version')} ({info.get('os','?')})")
    print(f"  mode     : {info.get('redis_mode')}  role: {info.get('role')}")
    print(f"  clients  : {info.get('connected_clients')}")
    print(f"  memory   : {info.get('used_memory_human')}")
    print(f"  uptime   : {info.get('uptime_in_seconds')}s")
    try:
        print(f"  keyspace : {client.dbsize()} keys in db")
    except Exception:
        pass


def cmd_keys(client: redis.Redis, args) -> None:
    pattern = args.pattern or "*"
    count = 0
    by_type: Dict[str, int] = {}
    print(f"Scanning keys matching {pattern!r} …")
    for key in client.scan_iter(match=pattern, count=500):
        ktype = client.type(key)
        by_type[ktype] = by_type.get(ktype, 0) + 1
        count += 1
        if count <= args.limit:
            size = _key_size(client, key, ktype)
            print(f"  {key}  [{ktype}{'' if size is None else f', {size}'}]")
    if count > args.limit:
        print(f"  … and {count - args.limit} more (raise --limit to see them)")
    print(f"\nTotal: {count} keys  |  by type: {by_type}")


def _key_size(client: redis.Redis, key: str, ktype: str) -> Optional[str]:
    try:
        if ktype == "hash":
            return f"{client.hlen(key)} fields"
        if ktype == "list":
            return f"{client.llen(key)} items"
        if ktype == "set":
            return f"{client.scard(key)} members"
        if ktype == "zset":
            return f"{client.zcard(key)} members"
        if ktype == "string":
            return f"{client.strlen(key)} bytes"
    except Exception:
        return None
    return None


def cmd_get(client: redis.Redis, args) -> None:
    key = args.key
    ktype = client.type(key)
    if ktype == "none":
        print(f"(key {key!r} does not exist)")
        return
    if ktype == "string":
        print(_fmt(_maybe_json(client.get(key))))
    elif ktype == "hash":
        raw = client.hgetall(key)
        out = {f: _maybe_json(v) for f, v in raw.items()}
        n = len(out)
        if args.limit and n > args.limit:
            out = dict(list(out.items())[: args.limit])
            print(f"(hash has {n} fields; showing first {args.limit} — raise --limit)")
        print(_fmt(out))
    elif ktype == "list":
        print(_fmt([_maybe_json(v) for v in client.lrange(key, 0, args.limit - 1)]))
    elif ktype == "set":
        print(_fmt([_maybe_json(v) for v in list(client.smembers(key))[: args.limit]]))
    elif ktype == "zset":
        print(_fmt(client.zrange(key, 0, args.limit - 1, withscores=True)))
    else:
        print(f"(unsupported type: {ktype})")


def cmd_field(client: redis.Redis, args) -> None:
    val = client.hget(args.key, args.field)
    if val is None:
        print(f"(field {args.field!r} not found in hash {args.key!r})")
        return
    print(_fmt(_maybe_json(val)))


def cmd_summary(client: redis.Redis, args) -> None:
    print(f"Redis summary — {client.dbsize()} keys total\n")
    print("Known booknow hashes:")
    for label, key in _KNOWN_HASHES:
        try:
            if client.exists(key) and client.type(key) == "hash":
                print(f"  {label:<22} {key:<22} {client.hlen(key):>6} fields")
            else:
                print(f"  {label:<22} {key:<22}      — (absent)")
        except Exception as e:
            print(f"  {label:<22} {key:<22}  error: {e}")
    print("\nPercentage-gain buckets:")
    for b in _BUCKETS:
        try:
            n = client.hlen(b) if client.exists(b) and client.type(b) == "hash" else 0
            print(f"  {b:<8} {n:>5} symbols")
        except Exception:
            print(f"  {b:<8}     —")
    print("\nBINANCE:* key families (sampled):")
    fam: Dict[str, int] = {}
    for key in client.scan_iter(match="BINANCE:*", count=500):
        prefix = ":".join(key.split(":")[:2]) + ":"
        fam[prefix] = fam.get(prefix, 0) + 1
    for prefix, n in sorted(fam.items()):
        print(f"  {prefix:<20} {n:>6} keys")
    if not fam:
        print("  (none)")


def cmd_movers(client: redis.Redis, args) -> None:
    """Top gainers from the watch-list hash — a quick worked analysis."""
    key = "BASE_CURRENT_INC_%"
    if not client.exists(key):
        print(f"(watch-list hash {key!r} not present)")
        return
    rows: List[Dict[str, Any]] = []
    for sym, payload in client.hgetall(key).items():
        data = _maybe_json(payload)
        if isinstance(data, dict):
            rows.append(data)
    rows.sort(key=lambda r: float(r.get("increasedPercentage", 0) or 0), reverse=True)
    top = rows[: args.n]
    print(f"Top {len(top)} movers by increasedPercentage:\n")
    print(f"  {'SYMBOL':<14}{'GAIN %':>10}{'CUR PRICE':>16}{'BASE %':>10}")
    for r in top:
        print(
            f"  {str(r.get('symbol','?')):<14}"
            f"{float(r.get('increasedPercentage',0) or 0):>10.2f}"
            f"{float(r.get('currentPrice',0) or 0):>16.8g}"
            f"{float(r.get('basePercentage',0) or 0):>10.2f}"
        )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Read-only BookNow Redis inspector/analyzer.")
    p.add_argument("--host"); p.add_argument("--port", type=int)
    p.add_argument("--db", type=int); p.add_argument("--password")
    p.add_argument("--tls", action="store_true", help="use TLS (ElastiCache in-transit encryption)")
    p.add_argument("--url", help="full redis:// or rediss:// URL (overrides host/port/...)")
    p.add_argument("--limit", type=int, default=50, help="max items to print (default 50)")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("ping")
    k = sub.add_parser("keys"); k.add_argument("pattern", nargs="?", default="*")
    g = sub.add_parser("get"); g.add_argument("key")
    f = sub.add_parser("field"); f.add_argument("key"); f.add_argument("field")
    sub.add_parser("summary")
    m = sub.add_parser("movers"); m.add_argument("n", nargs="?", type=int, default=10)
    return p


_DISPATCH = {
    "ping": cmd_ping, "keys": cmd_keys, "get": cmd_get,
    "field": cmd_field, "summary": cmd_summary, "movers": cmd_movers,
}


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        client = connect(args)
    except Exception as e:
        print(f"Connection setup failed: {e}", file=sys.stderr)
        return 2
    try:
        _DISPATCH[args.cmd](client, args)
    except redis.exceptions.ConnectionError as e:
        print(f"\n❌ Could not reach Redis: {e}", file=sys.stderr)
        print("   Check host/port/TLS/password and that egress to the endpoint "
              "is allowed by the network policy.", file=sys.stderr)
        return 1
    except redis.exceptions.AuthenticationError as e:
        print(f"\n❌ Auth failed: {e} — check REDIS_PASSWORD.", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        return 1
    finally:
        try:
            client.close()
        except Exception:
            pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
