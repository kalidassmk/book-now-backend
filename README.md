# python-engine

Single-process Python rewrite of the BookNow trading stack. Replaces:

- `book-now-v3/` (Spring Boot Java backend) — **deleted in Phase 18**
- `binance-sentiment-engine/` — wired in via :mod:`booknow.sentiment.supervisor`,
  consolidated under `booknow/sentiment/scripts/` in Phase 19. The original
  top-level `binance-sentiment-engine/` directory was a runtime dependency
  for the supervisor's subprocesses; after Phase 19 the engine ships
  everything in one tree.

## Layout

```
python-engine/
├── pyproject.toml
└── booknow/
    ├── main.py              # asyncio orchestrator — single boot for everything
    ├── config/              # static env + dashboard-editable TradingConfig
    ├── binance/             # all Binance I/O, WebSocket-first
    │   ├── rate_limit.py    # ★ Phase 1
    │   ├── ws_streams.py    # market WS (allRollingWindowTicker, miniTicker)
    │   ├── ws_api.py        # WS-API for orders + listenKey
    │   ├── user_data.py     # listenKey lifecycle + balance/order events
    │   ├── balances.py      # WS-driven, no REST poll
    │   ├── filters.py       # exchangeInfo cache (REST stays — no stream)
    │   ├── dust.py          # balance-driven detection; transfers stay REST
    │   ├── delist.py        # announcements scraper
    │   ├── klines_cache.py  # moved from binance-sentiment-engine
    │   └── tickers_cache.py # moved from binance-sentiment-engine
    ├── repository/
    │   └── redis_keys.py    # ★ Phase 1 — single source of truth for Redis key names
    ├── trading/             # executor, state, TSL, position monitor, safety
    ├── processors/          # ULF0To3, FastAnalyse, TimeAnalyser, FastMoveFilter
    ├── rules/               # RuleOne / RuleTwo / RuleThree
    ├── analysis/            # CoinAnalyzer, indicators
    ├── sentiment/           # market_engine, volume_price, profit_020_trend, etc.
    ├── subsystems/          # risk_management, fakeout_detector, volume_profile,
    │                        # trend_alignment, meta_model
    ├── scalper/             # order-flow scalper: WS stream + analyzer + engine
    └── api/                 # FastAPI routes for the dashboard
```

★ = present after Phase 1.

## Run

```bash
cd python-engine
pip install -e .
booknow                       # or: python -m booknow.main
```

Configuration is read from environment variables and `.env` files:
- `python-engine/.env` (preferred for engine-only overrides)
- `../dashboard/.env` (fallback — shares Binance keys with the existing
  Node dashboard so we don't duplicate credentials)

Key vars: `BINANCE_API_KEY`, `BINANCE_SECRET_KEY`, `REDIS_HOST`,
`REDIS_PORT`, `BOOKNOW_LIVE_MODE` (default `false`),
`BOOKNOW_HTTP_PORT` (default `8083`).

## Migration phases (this branch)

| Phase | Status | Description |
|---|---|---|
| 1 | ✅ DONE | Skeleton, config, Redis key constants, rate-limit guard |
| 2 | – | Move klines_cache + tickers_cache from binance-sentiment-engine |
| 3 | – | WS-API client (orders + listenKey) |
| 4 | – | User-data-stream + balance service (WS push) |
| 5 | – | Market WS consumer (allRollingWindowTicker) |
| 6 | – | filters / dust / delist services |
| 7 | – | Indicators + CoinAnalyzer |
| 8 | – | Processors (4) |
| 9 | – | TradeState + TSL + position monitor |
| 10 | – | TradeExecutor (orders via WS-API) — high risk |
| 11 | – | Rules (R1/R2/R3) |
| 12 | – | Sentiment integration as async tasks |
| 13 | – | Subsystem fetchers |
| 14 | – | FastAPI dashboard endpoints |
| 15 | – | main.py orchestrator wired to spawn all of the above |
| 16 | – | dashboard/server.js → python-engine port |
| 17 | ✅ | Paper-trade soak harness + 13-min validation run |
| 18 | ✅ | Deleted book-now-v3/ + dashboard/src/ (orphan scaffold) |
| 19 | ✅ | Moved binance-sentiment-engine/ under booknow/sentiment/scripts/ |

## REST → WS migration intent

Wherever a stream exists, the engine uses it. REST is reserved for:

- Things with no stream equivalent: `exchangeInfo`, historical klines
  (older than the current candle), dust transfers (write op).
- One-shot seeds where a WS would take longer to warm than a single
  REST call (e.g. the 10-minute history seed when a new symbol enters
  tracking).

Order placement uses the **WebSocket API** (`wss://ws-api.binance.com/ws-api/v3`),
not REST. Lower latency on +$0.20 scalps.

## Order-flow scalper

`booknow/scalper/` adds a real-time **order-flow scalping** algorithm. It opens
one combined Binance websocket (`aggTrade` + `depth`) for a small set of symbols
and continuously evaluates the scalper order-flow checklist, emitting
**BUY / SELL / HOLD** per symbol. It starts/stops with the engine (no separate
process) and its snapshots are served by the API and a built-in dashboard.

**Before BUYING (all must hold):** delta turning positive · market buys
increasing · buy wall below price · no large sell wall above · volume spike.
**Before SELLING (all must hold):** delta negative · market sells increasing ·
large sell wall above · buy wall disappears. Otherwise **HOLD**.

How each condition is derived:

| Condition | Definition |
|---|---|
| Delta | `buy_volume − sell_volume` over the rolling window (default 5s); on `aggTrade`, `m=false` → market buy, `m=true` → market sell |
| Delta turning positive / negative | current delta crosses 0 **and** rises / falls vs. the previous window |
| Market buys/sells increasing | window buy/sell volume greater than the previous window |
| Buy / sell wall | a book level ≥ `wall_multiple` × the average level size on that side (default 3×) |
| Buy wall disappears | a buy wall was present last tick and is now gone |
| Volume spike | window volume ≥ `volume_spike_multiple` × average per-window volume over the baseline (default 2× over 60s) |

Endpoints (mounted on the same FastAPI port, default `8083`):

| Method | Path | Description |
|---|---|---|
| GET | `/api/v1/scalper/status` | Engine + connection status and config |
| GET | `/api/v1/scalper/snapshots` | Latest order-flow snapshot for every symbol |
| GET | `/api/v1/scalper/snapshot/{symbol}` | Snapshot for one symbol |
| GET | `/api/v1/scalper/signals?limit=50` | Recent BUY/SELL signal log |
| WS | `/api/v1/scalper/ws` | Live push of status + snapshots + signals (1 Hz) |
| GET | `/api/v1/scalper/dashboard` | Self-contained live dashboard (HTML) |

Tunable via env vars: `SCALPER_SYMBOLS` (default `BTCUSDT,ETHUSDT,SOLUSDT`),
`SCALPER_WINDOW_SEC`, `SCALPER_BASELINE_SEC`, `SCALPER_WALL_MULTIPLE`,
`SCALPER_VOLUME_SPIKE_MULTIPLE`, `SCALPER_MIN_TRADES`, `SCALPER_DEPTH_LEVELS`.

Offline tests (no network): `python -m pytest tests/test_scalper_order_flow.py`.

## Development notes

- `BOOKNOW_LIVE_MODE` defaults to `false`. The trade executor logs
  intended orders without sending them; flip to `true` only after
  paper-trade validation.
- Every Binance caller checks `RateLimitGuard.is_banned()` first and
  reports back via `report_if_banned(exc)` on failure, so a single
  ban halts every task until the cool-down elapses.
