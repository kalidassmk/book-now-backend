# Paper-Trade Soak

The python-engine is designed to run for days at a time. This directory
holds the harness for verifying that — `scripts/soak.sh` boots the engine
in paper mode, samples once a minute, and prints a final summary.

## Quick start

```bash
# 5-minute smoke (default)
scripts/soak.sh

# 60-minute soak
scripts/soak.sh 60

# 24-hour soak with samples in a dated dir
scripts/soak.sh 1440 ~/booknow_soak_$(date +%Y%m%d)
```

The script writes three artefacts to `LOG_DIR` (default `/tmp/booknow_soak`):

- `engine.log` — the engine's full stdout (also includes uvicorn lines).
- `samples.csv` — one row per minute, headers as below.
- `summary.txt` — final report; printed to stdout too.

## What gets sampled

| Column            | Meaning                                                                         |
| ----------------- | ------------------------------------------------------------------------------- |
| `minute`          | Sample index (1-based, every 60 wall-clock seconds).                            |
| `rss_mb`          | Process resident-set size in MB (`ps -o rss=`). Watch for monotonic growth.    |
| `log_lines`       | `wc -l engine.log`. Sudden bumps usually mean a new error loop.                 |
| `errors`          | Count of `\| ERROR` lines.                                                      |
| `warnings`        | Count of `\| WARNING` lines.                                                    |
| `http_ms`         | Round-trip ms for `GET /api/v1/health`. >100ms = engine struggling.            |
| `r1_hits`         | Times `[rule_one]` logged. Rule-engine activity proxy.                          |
| `r2_hits`         | Times `[rule_two]` logged.                                                      |
| `r3_hits`         | Times `[rule_three]` logged.                                                    |
| `proc_detects`    | `detected\|SUPER_FAST\|ULTRA_FAST` matches. Processor activity.                  |
| `ws_reconnects`   | Times `WS connected` appears. >1 means at least one reconnect happened.        |

## What "healthy" looks like

After ~10 minutes of warmup, on a soak with no IP ban active:

- **RSS:** Plateaus within ~60–80 MB. If it grows linearly past minute 30 you have a leak.
- **Errors:** ~0 per hour outside boot. WARNING is fine — most are
  rate-limit/ban skips that the guard handles cleanly.
- **HTTP:** Sub-50 ms `/api/v1/health` for the entire run.
- **WS reconnects:** 0–2 per hour. Binance occasionally drops streams; the
  klines-cache and ws_streams services both reconnect with exponential backoff.
- **Rules:** Non-zero `r1`/`r2`/`r3` lines if any pair is moving fast
  enough to trip a pattern. They will be 0 in dead market conditions
  — that's a market signal, not an engine bug.
- **Detects:** Should accumulate steadily (hundreds–thousands per hour
  on a normal market).

## What "unhealthy" looks like

- **RSS climbs by ≥20 MB/hour after warmup** → likely an unbounded
  cache or a coroutine that holds references it shouldn't. Look at
  `KlinesCache.release`, `TradeState` size, `analysisCache`.
- **`http_ms` > 200 ms persistently** → the asyncio loop is starved.
  Check for blocking calls outside `run_in_executor`.
- **`ws_reconnects` > 1/min** → upstream connectivity issue or we're
  being throttled. `RateLimitGuard` should be catching the ban path.
- **Errors growing during steady-state** → a real bug. Run
  `grep '| ERROR' engine.log` and triage.

## Running against a real Binance IP ban

The harness is ban-tolerant: REST seeds skip when `RateLimitGuard.is_banned()`,
the trading core falls back to the WS price feed, and the HTTP layer keeps
serving cached state. Expect lots of WARNING lines and zero ERRORs while a
ban is active. This is the right behaviour — see `booknow/binance/rate_limit.py`.

## Extending

The script samples once per minute on purpose: cheap, low overhead. To
go finer-grained or capture additional signals (FD count, asyncio task
count via a debug endpoint), edit the `Sampling loop` block in
`soak.sh`. The CSV header is the contract — add new columns at the end
so existing analyses keep working.
