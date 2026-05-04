#!/usr/bin/env bash
# soak.sh
# ─────────────────────────────────────────────────────────────────────────────
# Paper-trade soak harness for the python-engine.
#
# Boots ``booknow.main`` in paper mode, then samples once a minute:
#   * process RSS (macOS ps / Linux ps; same flag on both)
#   * total log lines, errors, warnings
#   * HTTP /api/v1/health responsiveness (sub-second is healthy)
#   * count of trade-rule firings (R1/R2/R3)
#   * count of processor "detected" events
#   * KlinesCache WS reconnects
#
# Writes per-minute samples + a final summary to LOG_DIR (default /tmp).
# Sends SIGTERM on completion and waits for clean shutdown.
#
# Usage:
#     scripts/soak.sh [DURATION_MINUTES] [LOG_DIR]
#
# Defaults: 5 minutes, LOG_DIR=/tmp/booknow_soak. For a 24-hour run:
#     scripts/soak.sh 1440 ~/booknow_soak_$(date +%Y%m%d)

set -euo pipefail

DURATION_MIN="${1:-5}"
LOG_DIR="${2:-/tmp/booknow_soak}"
HTTP_PORT="${BOOKNOW_HTTP_PORT:-8083}"
ENGINE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON="${BOOKNOW_PYTHON:-/Users/bogoai/Book-Now/venv313/bin/python}"

mkdir -p "$LOG_DIR"
ENGINE_LOG="$LOG_DIR/engine.log"
SAMPLES="$LOG_DIR/samples.csv"
SUMMARY="$LOG_DIR/summary.txt"

echo "soak harness ──────────────────────────────────────────────"
echo "  duration:  ${DURATION_MIN} min"
echo "  log dir:   ${LOG_DIR}"
echo "  engine:    ${ENGINE_DIR}"
echo "  python:    ${PYTHON}"
echo "  http port: ${HTTP_PORT}"

# Bail out fast if the port is already in use.
if lsof -nP -iTCP:"$HTTP_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "ERROR: port $HTTP_PORT already in use" >&2
  exit 1
fi

# ── Boot engine in background ───────────────────────────────────────────────
echo "booting engine…"
(
  cd "$ENGINE_DIR"
  BOOKNOW_LIVE_MODE=false \
  BOOKNOW_SENTIMENT_ENABLED=false \
  BOOKNOW_HTTP_PORT="$HTTP_PORT" \
  PYTHONUNBUFFERED=1 \
  "$PYTHON" -m booknow.main > "$ENGINE_LOG" 2>&1
) &
ENGINE_PID=$!

# Wait up to 90s for the HTTP layer to bind. Cold boots take longer
# than you'd think — the engine seeds caches, opens WS connections,
# and can stall briefly on a Binance ban while RateLimitGuard kicks
# in. 90s leaves headroom for all of that.
boot_ok=false
for i in $(seq 1 180); do
  if curl -fsS "http://127.0.0.1:${HTTP_PORT}/api/v1/health" >/dev/null 2>&1; then
    boot_secs=$(awk -v i="$i" 'BEGIN{ printf "%.1f", i*0.5 }')
    echo "engine up after ${boot_secs}s (PID=$ENGINE_PID)"
    boot_ok=true
    break
  fi
  sleep 0.5
done
if ! $boot_ok; then
  echo "ERROR: engine never became healthy in 90s. Tail of log:" >&2
  tail -40 "$ENGINE_LOG" >&2
  kill -TERM "$ENGINE_PID" 2>/dev/null || true
  # Also kill any orphaned booknow.main process the subshell spawned.
  pkill -TERM -f 'booknow.main' 2>/dev/null || true
  exit 1
fi

# ── Sampling loop ───────────────────────────────────────────────────────────
echo "minute,rss_mb,log_lines,errors,warnings,http_ms,r1_hits,r2_hits,r3_hits,proc_detects,ws_reconnects" > "$SAMPLES"

for m in $(seq 1 "$DURATION_MIN"); do
  # Wait one wall-clock minute (or finish early if engine died).
  for _ in $(seq 1 60); do
    sleep 1
    if ! kill -0 "$ENGINE_PID" 2>/dev/null; then
      echo "ERROR: engine died at minute $m" >&2
      break 2
    fi
  done

  # Process RSS in MB. ps works on both macOS and Linux with the same flags.
  rss_kb=$(ps -o rss= -p "$ENGINE_PID" 2>/dev/null | tr -d ' ' || echo 0)
  rss_mb=$(awk -v k="$rss_kb" 'BEGIN{ printf "%.1f", k/1024 }')

  # Log volume + severity counts.
  log_lines=$(wc -l < "$ENGINE_LOG" | tr -d ' ')
  errors=$(grep -c '| ERROR' "$ENGINE_LOG" 2>/dev/null || echo 0)
  warns=$(grep -c '| WARNING' "$ENGINE_LOG" 2>/dev/null || echo 0)

  # HTTP responsiveness — single request, ms.
  http_ms=$(curl -s -o /dev/null -w '%{time_total}\n' "http://127.0.0.1:${HTTP_PORT}/api/v1/health" \
            | awk '{ printf "%.0f", $1*1000 }')

  # Application-level event counts.
  r1=$(grep -c '\[rule_one\]' "$ENGINE_LOG" 2>/dev/null || echo 0)
  r2=$(grep -c '\[rule_two\]' "$ENGINE_LOG" 2>/dev/null || echo 0)
  r3=$(grep -c '\[rule_three\]' "$ENGINE_LOG" 2>/dev/null || echo 0)
  detects=$(grep -cE 'detected|SUPER_FAST|ULTRA_FAST' "$ENGINE_LOG" 2>/dev/null || echo 0)
  ws_recon=$(grep -c 'WS connected' "$ENGINE_LOG" 2>/dev/null || echo 0)

  echo "$m,$rss_mb,$log_lines,$errors,$warns,$http_ms,$r1,$r2,$r3,$detects,$ws_recon" >> "$SAMPLES"
  printf "  min %3d  rss=%6s MB  errs=%3s  warns=%3s  http=%4sms  detects=%5s  ws_reconn=%2s\n" \
         "$m" "$rss_mb" "$errors" "$warns" "$http_ms" "$detects" "$ws_recon"
done

# ── Shutdown ────────────────────────────────────────────────────────────────
echo "soak window complete — sending SIGTERM to engine PID=$ENGINE_PID"
kill -TERM "$ENGINE_PID" 2>/dev/null || true

# Wait up to 30s for clean shutdown.
for i in $(seq 1 60); do
  if ! kill -0 "$ENGINE_PID" 2>/dev/null; then
    shutdown_secs=$(awk -v i="$i" 'BEGIN{ printf "%.1f", i*0.5 }')
    echo "engine stopped after ${shutdown_secs}s"
    break
  fi
  sleep 0.5
done
if kill -0 "$ENGINE_PID" 2>/dev/null; then
  echo "WARN: engine did not exit in 30s — escalating to SIGKILL"
  kill -KILL "$ENGINE_PID" 2>/dev/null || true
fi

# ── Summary ─────────────────────────────────────────────────────────────────
{
  echo "soak summary ──────────────────────────────────────────────"
  echo "  duration:        ${DURATION_MIN} min"
  echo "  engine log:      ${ENGINE_LOG}"
  echo "  per-min samples: ${SAMPLES}"
  echo
  echo "RSS over time (MB):"
  awk -F, 'NR>1 {print "  min " $1 ": " $2 " MB"}' "$SAMPLES"
  echo
  first_rss=$(awk -F, 'NR==2 {print $2}' "$SAMPLES")
  last_rss=$(awk -F, 'END {print $2}' "$SAMPLES")
  if [[ -n "$first_rss" && -n "$last_rss" ]]; then
    growth=$(awk -v a="$first_rss" -v b="$last_rss" 'BEGIN{ printf "%+.1f", b-a }')
    echo "RSS growth: ${first_rss} MB → ${last_rss} MB (${growth} MB)"
  fi
  echo
  echo "Final counts:"
  awk -F, 'END {
    print "  log lines:        " $3
    print "  errors:           " $4
    print "  warnings:         " $5
    print "  R1/R2/R3 ticks:   " $7 " / " $8 " / " $9
    print "  processor detects:" $10
    print "  WS reconnects:    " $11
  }' "$SAMPLES"
  echo
  echo "Last 5 ERROR lines:"
  grep '| ERROR' "$ENGINE_LOG" | tail -5 | sed 's/^/  /'
} | tee "$SUMMARY"
