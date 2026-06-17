#!/usr/bin/env bash
#
# signal_autobuy.sh — Signal auto-buy LIVE gate toggle + status
# ─────────────────────────────────────────────────────────────────────────────
# Flips ONLY `signalAutoBuyLiveEnabled`. It first GETs the current config so the
# validation-required fields (autoBuyEnabled, buyAmountUsdt, profitAmountUsdt)
# are preserved at their CURRENT values — never hard-coded, never reverted.
#
# Usage:
#   ./signal_autobuy.sh on       # enable LIVE real-money buys
#   ./signal_autobuy.sh off       # disable (stop buying; paper/idle)
#   ./signal_autobuy.sh status    # show current signal-autobuy config
#
# NOTE: `on` makes the bot place REAL MARKET BUY orders (5.05 USDT/coin).
set -euo pipefail

API="${BOOKNOW_API:-https://booknow-bogoai.duckdns.org}/api/v1/config"

get_cfg() { curl -fsS "$API"; }

show_status() {
  get_cfg | python3 -c "import sys,json; c=json.load(sys.stdin)
live=c.get('signalAutoBuyLiveEnabled')
print('  signalAutoBuyLiveEnabled :', 'ON (LIVE real buys)' if live else 'OFF')
print('  signalAutoBuyUsdt        :', c.get('signalAutoBuyUsdt'), 'USDT/coin')
print('  signalAutoBuyMaxPositions:', c.get('signalAutoBuyMaxPositions'))
print('  signalAutoBuy24hMaxPct   :', c.get('signalAutoBuy24hMaxPct'), '%')
print('  signalAutoBuyNoChase     :', c.get('signalAutoBuyNoChase'))
print('  signalAutoBuyMaxAgeSec   :', c.get('signalAutoBuyMaxAgeSec'))"
}

set_live() {
  local want="$1"   # true|false
  # Pull current required fields so we don't clobber them.
  local payload
  payload=$(get_cfg | python3 -c "import sys,json
c=json.load(sys.stdin)
import json as j
print(j.dumps({
  'autoBuyEnabled': bool(c.get('autoBuyEnabled', False)),
  'buyAmountUsdt': c.get('buyAmountUsdt'),
  'profitAmountUsdt': c.get('profitAmountUsdt'),
  'signalAutoBuyLiveEnabled': $want,
}))")
  curl -fsS -X POST "$API" -H "Content-Type: application/json" -d "$payload" \
    | python3 -c "import sys,json; c=json.load(sys.stdin).get('config',{}); print('  -> signalAutoBuyLiveEnabled =', c.get('signalAutoBuyLiveEnabled'))"
}

case "${1:-status}" in
  on|ON)
    echo "Enabling LIVE signal auto-buy (REAL market buys, 5.05 USDT/coin)..."
    set_live true
    echo "Done. Current status:"; show_status ;;
  off|OFF)
    echo "Disabling LIVE signal auto-buy..."
    set_live false
    echo "Done. Current status:"; show_status ;;
  status|"" )
    echo "Signal auto-buy status:"; show_status ;;
  *)
    echo "Usage: $0 {on|off|status}" >&2; exit 1 ;;
esac
