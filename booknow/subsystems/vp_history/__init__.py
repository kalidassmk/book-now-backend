"""
vp_history
─────────────────────────────────────────────────────────────────────────────
Volume/Price history recorder for fast-scanner-flagged symbols.

Streams (current_price, current_volume) snapshots from the local
WATCH_ALL hash + TickersCache into a separate Redis Cloud database
(``redis-18144.c89.us-east-1-3.ec2.cloud.redislabs.com``) under
sorted-set keys ``vp_history:{SYMBOL}``. Downstream the sell-price
engine reads this history to predict achievable sell prices across
multiple time horizons.

Recording rules (from product spec):
    • Only symbols present in WATCH_ALL (= fast-scanner flagged) are tracked.
    • If current_price drops below base_price, the symbol's history is
      deleted and the symbol is marked stopped. Stop is reversible — if
      price climbs back above base, recording resumes from a fresh base.
    • If a new sample's vol_pct is within EPSILON of the last sample's
      vol_pct, the last entry is overwritten in place (same point,
      fresher timestamp) instead of appended.

This subsystem is read-only against the existing fast-scanner pipeline.
"""
