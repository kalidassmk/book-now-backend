"""
stale_cleaner
─────────────────────────────────────────────────────────────────────────────
Periodic surgical cleanup of the operational Redis. Walks the WATCH_ALL
hash, finds symbols whose last WS update is older than the staleness
threshold, and deletes their stale entries (WATCH_ALL row, base price,
fast-move counter, bucket assignments).

Designed as a less-disruptive alternative to a full FLUSHALL refresh:
trading positions, configs, and symbol metadata are never touched —
only "the symbol stopped ticking, drop its market-state cruft" gets
cleaned up.
"""
