"""
routes_diag.py — iter 79 (2026-05-24)
─────────────────────────────────────────────────────────────────────────────
Diagnostic endpoints: log file listing + log tail.

Every subprocess (Fast Scalper, Virtual Scalper, PumpRider, VSP, LMC, CCP,
Market Scanner, etc.) writes to /logs/sentiment/<name>.log inside the
backend container.  These endpoints expose those files to the dashboard
so the operator can monitor activity without SSH-ing into the box.

Endpoints:
  GET /api/v1/diag/logs/list
      → list of available log file names + sizes + mtimes

  GET /api/v1/diag/logs/tail?source=fast_scalper&lines=200&grep=PATTERN
      → last N lines of source.log, optionally filtered by regex
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/api/v1/diag", tags=["diagnostics"])

LOGS_ROOT = Path(os.environ.get("BOOKNOW_LOGS_DIR", "/logs"))
# Search both /logs and /logs/sentiment for robustness.
SEARCH_DIRS = [LOGS_ROOT, LOGS_ROOT / "sentiment"]
MAX_LINES = 5000
MAX_TAIL_BYTES = 2_000_000  # cap how much of the file we read for tail


@router.get("/logs/list")
def list_logs() -> dict:
    """Return all .log files with size + mtime.  Sorted by mtime DESC."""
    out = []
    seen = set()
    for d in SEARCH_DIRS:
        if not d.exists():
            continue
        for p in d.glob("*.log"):
            if p.name in seen:
                continue
            seen.add(p.name)
            try:
                st = p.stat()
                out.append({
                    "source": p.stem,            # filename without .log
                    "file": p.name,
                    "path": str(p),
                    "size_bytes": st.st_size,
                    "mtime_ms": int(st.st_mtime * 1000),
                })
            except Exception:
                continue
    out.sort(key=lambda x: x.get("mtime_ms", 0), reverse=True)
    return {"logs": out, "count": len(out)}


@router.get("/logs/tail")
def tail_log(
    source: str = Query(..., description="log file stem, e.g. fast_scalper"),
    lines: int = Query(200, ge=1, le=MAX_LINES),
    grep: Optional[str] = Query(None, description="optional regex filter (case-insensitive)"),
) -> dict:
    """Return the last `lines` lines of <source>.log, optionally grep'd."""
    # Locate file (try both search dirs)
    target: Optional[Path] = None
    safe = re.sub(r"[^a-zA-Z0-9_.\-]", "", source)
    for d in SEARCH_DIRS:
        cand = d / f"{safe}.log"
        if cand.exists() and cand.is_file():
            target = cand
            break
    if target is None:
        raise HTTPException(
            status_code=404,
            detail=f"log file not found: {safe}.log (searched {[str(d) for d in SEARCH_DIRS]})",
        )

    try:
        size = target.stat().st_size
        with open(target, "rb") as f:
            # Read just the tail to keep latency low on big files.
            start = max(0, size - MAX_TAIL_BYTES)
            f.seek(start)
            data = f.read()
        text = data.decode("utf-8", errors="ignore")
        # Discard the first (likely partial) line if we seeked past byte 0
        rows = text.splitlines()
        if start > 0 and rows:
            rows = rows[1:]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"read failed: {exc}")

    if grep:
        try:
            pat = re.compile(grep, re.IGNORECASE)
            rows = [r for r in rows if pat.search(r)]
        except re.error as exc:
            raise HTTPException(status_code=400, detail=f"bad regex: {exc}")

    rows = rows[-lines:]
    return {
        "source": safe,
        "path": str(target),
        "size_bytes": size,
        "returned_lines": len(rows),
        "filter": grep or None,
        "lines": rows,
    }


# ─────────────────────────────────────────────────────────────────────────
# iter 79 — Backend restart endpoint.
#
# Docker Compose has `restart: unless-stopped` on the backend service, so
# when the process exits, Docker restarts it automatically.  This endpoint
# schedules a graceful shutdown after ~2 seconds (enough time to return
# the HTTP response).  No docker.sock or SSH needed.
# ─────────────────────────────────────────────────────────────────────────

@router.post("/restart")
def restart_backend() -> dict:
    """Schedule a process exit so Docker auto-restarts the backend.
    Returns immediately; backend exits ~2 seconds later.
    """
    import asyncio
    import os
    import signal

    async def delayed_kill():
        await asyncio.sleep(2.0)
        # SIGTERM first (graceful), then docker compose policy restarts us
        try:
            os.kill(os.getpid(), signal.SIGTERM)
        except Exception:
            os._exit(0)

    # Schedule on the running event loop
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(delayed_kill())
    except RuntimeError:
        # No loop — last resort, just exit shortly via a thread
        import threading, time as _t
        def _kill():
            _t.sleep(2.0)
            os._exit(0)
        threading.Thread(target=_kill, daemon=True).start()

    return {
        "ok": True,
        "message": "Backend will restart in ~2 seconds (Docker auto-restart policy).",
        "pid": os.getpid(),
    }
