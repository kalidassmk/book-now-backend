#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════╗
║             UTILITY & SCANNING MASTER  -  Intelligence Stack             ║
║  ----------------------------------------------------------------------  ║
║  Manages background discovery, market scanning, and deep-analysis tools.  ║
║  Handles automatic looping for single-shot scripts.                      ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

import subprocess
import time
import os
import signal
import sys
import threading
from queue import Queue, Empty
from datetime import datetime

# ─── SETUP TASKS (Run once at startup) ───────────────────────────────────────
SETUP_TASKS = [
    ("Fee Intelligence", ".", "fee_calculator_util.py"),
]

# ─── CONFIGURATION (Long running or looping) ──────────────────────────────────
# Format: (name, folder, script_with_args, interval_seconds_if_loop)
# interval = 0 means the script is persistent (has its own loop)
UTILITIES = [
    ("Symbol Sync",        ".", "sync_symbols.py",                      3600), # Every 1h
    ("Market Scanner",     ".", "market_sentiment_engine.py",           0),    # Persistent
    # Long-running WebSocket-backed kline cache + 10-min internal scan loop.
    # Replaces the previous "spawn `--scan` every 600s" subprocess pattern,
    # which made up to ~160 REST kline fetches per scan (~23k/day). The
    # daemon now keeps a persistent multiplexed WS connection and reads the
    # buffer; ~99% of those REST calls disappear.
    ("Fast Move Analyzer", ".", "volume_price_analyzer.py --daemon",     0),    # Persistent
    ("Fast Scalper",       ".", "ultra_fast_scalper.py",                0),    # Persistent
    ("Profit Analyzer",     ".", "profit_reached_analyzer.py",           0),    # Persistent
    ("Pattern Recorder",    ".", "success_pattern_recorder.py",          0),    # Persistent
    ("Pattern Matcher",     ".", "pattern_matching_engine.py",           0),    # Persistent
    ("Profit Trend",        ".", "profit_020_trend_analyzer.py",         0),    # Persistent
    ("Virtual Scalper",     ".", "virtual_scalp_executor.py",            0),    # Persistent
]

PYTHON_PATH = os.path.abspath("../venv313/bin/python3")
processes = {} # name -> process_obj
last_run = {}  # name -> timestamp

def enqueue_output(out, queue, name):
    for line in iter(out.readline, ''):
        queue.put((name, line))
    out.close()

def run_utility(name, folder, command_str):
    cwd = os.path.join(os.getcwd(), folder)
    cmd = [PYTHON_PATH] + command_str.split()
    
    p = subprocess.Popen(
        cmd,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )
    return p

def signal_handler(sig, frame):
    print("\n\n\033[93m🛑 [SHUTDOWN] Terminating all utility processes...\033[0m")
    for name, p in processes.items():
        if p.poll() is None:
            print(f"   - Stopping {name}...")
            p.terminate()
    print("\033[92m✨ Utility stack stopped. Goodbye!\033[0m")
    sys.exit(0)

def main():
    signal.signal(signal.SIGINT, signal_handler)
    q = Queue()
    
    print("\033[94m🚀 Starting Utility & Scanning Master Stack...\033[0m")
    print("-" * 65)

    # --- Run Setup Tasks First ---
    for name, folder, cmd in SETUP_TASKS:
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] 🛠️ [SETUP] {name:20} -> {cmd}")
        cwd = os.path.join(os.getcwd(), folder)
        full_cmd = [PYTHON_PATH] + cmd.split()
        subprocess.run(full_cmd, cwd=cwd)
    print("-" * 65)

    try:
        while True:
            now = time.time()
            
            for name, folder, cmd, interval in UTILITIES:
                should_run = False
                
                # Case A: Persistent process that died
                if interval == 0:
                    if name not in processes or processes[name].poll() is not None:
                        should_run = True
                
                # Case B: Scheduled process
                else:
                    if name not in processes or (processes[name].poll() is not None and (now - last_run.get(name, 0)) >= interval):
                        should_run = True

                if should_run:
                    if name in processes and processes[name].poll() is None:
                        continue # Still running from previous interval

                    timestamp = datetime.now().strftime("%H:%M:%S")
                    print(f"[{timestamp}] 📦 [LAUNCH] {name:20} -> {cmd}")
                    
                    p = run_utility(name, folder, cmd)
                    processes[name] = p
                    last_run[name] = now
                    
                    # Thread to capture logs
                    t = threading.Thread(target=enqueue_output, args=(p.stdout, q, name))
                    t.daemon = True
                    t.start()

            # Process output queue
            try:
                while True:
                    name, line = q.get_nowait()
                    # Filter for meaningful logs (errors, connections, or trading activity)
                    line_up = line.upper()
                    if any(kw in line_up for kw in [
                        "ERROR", "EXCEPTION", "❌", "✅", "✨", "🚀", "🔗", "📊", "🛒", "⚡", "💰", "🛡️",
                        "INITIALIZING", "INITIALIZED", "CONNECTED", "SCAN COMPLETE", "SYNC COMPLETE",
                        "BUYING", "SELLING", "PROFIT", "LOSS", "EXIT", "TREND", "REVERSAL"
                    ]):
                        print(f"[\033[95m{name}\033[0m] {line.strip()}")
            except Empty:
                pass

            time.sleep(1)

    except KeyboardInterrupt:
        signal_handler(None, None)

if __name__ == "__main__":
    main()
