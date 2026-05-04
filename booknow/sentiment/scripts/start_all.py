import subprocess
import time
import os
import signal
import sys
import threading
from queue import Queue, Empty

# Try to import redis for cleanup, but don't crash if missing
try:
    import redis
    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False

# List of algorithms to run
# Format: (name, directory, command)
ALGORITHMS = [
    ("Symbol Discovery Engine", ".", "symbol_discovery_engine.py"),
    ("Regime Trader", "regime_trader", "engine.py"),
    ("OBI Trader", "obi_trader", "engine.py"),
    ("Funding/OI Trader", "funding_oi_trader", "main.py"),
    ("Volume Profile Trader", "volume_profile_trader", "main.py"),
    ("Trend Alignment Engine", "trend_alignment_engine", "main.py"),
    ("Fakeout Detector", "fakeout_detector_system", "main.py"),
    ("Meta-Model System", "meta_model_system", "main.py"),
    ("Risk Management Engine", "risk_management_engine", "main.py"),
    ("BTC Correlation Filter", "btc_correlation_filter", "main.py"),
    ("Consensus Engine", ".", "consensus_engine.py"),
    ("Profit Reached Analyzer", ".", "profit_reached_analyzer.py"),
    ("Success Pattern Recorder", ".", "success_pattern_recorder.py"),
    ("Profit 0.20 Trend Analyzer", ".", "profit_020_trend_analyzer.py"),
]

REDIS_KEYS_TO_CLEAR = [
    "REGIME_STATE", "OBI_STATE", "FUNDING_OI_SIGNALS", 
    "VOLUME_PROFILE_SIGNALS", "TREND_ALIGNMENT_SIGNALS", 
    "FAKEOUT_SIGNALS", "META_MODEL_PREDICTIONS", 
    "FINAL_CONSENSUS_STATE", "RISK_STATE", "BTC_CORRELATION_FILTERS",
    "SYMBOLS:ACTIVE", "SYMBOLS:OBI", "SYMBOLS:METADATA"
]

processes = []

def clear_redis():
    """Clears existing trading state from Redis for a fresh start."""
    if not HAS_REDIS:
        print("\033[93m⚠️  [CLEANUP SKIPPED] 'redis' module not found in this Python environment.\033[0m")
        print("\033[90m   To enable cleanup, use: ../venv313/bin/python3 start_all.py\033[0m")
        return

    try:
        r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
        print("\033[94m🧹 [CLEANUP] Clearing stale state from Redis...\033[0m")
        count = 0
        for key in REDIS_KEYS_TO_CLEAR:
            if r.delete(key):
                count += 1
        print(f"\033[92m✨ [CLEANUP] Removed {count} state keys. Starting fresh!\033[0m")
    except Exception as e:
        print(f"\033[93m⚠️  [CLEANUP WARNING] Could not clear Redis: {e}\033[0m")

def enqueue_output(out, queue, name):
    for line in iter(out.readline, ''):
        queue.put((name, line))
    out.close()

def start_algorithms():
    # Step 0: Clear Redis
    clear_redis()
    
    print("\033[94m🚀 Starting Adaptive Market-Behavior Sentiment Engine Stack...\033[0m")
    print("-" * 60)
    
    python_path = os.path.abspath("../venv313/bin/python3")
    q = Queue()
    
    for name, folder, script in ALGORITHMS:
        cwd = os.path.join(os.getcwd(), folder)
        if not os.path.exists(cwd):
            print(f"\033[93m⚠️  [CONFIGURATION ERROR] Skipping {name}: Directory '{folder}' was not found at {cwd}\033[0m")
            continue
            
        script_path = os.path.join(cwd, script)
        if not os.path.exists(script_path):
             print(f"\033[91m❌ [FILE ERROR] Skipping {name}: Entry point '{script}' not found in {folder}\033[0m")
             continue

        print(f"📦 [INITIALIZING] {name:25} -> {folder}/{script}")
        
        # Start process with captured output
        p = subprocess.Popen(
            [python_path, script],
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        processes.append((name, p))
        
        # Thread to read output without blocking
        t = threading.Thread(target=enqueue_output, args=(p.stdout, q, name))
        t.daemon = True
        t.start()
        
        time.sleep(0.5)

    print("-" * 60)
    print(f"\033[92m✅ SUCCESS: {len(processes)} algorithms are now running in the background.\033[0m")
    print("\033[90mMonitoring active processes. Press Ctrl+C to safely shut down the stack.\033[0m")
    print("-" * 60)

    return q

def signal_handler(sig, frame):
    print("\n\n\033[93m🛑 [SHUTDOWN] Signal received. Terminating all active algorithms...\033[0m")
    for name, p in processes:
        if p.poll() is None:
            print(f"   - Stopping {name}...")
            p.terminate()
    print("\033[92m✨ All processes terminated. Goodbye!\033[0m")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    output_queue = start_algorithms()
    
    try:
        while True:
            # Check for crashed processes
            for name, p in processes:
                exit_code = p.poll()
                if exit_code is not None:
                    if exit_code != 0:
                        print(f"\n\033[91m🔥 [CRITICAL FAILURE] {name} has crashed with code {exit_code}!\033[0m")
                    else:
                        print(f"\n\033[90mℹ️  [INFO] {name} has finished execution.\033[0m")
                    processes.remove((name, p))

            # Print important logs
            try:
                name, line = output_queue.get_nowait()
                if any(kw in line.upper() for kw in ["ERROR", "EXCEPTION", "SIGNAL", "CRITICAL", "PREDICTION", "TRADE", "PROB", "BRAIN", "🧠"]):
                    print(f"[\033[95m{name}\033[0m] {line.strip()}")
            except Empty:
                pass
                
            time.sleep(0.01)
    except KeyboardInterrupt:
        signal_handler(None, None)
