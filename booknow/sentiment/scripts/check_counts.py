import redis

r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
try:
    count = r.hlen("PROFIT_REACHED_020")
    print(f"Total coins in PROFIT_REACHED_020: {count}")
    keys = r.hkeys("PROFIT_REACHED_020")
    print(f"Samples: {keys[:10]}")
except Exception as e:
    print(f"Error: {e}")
