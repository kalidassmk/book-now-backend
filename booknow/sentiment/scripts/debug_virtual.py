import redis
import json

r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)

try:
    analysis = r.hgetall('ANALYSIS_020_TIMELINE')
    print(f"Total coins in analysis: {len(analysis)}")
    for sym, data in analysis.items():
        timeline = json.loads(data)
        if timeline:
            last = timeline[-1]
            print(f"{sym}: last signal {last.get('micro_signal')}, price {last.get('price')}")
    
    positions = r.hgetall('VIRTUAL_POSITIONS:MICRO')
    print(f"\nActive Virtual Positions: {len(positions)}")
    for sym, pos in positions.items():
        print(f" - {sym}: {pos}")
        
    history = r.lrange('VIRTUAL_HISTORY:MICRO', 0, 5)
    print(f"\nRecent Virtual History: {len(history)}")
    for h in history:
        print(f" - {h}")

except Exception as e:
    print(f"Error: {e}")
