import redis
import json

r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
keys = r.keys("*")
for k in sorted(keys):
    print(k)
