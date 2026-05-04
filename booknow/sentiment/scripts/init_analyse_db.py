import redis
import sys

def init_db():
    print("🚀 Initializing Remote Redis Cloud for Success Patterns (AnalyseDB)...")
    
    # Remote Redis Cloud Configuration
    REMOTE_REDIS = {
        'host': 'redis-18144.c89.us-east-1-3.ec2.cloud.redislabs.com',
        'port': 18144,
        'password': 'Gn9jKtL0SBkMLYynSjXbblmkjkIGrdPS',
        'decode_responses': True
    }
    
    try:
        r = redis.Redis(**REMOTE_REDIS)
        r.ping()
        
        print(f"✅ Connected to Remote Redis Cloud at {REMOTE_REDIS['host']}")
        
        # We don't need to 'create' a database in Redis, we just ensure it's accessible.
        # However, we can set some metadata or clear it if requested.
        
        key_count = len(r.keys("*"))
        print(f"📊 Current patterns in DB {ANALYSE_DB_INDEX}: {key_count}")
        
        if key_count > 0:
            confirm = input(f"⚠️ DB {ANALYSE_DB_INDEX} already contains {key_count} keys. Do you want to clear it? (y/n): ")
            if confirm.lower() == 'y':
                r.flushdb()
                print(f"🧹 DB {ANALYSE_DB_INDEX} cleared successfully.")
        
        print(f"\n✨ AnalyseDB is ready at Redis Index {ANALYSE_DB_INDEX}!")
        print("Note: Success Pattern Recorder and Matcher will now be configured to use this database.")

    except Exception as e:
        print(f"❌ Error initializing DB: {e}")
        sys.exit(1)

if __name__ == "__main__":
    init_db()
