import redis
import json
from datetime import timedelta

class RedisCache:
    def __init__(self):
        self.r = redis.Redis(
            host='localhost',
            port=6379,
            db=0,
            decode_responses=True
        )
        self.default_ttl = timedelta(weeks=1)

    def get_similar_tracks(self, track_id):
        key = f"similar_tracks:{track_id}"
        cached_data = self.r.get(key)
        return json.loads(cached_data) if cached_data else None

    def set_similar_tracks(self, track_id, data):
        key = f"similar_tracks:{track_id}"
        self.r.setex(name=key, time=self.default_ttl, value=json.dumps(data))

    # def delete_similar_tracks(self, track_id):
    #     key = f"similar_tracks:{track_id}"
    #     self.r.delete(key)

# Экземпляр класса RedisCache
redis_cache = RedisCache()
