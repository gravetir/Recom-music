import threading
import time
from collections import defaultdict

class RecommendationStorage:
    def __init__(self, refill_threshold=10):
        self.user_recommendations = defaultdict(list)
        self.pending_refills = dict()  # user_id: timestamp
        self.lock = threading.Lock()
        self.last_cleanup = time.time()
        self.refill_threshold = refill_threshold

    def get_recommendations(self, user_id):
        with self.lock:
            return list(self.user_recommendations.get(user_id, []))

    def add_recommendation(self, user_id, beat):
        with self.lock:
            self.user_recommendations[user_id].append(beat)
            if (user_id in self.pending_refills and 
                len(self.user_recommendations[user_id]) >= self.refill_threshold):
                del self.pending_refills[user_id]

    def pop_recommendations(self, user_id, count):
        with self.lock:
            recs = self.user_recommendations.get(user_id, [])
            result = recs[:count]
            self.user_recommendations[user_id] = recs[count:]
            return result

    def should_request_refill(self, user_id, refill_timeout):
        now = time.time()
        with self.lock:
            if user_id in self.pending_refills:
                if now - self.pending_refills[user_id] < refill_timeout:
                    return False
            self.pending_refills[user_id] = now
            return True

    def cleanup(self, refill_timeout, full_cleanup_interval):
        now = time.time()
        with self.lock:
            # Очистка устаревших запросов на пополнение
            self.pending_refills = {
                uid: ts for uid, ts in self.pending_refills.items()
                if now - ts < refill_timeout
            }
            # Полная очистка хранилища по расписанию
            if now - self.last_cleanup > full_cleanup_interval:
                self.last_cleanup = now
                self.user_recommendations = defaultdict(
                    list,
                    {k: v for k, v in self.user_recommendations.items() if v}
                )
