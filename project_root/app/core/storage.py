from collections import defaultdict
from typing import Dict, List, Any, Set
import time

class RecommendationStorage:
    """
    Хранилище состояния — рекомендации, лайки, жанры, обработанные оффсеты и т.п.
    """
    def __init__(self):
        self.user_recommendations: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.direct_recommendations: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.user_likes: Dict[str, List[int]] = defaultdict(list)
        self.user_genres: Dict[str, List[str]] = defaultdict(list)
        self.processed_offsets: Dict[str, int] = {}
        self.pending_refills: Set[str] = set()
        self.last_refill_time: Dict[str, float] = {}
        self.MAX_RECOMMENDATIONS = 200

    def add_recommendation(self, user_id: str, beat: Dict[str, Any]):
        existing_ids = [b["id"] for b in self.user_recommendations[user_id]]
        if beat["id"] in existing_ids:
            return

        self.user_recommendations[user_id].append(beat)

        if len(self.user_recommendations[user_id]) > self.MAX_RECOMMENDATIONS:
            self.user_recommendations[user_id] = self.user_recommendations[user_id][-self.MAX_RECOMMENDATIONS:]

    def should_refill(self, user_id: str, threshold: int, cooldown: int) -> bool:
        now = time.time()
        if user_id in self.pending_refills:
            return False
        if now - self.last_refill_time.get(user_id, 0) < cooldown:
            return False

        total_recs = len(self.user_recommendations[user_id]) + len(self.direct_recommendations[user_id])
        return total_recs < threshold * 3

    def mark_refill_requested(self, user_id: str):
        self.pending_refills.add(user_id)
        self.last_refill_time[user_id] = time.time()

    def clear_recommendations(self, user_id: str):
        self.user_recommendations[user_id] = []
        self.direct_recommendations[user_id] = []
        self.processed_offsets[user_id] = -1
