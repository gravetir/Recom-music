from collections import defaultdict
from typing import Dict, List, Tuple
import app.services.globals as globals

class UserPreferenceAnalyzer:
    """
    Анализ предпочтений пользователя на основе лайкнутых треков.
    Использует глобальные данные из globals.dataset_df.
    """
    @staticmethod
    def analyze_preferences(liked_ids: List[int]) -> Tuple[Dict[str, float], Dict[str, float], Dict[str, float]]:
        if globals.dataset_df is None:
            raise ValueError("Данные не загружены в глобальные переменные")
        
        genre_counts = defaultdict(int)
        tag_counts = defaultdict(int)
        mood_counts = defaultdict(int)

        # Проходим по всему датасету, ищем лайкнутые треки и считаем категории
        for _, row in globals.dataset_df.iterrows():
            if row['beat_id'] in liked_ids:
                genres = row['genre_ids'].split('||') if isinstance(row['genre_ids'], str) else []
                for genre in genres:
                    genre_counts[genre.strip()] += 1
                
                tags = row['tag_ids'].split('||') if isinstance(row['tag_ids'], str) else []
                for tag in tags:
                    tag_counts[tag.strip()] += 1
                
                moods = row['mood_ids'].split('||') if isinstance(row['mood_ids'], str) else []
                for mood in moods:
                    mood_counts[mood.strip()] += 1

        total = len(liked_ids) or 1  

        # Нормализация по количеству лайков 
        genre_vector = {k: v / total for k, v in genre_counts.items()}
        tag_vector = {k: v / total for k, v in tag_counts.items()}
        mood_vector = {k: v / total for k, v in mood_counts.items()}

        return genre_vector, tag_vector, mood_vector
