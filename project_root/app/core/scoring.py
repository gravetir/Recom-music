from typing import Dict
from app.services import globals

class TrackScorer:
    """
    Класс для оценки треков по весам жанров, тегов и настроений
    """
    def __init__(self, genre_weight: float = 0.6, tag_weight: float = 0.3, mood_weight: float = 0.1):
        self.genre_weight = genre_weight
        self.tag_weight = tag_weight
        self.mood_weight = mood_weight
        self.penalty = 0.5  # штраф за неподходящие жанры

    def calculate_score(self, track_id: int, 
                        genre_weights: Dict[str, float],
                        tag_weights: Dict[str, float],
                        mood_weights: Dict[str, float]) -> float:
        if globals.dataset_df is None:
            raise ValueError("Данные не загружены в глобальные переменные")
        
        # Поиск строки трека по ID
        track_row = globals.dataset_df[globals.dataset_df['beat_id'] == track_id]
        if track_row.empty:
            return 0.0
        
        row = track_row.iloc[0]
        
        # Получаем жанры, теги и настроения 
        genres = row['genre_ids'].split('||') if isinstance(row['genre_ids'], str) else []
        tags = row['tag_ids'].split('||') if isinstance(row['tag_ids'], str) else []
        moods = row['mood_ids'].split('||') if isinstance(row['mood_ids'], str) else []

        # Вычисляем сумму весов для каждого типа признаков
        genre_score = sum(genre_weights.get(g.strip(), 0) for g in genres)
        tag_score = sum(tag_weights.get(t.strip(), 0) for t in tags)
        mood_score = sum(mood_weights.get(m.strip(), 0) for m in moods)

        # Применяем штраф за неподходящие жанры
        extra_genres = [g for g in genres if g.strip() not in genre_weights]
        if extra_genres:
            genre_score *= self.penalty ** len(extra_genres)

        # Нормализация по сумме весов
        genre_norm = genre_score / (sum(genre_weights.values()) or 1)
        tag_norm = tag_score / (sum(tag_weights.values()) or 1)
        mood_norm = mood_score / (sum(mood_weights.values()) or 1)

        # Итоговый скор с учетом весов
        final_score = (
            self.genre_weight * genre_norm +
            self.tag_weight * tag_norm +
            self.mood_weight * mood_norm
        )
        return final_score
