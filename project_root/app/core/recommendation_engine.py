from typing import List, Tuple, Dict, Any
from collections import defaultdict
import numpy as np
import logging
import json 
import pandas as pd
import app.services.globals as globals
from app.core.scoring import TrackScorer
from app.core.preferences2 import UserPreferenceAnalyzer
from app.config import Config

logger = logging.getLogger(__name__)

class SimilarityCalculator:
    """Класс для вычисления косинусного сходства между двумя словарями-векторами"""

    @staticmethod
    def cosine_similarity(vec1: Dict[str, float], vec2: Dict[str, float]) -> float:
        all_keys = set(vec1.keys()).union(vec2.keys())
        v1 = np.array([vec1.get(k, 0) for k in all_keys])
        v2 = np.array([vec2.get(k, 0) for k in all_keys])
        norm1 = np.linalg.norm(v1)
        norm2 = np.linalg.norm(v2)
        if norm1 == 0 or norm2 == 0:
            return 0.0
        return float(np.dot(v1, v2) / (norm1 * norm2))


class RecommendationEngine:
    """Генерация рекомендаций по жанрам или по лайкам"""

    def __init__(self):
        self.scorer = TrackScorer()
        self.similarity = SimilarityCalculator()
        self.preference = UserPreferenceAnalyzer()

        if globals.dataset_df is None:
            logger.error("[Engine] Глобальные данные не загружены")
            raise ValueError("Данные не загружены в глобальные переменные")

        logger.info("[Engine] Инициализация RecommendationEngine")
        self.beats = self._prepare_beats_data()
        logger.info(f"[Engine] Загружено {len(self.beats)} треков")

    def _prepare_beats_data(self) -> List[Dict[str, Any]]:
            import json 

            beats = []

            def safe_parse_ids(ids):
                if ids is None or (isinstance(ids, (float, np.number)) and np.isnan(ids)):
                    return []
                if isinstance(ids, str):
                    clean_str = ids.strip("[]'\" ")
                    if not clean_str:
                        return []
                    if '||' in clean_str:
                        return [x.strip() for x in clean_str.split('||') if x.strip()]
                    elif '|' in clean_str:
                        return [x.strip() for x in clean_str.split('|') if x.strip()]
                    elif ',' in clean_str:
                        return [x.strip() for x in clean_str.split(',') if x.strip()]
                    else:
                        return [clean_str]
                elif isinstance(ids, (list, np.ndarray)):
                    return [str(x).strip() for x in ids if str(x).strip()]
                else:
                    return [str(ids).strip()] if str(ids).strip() else []

            for _, row in globals.dataset_df.iterrows():
                try:
                    # Обработка timestamps
                    timestamps_raw = row.get('timestamps', [])
                    timestamps = []
                    if isinstance(timestamps_raw, str):
                        try:
                            timestamps = json.loads(timestamps_raw)
                        except json.JSONDecodeError:
                            logger.warning(f"[Engine] Невозможно распарсить timestamps у трека {row.get('beat_id')}: {timestamps_raw}")
                    elif isinstance(timestamps_raw, list):
                        timestamps = timestamps_raw
                    else:
                        timestamps = []

                    beat = {
                        "id": str(row['beat_id']),
                        "title": str(row['file']),
                        "genres": safe_parse_ids(row.get('genre_ids')),
                        "tags": safe_parse_ids(row.get('tag_ids')),
                        "moods": safe_parse_ids(row.get('mood_ids')),
                        "timestamps": timestamps,
                        "picture": row['picture'],
                        "price": float(row['price']),
                        "url": row['url'],
                        # "audio_features": {
                        #     "crm": [float(row.get(f'crm{i}', 0)) for i in range(1, 13)],
                        #     "melspectrogram": row.get('melspectrogram', []),
                        #     "spectral_centroid": float(row.get('spectral_centroid', 0)),
                        #     "mfcc": [float(row.get(f'mfcc{i}', 0)) for i in range(1, 51)]
                        # }
                    }
                    beats.append(beat)

                    logger.debug(f"Processed beat: {beat['id']}")
                    logger.debug(f"Genres: {beat['genres']}")
                    logger.debug(f"Tags: {beat['tags']}")
                    logger.debug(f"Moods: {beat['moods']}")

                except Exception as e:
                    logger.error(f"Error processing beat {row.get('beat_id')}: {str(e)}")
                    continue

            return beats

    def alternate_genres(self, tracks: List[Tuple], preferred_genres: List[str]) -> List[Tuple]:
        logger.debug("[Engine] Перемешивание по жанрам")
        genre_map = {genre: [] for genre in preferred_genres}
        for track in tracks:
            for genre in preferred_genres:
                if genre in track[2]:
                    genre_map[genre].append(track)
                    break

        alternated = []
        max_len = max(len(lst) for lst in genre_map.values()) if genre_map else 0
        for i in range(max_len):
            for genre in preferred_genres:
                if i < len(genre_map[genre]):
                    alternated.append(genre_map[genre][i])

        logger.debug(f"[Engine] После перемешивания {len(alternated)} треков")
        return alternated

    def generate_recommendations_by_genres(self, genres: List[str]) -> List[Tuple]:
        logger.info(f"[Engine] Генерация по жанрам: {genres}")
        if len(genres) < Config.MIN_GENRES or len(genres) > Config.MAX_GENRES:
            raise ValueError(f"Количество жанров должно быть от {Config.MIN_GENRES} до {Config.MAX_GENRES}")

        genre_vec = {genre: 1 / len(genres) for genre in genres}
        tag_scores = defaultdict(float)
        mood_scores = defaultdict(float)
        print(self.beats)

        for beat in self.beats:
            beat_vec = {
                "genres": {g: 1 / len(beat["genres"]) for g in beat["genres"]} if beat["genres"] else {},
                "tags": {t: 1 / len(beat["tags"]) for t in beat["tags"]} if beat["tags"] else {},
                "moods": {m: 1 / len(beat["moods"]) for m in beat["moods"]} if beat["moods"] else {}
            }
            tag_sim = self.similarity.cosine_similarity(genre_vec, beat_vec["tags"])
            mood_sim = self.similarity.cosine_similarity(genre_vec, beat_vec["moods"])

            for tag, val in beat_vec["tags"].items():
                tag_scores[tag] += val * tag_sim
            for mood, val in beat_vec["moods"].items():
                mood_scores[mood] += val * mood_sim

        scored_tracks = [
            (
                beat["id"],
                beat["title"],
                beat["genres"],
                beat["tags"],
                beat["moods"],
                self.scorer.calculate_score(beat["id"], genre_vec, tag_scores, mood_scores)
            ) for beat in self.beats
        ]
        scored_tracks.sort(key=lambda x: x[5], reverse=True)
        logger.info(f"[Engine] Отсортировано {len(scored_tracks)} треков, лучшие: {[s[5] for s in scored_tracks[:5]]}")

        alternated = self.alternate_genres(scored_tracks, genres)
        return alternated[:Config.BATCH_SIZE]

    def generate_recommendations_by_likes(self, liked_ids: List[int], count: int = Config.REFILL_COUNT) -> List[Tuple]:
        logger.info(f"[Engine] Генерация по лайкам: {liked_ids}")
        genre_v, tag_v, mood_v = self.preference.analyze_preferences(liked_ids)
        logger.debug(f"[Engine] Вектора предпочтений: genre={genre_v}, tag={tag_v}, mood={mood_v}")

        candidates = [
            (
                beat["id"],
                beat["title"],
                beat["genres"],
                beat["tags"],
                beat["moods"],
                self.scorer.calculate_score(beat["id"], genre_v, tag_v, mood_v)
            )
            for beat in self.beats if beat["id"] not in liked_ids
        ]

        candidates.sort(key=lambda x: x[5], reverse=True)
        logger.info(f"[Engine] Отобрано {len(candidates)} кандидатов, лучшие: {[s[5] for s in candidates[:5]]}")
        return candidates[:count]
