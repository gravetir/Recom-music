import json
import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
import services.globals as globals
from services.update_dataset import update_dataset
import logging
from typing import Dict, List, Any, Tuple, Optional

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def get_updated_data() -> Tuple[pd.DataFrame, np.ndarray, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Обновляем и возвращаем актуальные данные для расчетов"""
    try:
        if globals.dataset_df is None or globals.df_feature_matrix is None:
            logger.info("Data not loaded, updating dataset...")
            update_dataset()
        
        return (
            globals.dataset_df, 
            globals.df_feature_matrix, 
            globals.df_genres, 
            globals.df_moods, 
            globals.df_tags
        )
    except Exception as e:
        logger.error(f"Error getting updated data: {str(e)}")
        raise

def calculate_similarities(
    track_idx: int,
    feature_matrix: np.ndarray,
    genres_df: pd.DataFrame,
    tags_df: pd.DataFrame,
    moods_df: pd.DataFrame,
    mfcc_weight: float = 0.2,
    genre_weight: float = 0.3,
    tag_weight: float = 0.3,
    mood_weight: float = 0.2
) -> np.ndarray:
    """Вычисляем меру схожести треков"""
    mfcc_sim = cosine_similarity(
        feature_matrix[track_idx].reshape(1, -1), 
        feature_matrix
    )[0] * mfcc_weight

    genre_sim = cosine_similarity(
        genres_df.values[track_idx].reshape(1, -1),
        genres_df.values
    )[0] * genre_weight

    tag_sim = cosine_similarity(
        tags_df.values[track_idx].reshape(1, -1),
        tags_df.values
    )[0] * tag_weight

    mood_sim = cosine_similarity(
        moods_df.values[track_idx].reshape(1, -1),
        moods_df.values
    )[0] * mood_weight

    return mfcc_sim + genre_sim + tag_sim + mood_sim

def prepare_track_response(track: pd.Series) -> Dict[str, Any]:
    """Подготавливаем данные трека для ответа API"""
    return {
        "beat_id": str(track['beat_id']),
        "file": track.get("file", ""),
        "url": track.get("url", ""),
        "price": float(track.get("price", 0.0)),
        "picture": str(track.get("picture", "")),
        "timestamps": track.get("timestamps", [])
    }

def prepare_full_track_data(track: pd.Series) -> Dict[str, Any]:
    """Подготавливаем полные данные трека"""
    response = prepare_track_response(track)
    response.update({
        # "beat_id": str(track['beat_id']),
        # "file": track.get("file", ""),
        # "url": track.get("url", ""),
        # "price": float(track.get("price", 0.0)),
        # "picture": str(track.get("picture", "")),
        # "timestamps": track.get("timestamps", []),
        "genres": track.get('genre_ids', '').split(','),
        "moods": track.get('mood_ids', '').split(','),
        "tags": track.get('tag_ids', '').split(',')
    })
    return response

def find_similar_tracks(
    track_id: str,
    top_n: int = 10,
    mfcc_weight: float = 0.2,
    genre_weight: float = 0.3,
    tag_weight: float = 0.3,
    mood_weight: float = 0.2,
    return_full_data: bool = False
) -> List[Dict[str, Any]]:
    """
    Находит похожие треки с обновлёнными данными
    
        track_id: ID трека для поиска похожих
        top_n: количество возвращаемых треков
        mfcc_weight: вес MFCC-признаков в схожести
        genre_weight: вес жанров в схожести
        tag_weight: вес тегов в схожести
        mood_weight: вес настроений в схожести
        return_full_data: если True, возвращает полные данные (для кэша)
    
        Список словарей с информацией о похожих треках
    """
    try:
        # Загружаем актуальные данные
        df, feature_matrix, genres_df, tags_df, moods_df = get_updated_data()
        
        logger.info(f"Processing track_id: {track_id}")
        logger.debug(f"Dataset shape: {df.shape}")

        # Проверяем наличие трека
        track_idx_list = df.index[df['beat_id'] == track_id].tolist()
        if not track_idx_list:
            update_dataset()
            df, feature_matrix, genres_df, tags_df, moods_df = get_updated_data()
            track_idx_list = df.index[df['beat_id'] == track_id].tolist()
            if not track_idx_list:
                raise ValueError(f"Track {track_id} not found in dataset")

        track_idx = track_idx_list[0]

        # Вычисляем схожести
        similarities = calculate_similarities(
            track_idx,
            feature_matrix,
            genres_df,
            tags_df,
            moods_df,
            mfcc_weight,
            genre_weight,
            tag_weight,
            mood_weight
        )

        # Получаем топ-N похожих треков (исключая исходный)
        similar_indices = np.argsort(similarities)[::-1]
        similar_indices = [idx for idx in similar_indices if idx != track_idx][:top_n]

        # Формируем результат
        results = []
        for idx in similar_indices:
            track = df.iloc[idx]
            if return_full_data:
                results.append(prepare_full_track_data(track))
            else:
                results.append(prepare_track_response(track))

        logger.info(f"Found {len(results)} similar tracks for track_id {track_id}")
        return results

    except Exception as e:
        logger.error(f"Error in find_similar_tracks: {str(e)}", exc_info=True)
        raise RuntimeError(f"Similarity calculation failed: {str(e)}")