from infrastructure.redis_cache import redis_cache
from services.similarity_service import find_similar_tracks
import services.globals as globals
from services.update_dataset import update_dataset
from typing import List, Dict, Any
import pandas as pd
import logging

logger = logging.getLogger(__name__)
# def _map_ids_to_names(id_list: List[str], lookup_df: pd.DataFrame, id_col: str = "id") -> List[str]:
#     """Преобразуем список ID в список названий"""
#     if lookup_df is None or not isinstance(lookup_df, pd.DataFrame) or lookup_df.empty:
#         logger.warning("Lookup DataFrame is not available")
#         return []
    
#     try:
#         id_set = set(id_list)
#         return lookup_df[lookup_df[id_col].astype(str).isin(id_set)]['name'].tolist()
#     except Exception as e:
#         logger.error(f"Error mapping IDs to names: {str(e)}")
#         return []

def get_similar_tracks_use_case(track_id: str, top_n: int) -> List[Dict[str, Any]]:
    """
    Получаем похожие треки для заданного track_id
    
    Args:
        track_id: UUID трека в строковом формате
        top_n: количество возвращаемых треков
        
    Returns:
        Список словарей с информацией о похожих треках (без genres/moods/tags)
    """
    try:
        # Проверяем и загружаем данные
        if (globals.dataset_df is None or 
            globals.df_feature_matrix is None or 
            globals.df_genres is None or 
            globals.df_moods is None or 
            globals.df_tags is None):
            
            logger.info("Initial dataset load...")
            if not update_dataset():
                logger.error("Initial dataset load failed")
                raise RuntimeError("Could not load dataset")
        
        # Проверяем что данные не пустые
        if (not isinstance(globals.dataset_df, pd.DataFrame) or 
            globals.dataset_df.empty or 
            globals.df_feature_matrix.size == 0):
            
            logger.warning("Data appears to be empty, trying to reload...")
            if not update_dataset():
                raise RuntimeError("Dataset reload failed")
        
        if str(track_id) not in globals.dataset_df['beat_id'].astype(str).values:
            logger.warning(f"Track {track_id} not found in dataset")
            raise ValueError(f"Track {track_id} not found")

        # Пытаемся найти в кэше
        cached_data = redis_cache.get_similar_tracks(track_id)
        if cached_data:
            logger.debug(f"Returning cached data for track {track_id}")
            return [{
                "beat_id": item["beat_id"],
                "file": item["file"],
                "url": item["url"],
                "price": item["price"],
                "picture": item["picture"],
                "timestamps": item["timestamps"],
                # "genres": item.get("genres", []),
                # "moods": item.get("moods", []),
                # "tags": item.get("tags", [])
            } for item in cached_data]

        # Получаем похожие треки
        similar_tracks = find_similar_tracks(track_id, top_n=top_n, return_full_data=False)
        
        if not similar_tracks:
            logger.warning(f"No similar tracks found for {track_id}")
            return []

        # Получаем и кэшируем полные данные
        try:
            full_data = find_similar_tracks(track_id, top_n=top_n, return_full_data=True)
            redis_cache.set_similar_tracks(track_id, full_data)
        except Exception as e:
            logger.error(f"Failed to cache full results: {str(e)}")

        return similar_tracks

    except Exception as e:
        logger.error(f"Error in get_similar_tracks_use_case: {str(e)}", exc_info=True)
        raise RuntimeError(f"Service unavailable: {str(e)}")