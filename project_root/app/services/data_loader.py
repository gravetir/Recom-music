import json
import os
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Tuple, Optional
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from sklearn.impute import SimpleImputer
import app.services as globals


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


load_dotenv()

def get_db_engine():
    return create_engine(
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        f"?sslmode={os.getenv('DB_SSLMODE')}"
    )

def safe_str_split(x: str, sep: str = '||') -> List[str]:
    return [] if pd.isna(x) or not x else [item.strip() for item in x.split(sep) if item.strip()]

def load_lookup_tables() -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    try:
        with get_db_engine().connect() as conn:
            logger.info("Loading lookup tables...")
            genres = pd.read_sql(text("SELECT id, name FROM genres"), conn)
            tags = pd.read_sql(text("SELECT id, name FROM tags"), conn)
            moods = pd.read_sql(text("SELECT id, name FROM moods"), conn)
            
            globals.df_genres_lookup = genres
            globals.df_tags_lookup = tags
            globals.df_moods_lookup = moods
            
            logger.info("Lookup tables loaded successfully")
            return genres, tags, moods
            
    except Exception as e:
        logger.error(f"Error loading lookup tables: {str(e)}", exc_info=True)
        return None, None, None

def load_data() -> Tuple[
    Optional[pd.DataFrame],  # исходный DataFrame с данными из SQL
    Optional[List[Dict[str, Any]]],  # beats — список словарей
    Optional[np.ndarray],  # feature_matrix
    Optional[pd.DataFrame],  # df_genres
    Optional[pd.DataFrame],  # df_tags
    Optional[pd.DataFrame]   # df_moods
]:
    try:
        logger.info("Starting data loading process...")
        engine = get_db_engine()
        
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            logger.info("Database connection established")

            query = text("""
                SELECT 
                    b.id AS beat_id,
                    b.name AS file,
                    b.picture,
                    b.price,
                    b.url,
                    COALESCE((
                        SELECT json_agg(json_build_object(
                            'id', t.id,
                            'name', t.name,
                            'time_start', t.time_start,
                            'time_end', t.time_end
                        ))
                        FROM timestamps t WHERE t.beat_id = b.id
                    ), '[]') AS timestamps,
                    (SELECT string_agg(bg.genre_id::text, '||') FROM beat_genres bg WHERE bg.beat_id = b.id) AS genre_ids,
                    (SELECT string_agg(bt.tag_id::text, '||') FROM beat_tags bt WHERE bt.beat_id = b.id) AS tag_ids,
                    (SELECT string_agg(bm.mood_id::text, '||') FROM beat_moods bm WHERE bm.beat_id = b.id) AS mood_ids,
                    mf.crm1, mf.crm2, mf.crm3, mf.crm4, mf.crm5, mf.crm6, mf.crm7, mf.crm8,
                    mf.crm9, mf.crm10, mf.crm11, mf.crm12,
                    mf.mlspc AS melspectrogram,
                    mf.spc AS spectral_centroid,
                    mf.mfcc1, mf.mfcc2, mf.mfcc3, mf.mfcc4, mf.mfcc5, mf.mfcc6, mf.mfcc7, mf.mfcc8,
                    mf.mfcc9, mf.mfcc10, mf.mfcc11, mf.mfcc12, mf.mfcc13, mf.mfcc14, mf.mfcc15,
                    mf.mfcc16, mf.mfcc17, mf.mfcc18, mf.mfcc19, mf.mfcc20, mf.mfcc21, mf.mfcc22,
                    mf.mfcc23, mf.mfcc24, mf.mfcc25, mf.mfcc26, mf.mfcc27, mf.mfcc28, mf.mfcc29,
                    mf.mfcc30, mf.mfcc31, mf.mfcc32, mf.mfcc33, mf.mfcc34, mf.mfcc35, mf.mfcc36,
                    mf.mfcc37, mf.mfcc38, mf.mfcc39, mf.mfcc40, mf.mfcc41, mf.mfcc42, mf.mfcc43,
                    mf.mfcc44, mf.mfcc45, mf.mfcc46, mf.mfcc47, mf.mfcc48, mf.mfcc49, mf.mfcc50
                FROM beats b
                LEFT JOIN mfccs mf ON b.id = mf.beat_id
            """)
            
            df = pd.read_sql(query, conn)
            
            if df.empty:
                logger.error("Query returned empty dataframe")
                return None, None, None, None, None, None

            # Обработка данных
            beats, feature_matrix, df_genres, df_tags, df_moods = process_raw_data(df)
            
            logger.info(f"Data loaded successfully. Beats: {len(beats)}")
            return df, beats, feature_matrix, df_genres, df_tags, df_moods
            
    except Exception as e:
        logger.error(f"Data loading failed: {str(e)}", exc_info=True)
        return None, None, None, None, None, None

def process_raw_data(df: pd.DataFrame) -> Tuple[List[Dict[str, Any]], np.ndarray, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # Обработка JSON
    df['timestamps'] = df['timestamps'].apply(
        lambda x: json.loads(x) if isinstance(x, str) else x
    )
    
    # Преобразование строк в списки
    for col in ['genre_ids', 'tag_ids', 'mood_ids']:
        df[col] = df[col].apply(lambda x: safe_str_split(x, '||'))
    
    # Создание списка треков
    beats = []
    for _, row in df.iterrows():
        beats.append({
            "beat_id": row['beat_id'],
            "file": row['file'],
            "picture": row['picture'],
            "price": float(row['price']),
            "url": row['url'],
            "timestamps": row['timestamps'],
            "genres": row['genre_ids'],
            "tags": row['tag_ids'],
            "moods": row['mood_ids'],
            "audio_features": get_audio_features(row)
        })
    
    # One-hot encoding
    df_genres = pd.get_dummies(df['genre_ids'].explode()).groupby(level=0).sum()
    df_tags = pd.get_dummies(df['tag_ids'].explode()).groupby(level=0).sum()
    df_moods = pd.get_dummies(df['mood_ids'].explode()).groupby(level=0).sum()
    
    # Матрица фичей
    audio_cols = [f'crm{i}' for i in range(1, 13)] + \
                [f'mfcc{i}' for i in range(1, 51)] + \
                ['melspectrogram', 'spectral_centroid']
    
    feature_matrix = SimpleImputer(strategy='mean').fit_transform(df[audio_cols].values)
    
    return beats, feature_matrix, df_genres, df_tags, df_moods

def get_audio_features(row) -> Dict[str, Any]:
    """Извлекает аудио-фичи из строки DataFrame"""
    return {
        "crm": [row[f'crm{i}'] for i in range(1, 13)],
        "melspectrogram": row['melspectrogram'],
        "spectral_centroid": row['spectral_centroid'],
        "mfcc": [row[f'mfcc{i}'] for i in range(1, 51)]
    }