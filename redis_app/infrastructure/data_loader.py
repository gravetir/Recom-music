import json
import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from sklearn.impute import SimpleImputer
import logging
import services.globals as globals

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

load_dotenv()

DATABASE_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}?sslmode={os.getenv('DB_SSLMODE')}"
)

engine = create_engine(DATABASE_URL)


def safe_str_split(x, sep='||'):
    if not x or pd.isna(x):
        return []
    return [item.strip() for item in x.split(sep) if item.strip()]


def load_lookup_tables():
    """
    Загружаем справочные таблицы genres, tags, moods с колонками id и name
    """
    try:
        with engine.connect() as conn:
            genres = pd.read_sql(text("SELECT id, name FROM genres"), conn)
            tags = pd.read_sql(text("SELECT id, name FROM tags"), conn)
            moods = pd.read_sql(text("SELECT id, name FROM moods"), conn)
        logger.info("Loaded lookup tables: genres, tags, moods")

        # Сохраняем в глобальные переменные
        globals.df_genres_lookup = genres
        globals.df_tags_lookup = tags
        globals.df_moods_lookup = moods

        return genres, tags, moods
    except Exception as e:
        logger.error(f"Error loading lookup tables: {e}", exc_info=True)
        return None, None, None


def load_data():
    try:
        logger.info("Connecting to database...")
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            logger.info("Database connection successful")

        query = text("""
            SELECT 
                b.id AS beat_id,
                b.name AS file,
                b.picture,
                b.price,
                b.url,

                -- timestamps как JSON-массив без id
                COALESCE((
                    SELECT json_agg(json_build_object(
                        'id', t.id,
                        'name', t.name,
                        'time_start', t.time_start,
                        'time_end', t.time_end
                    ))
                    FROM timestamps t
                    WHERE t.beat_id = b.id
                ), '[]') AS timestamps,

                -- связи по жанрам, тегам, настроениям
                (SELECT string_agg(bg.genre_id::text, '||') FROM beat_genres bg WHERE bg.beat_id = b.id) AS genre_ids,
                (SELECT string_agg(bt.tag_id::text, '||') FROM beat_tags bt WHERE bt.beat_id = b.id) AS tag_ids,
                (SELECT string_agg(bm.mood_id::text, '||') FROM beat_moods bm WHERE bm.beat_id = b.id) AS mood_ids,

                -- аудиофичи
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

        df = pd.read_sql(query, engine)
        if df.empty:
            logger.error("Query returned empty dataframe")
            return None, None, None, None, None
        df['timestamps'] = df['timestamps'].apply(
            lambda x: json.loads(x) if isinstance(x, str) else x
        )

        # Проверяем наличие id в первом элементе timestamps (для логирования)
        if len(df) > 0 and len(df.iloc[0]['timestamps']) > 0:
            logger.debug(f"First timestamp sample: {df.iloc[0]['timestamps'][0]}")
            if 'id' not in df.iloc[0]['timestamps'][0]:
                logger.warning("Timestamps are missing 'id' field!")

        logger.info(f"Loaded {len(df)} records")

        # Обработка жанров, тегов и настроений:
        for col in ['genre_ids', 'tag_ids', 'mood_ids']:
            df[col] = df[col].fillna('')
            # Преобразуем строки '1||3||5' в '1,3,5'
            df[col] = df[col].apply(lambda x: ','.join(sorted(set(safe_str_split(x, sep='||')))) if x else '')

        # Получаем one-hot encoding с разделителем ','
        df_genres = df['genre_ids'].str.get_dummies(sep=',')
        df_tags = df['tag_ids'].str.get_dummies(sep=',')
        df_moods = df['mood_ids'].str.get_dummies(sep=',')

        audio_features = [f'crm{i}' for i in range(1, 13)] + \
                       [f'mfcc{i}' for i in range(1, 51)] + \
                       ['melspectrogram', 'spectral_centroid']
        
        existing_features = [f for f in audio_features if f in df.columns]
        df[existing_features] = df[existing_features].apply(pd.to_numeric, errors='coerce')

        matrices = [df[existing_features].values] if existing_features else []
        matrices.extend([df_genres.values, df_tags.values, df_moods.values])

        feature_matrix = SimpleImputer(strategy='mean').fit_transform(np.hstack(matrices))

        return df, feature_matrix, df_genres, df_tags, df_moods

    except Exception as e:
        logger.error(f"Error loading data: {str(e)}", exc_info=True)
        return None, None, None, None, None
