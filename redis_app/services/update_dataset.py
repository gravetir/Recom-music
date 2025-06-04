from infrastructure.data_loader import load_data
import services.globals as globals
import pandas as pd
import logging
from threading import Lock
import time
import threading
from datetime import datetime

logger = logging.getLogger(__name__)
update_lock = Lock()
def update_dataset() -> bool:
    try:
        df, features, genres, moods, tags = load_data()

        globals.dataset_df = df
        globals.df_feature_matrix = features
        globals.df_genres = genres
        globals.df_moods = moods
        globals.df_tags = tags

        logger.info(f"Dataset updated. Records: {len(df)}")
        return True

    except Exception as e:
        logger.error(f"Failed to update dataset: {e}")
        return False

def run_nightly_update():
    """Запускает фоновый поток для ежедневного обновления в 00:00"""
    def scheduler():
        while True:
            now = datetime.now()
            if now.hour == 0 and now.minute == 0:
                if update_dataset(): 
                    time.sleep(60)
            time.sleep(30)

    thread = threading.Thread(target=scheduler, daemon=True)
    thread.start()