import pandas as pd
from typing import Optional
import numpy as np



dataset_df: Optional[pd.DataFrame] = None
df_feature_matrix: Optional[np.ndarray] = None

df_genres: Optional[pd.DataFrame] = None
df_moods: Optional[pd.DataFrame] = None
df_tags: Optional[pd.DataFrame] = None

df_genres_lookup: Optional[pd.DataFrame] = None
df_tags_lookup: Optional[pd.DataFrame] = None
df_moods_lookup: Optional[pd.DataFrame] = None