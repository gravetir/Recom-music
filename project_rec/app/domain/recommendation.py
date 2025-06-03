from dataclasses import dataclass
from typing import List

@dataclass
class Beat:
    id: int
    title: str
    genres: List[str]
    tags: List[str]
    moods: List[str]
    score: float

@dataclass
class Recommendation:
    user_id: str
    beat: Beat
    timestamp: int