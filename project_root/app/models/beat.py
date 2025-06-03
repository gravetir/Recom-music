from dataclasses import dataclass
from typing import List, Optional

@dataclass
class Track:
    beat_id: str
    file: str
    genres: List[str]
    moods: List[str]
    tags: List[str]
    url: Optional[str] = None
    price: Optional[float] = None
    picture: Optional[str] = None
    created_at: Optional[str] = None
    id: Optional[str] = None
    time_start: Optional[str] = None
    time_end: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            beat_id=str(data['beat_id']),
            file=data.get('file'),
            genres=list(map(str, data.get('genres', []))),
            moods=list(map(str, data.get('moods', []))),
            tags=list(map(str, data.get('tags', []))),
            url=data.get('url'),
            price=data.get('price'),
            picture=data.get('picture'),
            created_at=data.get('created_at'),
            id=data.get('id'), 
            time_start=data.get('time_start'),
            time_end=data.get('time_end'),
        )

    def to_dict(self):
        return {
            "beat_id": self.beat_id,
            "file": self.file,
            "genres": self.genres,
            "moods": self.moods,
            "tags": self.tags,
            "url": self.url,
            "price": self.price,
            "picture": self.picture,
            "created_at": self.created_at,
            "id": self.id,
            "time_start": self.time_start,
            "time_end": self.time_end
        }