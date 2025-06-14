import os
from dotenv import load_dotenv
load_dotenv()
class Config:
    KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
    REC_BEATS_TOPIC = "rec_beats_topic2"
    REFILL_TOPIC = "rec_refill_requests"
    REFILL_COOLDOWN = 300
    REFILL_THRESHOLD = 5  # Пороговое значение для дозапроса
    REFILL_COUNT = 9     
    BATCH_SIZE = 9       # Максимальное кол-во рекомендаций за раз
    MIN_GENRES = 1       
    MAX_GENRES = 3
    JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY") 
    JWT_TOKEN_LOCATION = ["headers"]          
    JWT_ACCESS_TOKEN_EXPIRES = 3600      