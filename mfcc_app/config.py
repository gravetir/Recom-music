import os
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
load_dotenv()

class Config:
    """Основная конфигурация приложения"""
    # Базовая информация о S3
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    BUCKET_NAME = os.getenv('BUCKET_NAME', 'default-bucket') 
    MP3_FOLDER = os.getenv('MP3_FOLDER', 'mp3/') 

    # Конфигурация Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
    KAFKA_PUBLISH_TOPIC = os.getenv('KAFKA_PUBLISH_TOPIC', 'publish_beat')
    KAFKA_TRACK_TOPIC = os.getenv('KAFKA_TRACK_TOPIC', 'track_for_mfcc')

    # Прочие настройки (например, Flask)
    FLASK_APP_HOST = os.getenv('FLASK_APP_HOST', '0.0.0.0')
    FLASK_APP_PORT = int(os.getenv('FLASK_APP_PORT', 8004))
    DEBUG = os.getenv('DEBUG', 'True') == 'True'

