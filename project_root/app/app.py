from flask import Flask
from flasgger import Swagger
from app.api.routes import register_routes
from app.config import Config
import threading
from app.services.kafka_service import consume_recommendations, consume_refill_requests
import logging
from app.services.data_loader import load_data, load_lookup_tables
from app.services.update_dataset import run_nightly_update
import app.services.globals as globals
from app.core.recommendation_engine import RecommendationEngine
from app.core.storage import RecommendationStorage
from app.services.kafka_service import KafkaClient
from flask_jwt_extended import JWTManager
import os

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)
    app.config["JWT_SECRET_KEY"] = os.getenv(
        "JWT_SECRET_KEY", "default-secret-if-not-set"
    )
    jwt = JWTManager(app)

    def initialize_data():
        """Загрузка и инициализация данных при старте приложения"""
        try:
            logger.info("Initializing dataset...")
            dataset, features, genres, moods, beats, tags = load_data()
            logger.info(f"Type of dataset: {type(dataset)}")
            globals.beats_list = beats
            globals.dataset_df = dataset
            globals.df_feature_matrix = features
            globals.df_genres = genres
            globals.df_moods = moods
            globals.df_tags = tags

            logger.info(f"Loaded dataset with {len(dataset)} beats")

            logger.info("Loading lookup tables...")
            g, t, m = load_lookup_tables()
            globals.df_genres_lookup = g
            globals.df_tags_lookup = t
            globals.df_moods_lookup = m
            logger.info("Lookup tables loaded")

        except Exception as e:
            logger.critical(f"Data initialization failed: {str(e)}", exc_info=True)
            raise

    def configure_swagger():
        swagger_config = {
            "headers": [],
            "specs": [
                {
                    "endpoint": "apispec",
                    "route": "/api-spec.json",
                    "rule_filter": lambda rule: True,
                    "model_filter": lambda tag: True,
                }
            ],
            "static_url_path": "/flasgger_static",
            "swagger_ui": True,
            "specs_route": "/docs/",
            "title": "Music Recommendations API",
            "version": "1.0.0",
            "uiversion": 3,
            "termsOfService": "",
        }

        template = {
            "swagger": "2.0",
            "info": {
                "title": "Music Recommendations API",
                "description": "API для генерации музыкальных рекомендаций",
                "version": "1.0.0",
            },
            "host": "localhost:8000",
            "basePath": "/",
            "schemes": ["http"],
            "consumes": ["application/json"],
            "produces": ["application/json"],
            "securityDefinitions": {
                "JWT": {
                    "type": "apiKey",
                    "name": "Authorization",
                    "in": "header",
                    "description": "Введите JWT токен с словом Bearer ",
                }
            },
            # "security": [{"Bearer": []}]
        }

        Swagger(app, config=swagger_config, template=template)

    def start_background_tasks():
        def run_safe(target, name):
            try:
                logger.info(f"Starting {name} thread...")
                target()
            except Exception as e:
                logger.error(f"{name} thread failed: {str(e)}", exc_info=True)

        tasks = [
            (consume_recommendations, "Kafka Recommendations Consumer"),
            (consume_refill_requests, "Kafka Refill Consumer"),
            (run_nightly_update, "Nightly Dataset Update"),
        ]

        for target, name in tasks:
            thread = threading.Thread(
                target=lambda: run_safe(target, name), daemon=True
            )
            thread.start()

    try:
        initialize_data()
        engine = RecommendationEngine()
        storage = RecommendationStorage()
        kafka = KafkaClient()
        register_routes(app, engine, storage, kafka)
        configure_swagger()
        start_background_tasks()
        logger.info("Application initialized successfully")
    except Exception as e:
        logger.critical(f"Application failed to initialize: {str(e)}", exc_info=True)
        raise

    return app
