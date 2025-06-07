import threading
import logging
from flask import Flask
from flasgger import Swagger
from app.config.settings import Config
from app.domain.recommendation_storage import RecommendationStorage
from app.use_cases.recommendation_service import RecommendationService
from app.interfaces.api.recommendation_routes import create_recommendation_blueprint
from app.interfaces.kafka.consumer import RecommendationConsumer
from app.interfaces.kafka.producer import RefillProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_app():
    app = Flask(__name__)
    app.config['SWAGGER'] = {
        'title': 'Music Recommendations API',
        'description': 'API для получения музыкальных рекомендаций',
        'uiversion': 3,
        'specs_route': '/docs/'
    }
    
    Swagger(app)

    
    storage = RecommendationStorage()
    producer = RefillProducer()
    service = RecommendationService(storage, producer)

    # Регистрация маршрутов
    recommendation_bp = create_recommendation_blueprint(service)
    app.register_blueprint(recommendation_bp)

    # Запуск потребителя Kafka в фоновом потоке
    consumer = RecommendationConsumer(service) 
    threading.Thread(target=consumer.start, daemon=True).start()

    # Запуск очистки хранилища в фоновом потоке
    threading.Thread(target=service.cleanup_storage, daemon=True).start()

    return app

if __name__ == '__main__':
    logger.info("Starting Recommendation Service...")
    app = create_app()
    app.run(host='0.0.0.0', port=8002, threaded=True)
