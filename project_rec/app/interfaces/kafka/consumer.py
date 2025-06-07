import time
import logging
import json
from kafka import KafkaConsumer
from app.config.settings import Config
from app.use_cases.recommendation_service import RecommendationService

logger = logging.getLogger(__name__)

class RecommendationConsumer:
    def __init__(self, service: RecommendationService):
        """
        Конструктор для инициализации Kafka Consumer и сервиса
        """
        self.service = service
        self.consumer = KafkaConsumer(
            Config.REC_BEATS_TOPIC,
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            group_id="recommendation_service_group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

    def start(self):
        """
        Основной метод потребителя, который запускает прослушку Kafka и обработку сообщений
        """
        while True:
            try:
                logger.info("Kafka consumer started on topic: %s", Config.REC_BEATS_TOPIC)

                for message in self.consumer:
                    try:
                        # Извлечение user_id из ключа сообщения
                        user_id = message.key.decode('utf-8') if message.key else None
                        if not user_id:
                            continue

                        # Извлечение beat из значения сообщения
                        message_value = message.value
                        beat = message_value.get('beat') if isinstance(message_value, dict) else message_value

                        if not beat:
                            continue

                        # Обработка сообщения 
                        self.service.process_kafka_message(user_id, beat)

                        self.consumer.commit()

                    except Exception as e:
                        logger.error("Error processing message: %s", str(e))
                        time.sleep(1)

            except Exception as e:
                logger.error("Kafka consumer error: %s. Restarting in 5s...", str(e))
                time.sleep(5)