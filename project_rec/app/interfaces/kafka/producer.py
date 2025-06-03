import json
import uuid
import time
import logging
from kafka import KafkaProducer
from app.config.settings import Config

logger = logging.getLogger(__name__)

class RefillProducer:
    def __init__(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            logger.info("Kafka producer initialized")
        except Exception as e:
            logger.error("Failed to initialize Kafka producer: %s", e)
            raise

    def send_refill_request(self, user_id: str):
        try:
            message = {
                "request_id": str(uuid.uuid4()),
                "user_id": user_id,
                "count": Config.REFILL_COUNT,
                "timestamp": int(time.time())
            }
            print(f"Sending refill message: {message}")

            self.producer.send(
                topic=Config.REFILL_TOPIC,
                key=user_id,
                value=message
            )
            self.producer.flush()

            logger.info("Sent refill request for user %s", user_id)

        except Exception as e:
            logger.error("Failed to send refill request for user %s: %s", user_id, e)
