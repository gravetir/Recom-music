import json
import time
import logging
from kafka import KafkaProducer, KafkaConsumer
from app.config import Config
from app.core.storage import RecommendationStorage
from app.core.recommendation_engine import RecommendationEngine

logger = logging.getLogger(__name__)

storage = RecommendationStorage()
kafka_client = None
recommendation_engine = None

class KafkaClient:
    def __init__(self):
        logger.info("[KafkaClient] Initializing Kafka Producer and Consumers")

        self.producer = KafkaProducer(
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            retries=3,
            acks='all'
        )
        logger.info("[KafkaProducer] Initialized")

        self.rec_consumer = KafkaConsumer(
            Config.REC_BEATS_TOPIC,
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            group_id="rec_service_group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )
        logger.info(f"[KafkaConsumer] Subscribed to topic '{Config.REC_BEATS_TOPIC}'")

        self.refill_consumer = KafkaConsumer(
            Config.REFILL_TOPIC,
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            group_id="refill_service_group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logger.info(f"[KafkaConsumer] Subscribed to topic '{Config.REFILL_TOPIC}'")

    def send_recommendation(self, user_id: str, beat: dict):
        try:
            payload = {
                "user_id": user_id,
                "beat": beat,
                # "timestamp": int(time.time())
            }
            logger.debug(f"[KafkaProducer] Sending recommendation to '{Config.REC_BEATS_TOPIC}': {payload}")
            self.producer.send(
                topic=Config.REC_BEATS_TOPIC,
                value=payload,
                key=user_id.encode('utf-8')
            )
            logger.info(f"[KafkaProducer] Queued recommendation for user_id={user_id}, beat_id={beat.get('id') or beat.get('beat_id')}")
        except Exception as e:
            logger.error(f"[KafkaProducer] Failed to send recommendation for user_id={user_id}: {str(e)}")

    def flush_producer(self):
        try:
            logger.debug("[KafkaProducer] Flushing producer buffer")
            self.producer.flush()
            logger.info("[KafkaProducer] Producer flush completed")
        except Exception as e:
            logger.error(f"[KafkaProducer] Producer flush failed: {str(e)}")

def consume_recommendations():
    global kafka_client
    if not kafka_client:
        kafka_client = KafkaClient()

    for msg in kafka_client.rec_consumer:
        try:
            user_id = msg.key.decode('utf-8') if msg.key else None
            if not user_id:
                logger.warning("[KafkaConsumer] Received message without user_id key")
                kafka_client.rec_consumer.commit()
                continue

            logger.debug(f"[KafkaConsumer] Received recommendation message: {msg.value}")

            beat_wrapper = msg.value
            beat = beat_wrapper.get("beat")

            if isinstance(beat, dict) and "beat" in beat:
                beat = beat["beat"]

            if not isinstance(beat, dict):
                logger.error(f"[KafkaConsumer] beat is not dict or missing: {beat_wrapper}")
                kafka_client.rec_consumer.commit()
                continue

            if "id" not in beat:
                logger.error(f"[KafkaConsumer] beat missing 'id' field: {beat}")
                kafka_client.rec_consumer.commit()
                continue

            offset = msg.offset
            if offset <= storage.processed_offsets.get(user_id, -1):
                logger.debug(f"[KafkaConsumer] Skipping already processed offset={offset} for user_id={user_id}")
                kafka_client.rec_consumer.commit()
                continue

            existing_ids = [r["id"] for r in storage.user_recommendations.get(user_id, [])]
            if beat["id"] not in existing_ids:
                logger.info(f"[KafkaConsumer] Storing beat_id={beat['id']} for user_id={user_id}")
                storage.user_recommendations.setdefault(user_id, []).append(beat)
                storage.processed_offsets[user_id] = offset

            if user_id in storage.pending_refills and len(storage.user_recommendations[user_id]) >= Config.REFILL_THRESHOLD * 2:
                logger.info(f"[KafkaConsumer] Refill complete for user_id={user_id}")
                storage.pending_refills.remove(user_id)

            kafka_client.rec_consumer.commit()
        except Exception as e:
            logger.error(f"[KafkaConsumer] Error processing recommendation message: {str(e)}")
            try:
                kafka_client.rec_consumer.commit()
            except Exception as ce:
                logger.error(f"[KafkaConsumer] Commit failed after exception: {str(ce)}")

def consume_refill_requests():
    global kafka_client, recommendation_engine
    if not kafka_client:
        kafka_client = KafkaClient()
    if not recommendation_engine:
        recommendation_engine = RecommendationEngine()

    beats_map = {beat['id']: beat for beat in recommendation_engine.beats}

    for msg in kafka_client.refill_consumer:
        try:
            logger.debug(f"[KafkaConsumer] Received refill message: {msg.value}")
            data = msg.value
            user_id = data.get("user_id")
            count = data.get("count", Config.REFILL_COUNT)

            if not user_id:
                logger.warning("[KafkaConsumer] Refill message missing user_id")
                kafka_client.refill_consumer.commit()
                continue

            logger.info(f"[KafkaConsumer] Processing refill request for user_id={user_id}, count={count}")

            liked_ids = storage.user_likes.get(user_id, [56, 70, 82])
            if liked_ids:
                recommendations = recommendation_engine.generate_recommendations_by_likes(liked_ids, count)
            else:
                genres = storage.user_genres.get(user_id, [])
                if not genres:
                    logger.warning(f"[KafkaConsumer] No genres found for user_id={user_id}, skipping refill")
                    kafka_client.refill_consumer.commit()
                    continue
                recommendations = recommendation_engine.generate_recommendations_by_genres(genres)
                logger.info(f"[Engine] Recommendations: {recommendations}")

            for rec in recommendations[:count]:
                beat_id = rec[0]
                full_beat = beats_map.get(beat_id)
                if not full_beat:
                    logger.warning(f"[KafkaConsumer] Beat id {beat_id} not found in engine.beats")
                    continue

                timestamps = []
                if len(rec) > 6:
                    try:
                        timestamps = json.loads(rec[6])
                    except Exception as e:
                        logger.error(f"[KafkaConsumer] Failed to parse timestamps for beat_id={beat_id}: {e}")

                beat = {
                    **full_beat, # распаковка словаря в Python
                    # "timestamps": timestamps
                }
                for field in ["genres", "tags", "moods"]:
                    beat.pop(field, None)

                kafka_client.send_recommendation(user_id, beat)

            kafka_client.flush_producer()
            kafka_client.refill_consumer.commit()
            logger.info(f"[KafkaConsumer] Completed refill for user_id={user_id}")
        except Exception as e:
            logger.error(f"[KafkaConsumer] Error processing refill message: {str(e)}")
            try:
                kafka_client.refill_consumer.commit()
            except Exception as ce:
                logger.error(f"[KafkaConsumer] Commit failed after exception: {str(ce)}")
