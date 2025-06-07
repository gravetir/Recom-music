import threading
import time
from collections import defaultdict
import logging
from app.config.settings import Config
from app.domain.recommendation_storage import RecommendationStorage
from app.interfaces.kafka.producer import RefillProducer

logger = logging.getLogger(__name__)

class RecommendationService:
    def __init__(self, storage: RecommendationStorage, refill_producer: RefillProducer):
        """
        Инициализация сервиса рекомендаций с хранилищем и продюсером refill-запросов
        """
        self.storage = storage
        self.refill_producer = refill_producer

    def get_recommendations(self, user_id: str, count: int):
        """
        Получение списка рекомендаций для пользователя с проверкой на количество оставшихся
        Если количество рекомендаций меньше порогового значения, запускается запрос на пополнение
        """
        with self.storage.lock:
            recommendations = self.storage.user_recommendations.get(user_id, [])
            result = recommendations[:count]
            remaining = len(recommendations) - len(result)

            # Обновить хранилище, удалив использованные рекомендации
            self.storage.user_recommendations[user_id] = recommendations[count:]

            # Проверка, нужно ли пополнить рекомендации
            if remaining < Config.REFILL_THRESHOLD:
                threading.Thread(target=self.request_refill, args=(user_id,)).start()
            
            return {
            "user_id": user_id,
            "requested": count,
            "returned": len(result),
            "remaining": remaining,
            "status": "complete" if len(result) >= count else "partial",
            "recommendations": result
            }
        return result, remaining

    def request_refill(self, user_id: str):
        """
        Запрос на пополнение рекомендаций, если это необходимо
        """
        current_time = time.time()

        with self.storage.lock:
            if user_id in self.storage.pending_refills:
                if current_time - self.storage.pending_refills[user_id] < Config.REFILL_TIMEOUT:
                    return
            self.storage.pending_refills[user_id] = current_time

        try:
            # Отправка запроса на пополнение через refill-продюсера
            self.refill_producer.send_refill_request(user_id)
        except Exception as e:
            logger.error(f"Failed to send refill request for {user_id}: {e}")
            # В случае ошибки — откат ожидания пополнения
            with self.storage.lock:
                self.storage.pending_refills.pop(user_id, None)

    def process_kafka_message(self, user_id: str, beat: dict):
        """
        Метод для обработки сообщений, полученных из Kafka
        Добавляет beat в список рекомендаций для пользователя
        """
        # Если пользователя нет в хранилище рекомендаций, создаём пустой список
        if user_id not in self.storage.user_recommendations:
            self.storage.user_recommendations[user_id] = []
        
        # Добавление нового beat в список рекомендаций пользователя
        self.storage.user_recommendations[user_id].append(beat)
        
        # Для отладки выводим обработанный beat
        print(f"Processed beat for {user_id}: {beat}")

    def cleanup_storage(self):
        """
        Периодическая очистка хранилища — удаление старых запросов и пользователей
        """
        while True:
            time.sleep(60)  # Очистка выполняется раз в минуту
            with self.storage.lock:
                current_time = time.time()

                # Удаление старых refill-запросов
                expired = [
                    uid for uid, ts in self.storage.pending_refills.items()
                    if current_time - ts > Config.REFILL_TIMEOUT
                ]
                for uid in expired:
                    del self.storage.pending_refills[uid]

                # Периодическая очистка рекомендаций для пользователей
                if current_time - getattr(self.storage, 'last_cleanup', 0) > Config.CLEANUP_INTERVAL:
                    self.storage.last_cleanup = current_time
                    self.storage.user_recommendations = defaultdict(
                        list,
                        {uid: recs for uid, recs in self.storage.user_recommendations.items() if recs}
                    )
                    logger.info("Storage cleanup complete.")
