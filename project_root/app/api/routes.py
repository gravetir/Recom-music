import time
import uuid
from flask import request, jsonify
from flasgger import swag_from
from app.config import Config
import logging
from datetime import datetime
import json

logger = logging.getLogger(__name__)

def register_routes(app, engine, storage, kafka):

    @app.route('/create_rec_first_launch', methods=['POST'])
    @swag_from({
        'tags': ['Recommendations'],
        'description': 'Генерация рекомендаций по жанрам',
        'parameters': [
            {
                'in': 'body',
                'name': 'body',
                'required': True,
                'schema': {
                    'type': 'object',
                    'properties': {
                        'genres': {
                            'type': 'array',
                            'items': {'type': 'int'},
                            'example': ['3', '5', '9'],
                            'description': 'Список жанров (3-5 элементов)'
                        },
                        'user_id': {
                            'type': 'string',
                            'description': 'ID пользователя (опционально)'
                        }
                    },
                    'required': ['genres']
                }
            }
        ],
        'responses': {
            200: {
                'description': 'Успешный ответ',
                'schema': {
                    'type': 'object',
                    'properties': {
                        'status': {'type': 'string'},
                        'message': {'type': 'string'},
                        'user_id': {'type': 'string'}
                    }
                }
            },
            400: {
                'description': 'Неверный ввод',
                'schema': {
                    'type': 'object',
                    'properties': {
                        'error': {'type': 'string'}
                    }
                }
            }
        }
    })
    def create_rec_first_launch():
        logger.info("[API] Запрос на первую генерацию рекомендаций")

        data = request.get_json()
        if not data or 'genres' not in data:
            logger.warning("[API] Отсутствует поле 'genres' в теле запроса")
            return jsonify({"error": "Genres list is required"}), 400

        genres = [str(g).strip() for g in data['genres'] if str(g).strip()]
        logger.debug(f"[API] Получены жанры: {genres}")

        if len(genres) < Config.MIN_GENRES or len(genres) > Config.MAX_GENRES:
            logger.warning(f"[API] Некорректное количество жанров: {len(genres)}")
            return jsonify({
                "error": f"Number of genres must be between {Config.MIN_GENRES} and {Config.MAX_GENRES}"
            }), 400

        user_id = data.get('user_id', str(uuid.uuid4()))
        logger.info(f"[API] Используется user_id: {user_id}")

        storage.user_genres[user_id] = genres
        storage.clear_recommendations(user_id)

        try:
            recommendations = engine.generate_recommendations_by_genres(genres)
            logger.info(f"[API] Сгенерировано {len(recommendations)} рекомендаций")

            # Создаем мапу для быстрого поиска полного трека по id
            beats_map = {beat['id']: beat for beat in engine.beats}

            beats = []
            for rec in recommendations[:Config.BATCH_SIZE]:
                full_beat = beats_map.get(rec[0])
                if not full_beat:
                    logger.warning(f"[API] Beat id {rec[0]} не найден в beats_map")
                    continue

                # Добавляем поле score из rec и остальные поля из полного объекта
                beat = {
                    **full_beat
                }
                beats.append(beat)

                kafka.send_recommendation(user_id, beat)

            kafka.flush_producer()
            storage.direct_recommendations[user_id] = beats
            logger.info(f"[API] Отправлено {len(beats)} треков в Kafka")

            if len(beats) <= Config.REFILL_THRESHOLD:
                logger.info(f"[API] Недостаточно рекомендаций, инициируем refill для {user_id}")
                request_refill(user_id)

            return jsonify({
                "status": "success",
                "message": f"Sent {len(beats)} recommendations",
                "user_id": user_id
            })

        except Exception as e:
            logger.exception(f"[API] Ошибка при генерации рекомендаций: {e}")
            return jsonify({"error": "Failed to generate recommendations"}), 500

    @app.route('/create_rec_likes_tracks', methods=['POST'])
    @swag_from({
        'tags': ['Recommendations'],
        'description': 'Генерация рекомендаций по лайкам',
        'parameters': [
            {
                'in': 'body',
                'name': 'body',
                'required': True,
                'schema': {
                    'type': 'object',
                    'properties': {
                        'song_id': {
                            'type': 'array',
                            'items': {'type': 'integer'},
                            'example': [1, 2, 3],
                            'description': 'Список ID лайкнутых треков'
                        },
                        'user_id': {
                            'type': 'string',
                            'description': 'ID пользователя (опционально)'
                        }
                    },
                    'required': ['song_id']
                }
            }
        ],
        'responses': {
            200: {
                'description': 'Успешный ответ',
                'schema': {
                    'type': 'object',
                    'properties': {
                        'status': {'type': 'string'},
                        'sent_count': {'type': 'integer'},
                        'user_id': {'type': 'string'}
                    }
                }
            },
            400: {
                'description': 'Неверный ввод',
                'schema': {
                    'type': 'object',
                    'properties': {
                        'error': {'type': 'string'}
                    }
                }
            }
        }
    })
    def create_rec_likes_tracks():
        logger.info("[API] Запрос на генерацию рекомендаций по лайкам")

        data = request.get_json()
        if not data or 'song_id' not in data:
            logger.warning("[API] Отсутствует поле 'song_id' в теле запроса")
            return jsonify({"error": "song_id list is required"}), 400

        liked_ids = data['song_id']
        logger.debug(f"[API] Получены лайкнутые ID: {liked_ids}")

        user_id = data.get('user_id', str(uuid.uuid4()))
        logger.info(f"[API] Используется user_id: {user_id}")

        storage.user_likes[user_id] = liked_ids
        storage.clear_recommendations(user_id)

        try:
            recommendations = engine.generate_recommendations_by_likes(liked_ids)
            logger.info(f"[API] Сгенерировано {len(recommendations)} рекомендаций")


            beats_map = {beat['id']: beat for beat in engine.beats}

            beats = []
            for rec in recommendations[:Config.BATCH_SIZE]:
                full_beat = beats_map.get(rec[0])
                if not full_beat:
                    logger.warning(f"[API] Beat id {rec[0]} не найден в beats_map")
                    continue

                beat = {
                    **full_beat,
                    "timestamp": datetime.now().isoformat()
                }
                beats.append(beat)

                kafka.send_recommendation(user_id, beat)

            kafka.flush_producer()
            storage.direct_recommendations[user_id] = beats
            logger.info(f"[API] Отправлено {len(beats)} треков в Kafka")

            if len(beats) <= Config.REFILL_THRESHOLD:
                logger.info(f"[API] Недостаточно рекомендаций, инициируем refill для {user_id}")
                request_refill(user_id)

            return jsonify({
                "status": "success",
                "sent_count": len(beats),
                "user_id": user_id
            })

        except Exception as e:
            logger.exception(f"[API] Ошибка при генерации рекомендаций: {e}")
            return jsonify({"error": "Failed to generate recommendations"}), 500

    @app.route('/health', methods=['GET'])
    @swag_from({
        'tags': ['Health'],
        'description': 'Проверка работоспособности сервиса',
        'responses': {
            200: {
                'description': 'Сервис работает',
                'schema': {
                    'type': 'object',
                    'properties': {
                        'status': {'type': 'string'}
                    }
                }
            }
        }
    })
    def health_check():
        logger.info("[API] Health check requested")
        return jsonify({"status": "healthy"})

    def request_refill(user_id: str):
        if not storage.should_refill(user_id, Config.REFILL_THRESHOLD, Config.REFILL_COOLDOWN):
            logger.info(f"[API] Повторная генерация для {user_id} не требуется")
            return

        refill_request = {
            "request_id": str(uuid.uuid4()),
            "user_id": user_id,
            "count": Config.REFILL_COUNT,
            "timestamp": int(time.time())
        }

        try:
            logger.info(f"[API] Отправка refill-запроса для пользователя {user_id}")
            kafka.producer.send(
                topic=Config.REFILL_TOPIC,
                value=refill_request,
                key=user_id.encode('utf-8')
            )
            kafka.flush_producer()
            storage.mark_refill_requested(user_id)
        except Exception as e:
            logger.error(f"[API] Ошибка при отправке refill-запроса: {e}")
