import time
import uuid
import logging
from datetime import datetime
from typing import List, Dict, Any

from flask import request, jsonify, Flask
from flasgger import swag_from
from flask_jwt_extended import (
    jwt_required,
    get_jwt_identity,
    create_access_token
)
from flask_cors import CORS
from app.config import Config

logger = logging.getLogger(__name__)

def register_routes(app: Flask, engine, storage, kafka):
    CORS(app)

    @app.route('/login', methods=['POST'])
    @swag_from({
        'tags': ['Auth'],
        'description': 'Получение JWT токена по username и password',
        'parameters': [{
            'in': 'body',
            'name': 'body',
            'required': True,
            'schema': {
                'type': 'object',
                'properties': {
                    'username': {'type': 'string', 'example': 'user1'},
                    'password': {'type': 'string', 'example': '123'}
                },
                'required': ['username', 'password']
            }
        }],
        'responses': {
            200: {
                'description': 'JWT токен успешно сгенерирован',
                'schema': {
                    'type': 'object',
                    'properties': {
                        'access_token': {'type': 'string'}
                    }
                }
            },
            401: {'description': 'Неверный логин или пароль'}
        }
    })
    def login():
        data = request.get_json()
        username = data.get('username')
        password = data.get('password')

        if not username or not password:
                return jsonify({"msg": "Username and password are required"}), 400

        access_token = create_access_token(identity=username)
        return jsonify(access_token=access_token)


    def send_recommendations(user_id: str, recommendations: List, beats_map: Dict[int, Dict[str, Any]]) -> List[Dict[str, Any]]:
        beats = []

        for rec in recommendations[:Config.BATCH_SIZE]:
            full_beat = beats_map.get(rec[0])
            if not full_beat:
                logger.warning(f"[API] Beat id {rec[0]} не найден в beats_map")
                continue

            beat = {**full_beat}
            beat["timestamp"] = datetime.now().isoformat()
            beats.append(beat)
            kafka.send_recommendation(user_id, beat)

        kafka.flush_producer()
        storage.direct_recommendations[user_id] = beats

        if len(beats) <= Config.REFILL_THRESHOLD:
            logger.info(f"[API] Недостаточно рекомендаций, инициируем refill для {user_id}")
            request_refill(user_id)

        return beats

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

    @app.route('/create_rec_first_launch', methods=['POST'])
    @jwt_required()
    @swag_from({
        'tags': ['Recommendations'],
        'description': 'Генерация рекомендаций по жанрам (требуется JWT)',
        'parameters': [{
            'in': 'body',
            'name': 'body',
            'required': True,
            'schema': {
                'type': 'object',
                'properties': {
                    'genres': {
                        'type': 'array',
                        'items': {'type': 'int'},
                        'example': [3, 5, 9],
                        'description': 'Список жанров (3-5 элементов)'
                    }
                },
                'required': ['genres']
            }
        }],
        'security': [{'Bearer': []}],
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
            400: {'description': 'Неверный ввод'}
        }
    })
    def create_rec_first_launch():
        logger.info("[API] Первая генерация рекомендаций")

        data = request.get_json()
        genres = [str(g).strip() for g in data.get('genres', []) if str(g).strip()]
        logger.debug(f"[API] Получены жанры: {genres}")

        if not genres or len(genres) < Config.MIN_GENRES or len(genres) > Config.MAX_GENRES:
            return jsonify({
                "error": f"Number of genres must be between {Config.MIN_GENRES} and {Config.MAX_GENRES}"
            }), 400

        user_id = get_jwt_identity()
        logger.info(f"[API] user_id из JWT: {user_id}")

        storage.user_genres[user_id] = genres
        storage.clear_recommendations(user_id)

        try:
            recommendations = engine.generate_recommendations_by_genres(genres)
            logger.info(f"[API] Сгенерировано {len(recommendations)} рекомендаций")

            beats = send_recommendations(user_id, recommendations, {b['id']: b for b in engine.beats})

            return jsonify({
                "status": "success",
                "message": f"Sent {len(beats)} recommendations",
                "user_id": user_id
            })

        except Exception as e:
            logger.exception(f"[API] Ошибка при генерации по жанрам: {e}")
            return jsonify({"error": "Failed to generate recommendations"}), 500

    @app.route('/create_rec_likes_tracks', methods=['POST'])
    @swag_from({
        'tags': ['Recommendations'],
        'description': 'Генерация рекомендаций по лайкам',
        'parameters': [{
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
        }],
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
            400: {'description': 'Неверный ввод'}
        }
    })
    def create_rec_likes_tracks():
        logger.info("[API] Генерация рекомендаций по лайкам")

        data = request.get_json()
        liked_ids = data.get('song_id')
        if not liked_ids:
            return jsonify({"error": "song_id list is required"}), 400

        user_id = data.get('user_id', str(uuid.uuid4()))
        logger.info(f"[API] Используется user_id: {user_id}")

        storage.user_likes[user_id] = liked_ids
        storage.clear_recommendations(user_id)

        try:
            recommendations = engine.generate_recommendations_by_likes(liked_ids)
            logger.info(f"[API] Сгенерировано {len(recommendations)} рекомендаций")

            beats = send_recommendations(user_id, recommendations, {b['id']: b for b in engine.beats})

            return jsonify({
                "status": "success",
                "sent_count": len(beats),
                "user_id": user_id
            })

        except Exception as e:
            logger.exception(f"[API] Ошибка при генерации по лайкам: {e}")
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
        logger.info("[API] Health check")
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
