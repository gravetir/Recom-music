import uuid
from flask import Blueprint, jsonify
from flasgger import swag_from
from services.s3_services import check_file_in_s3
from services.kafka_service import send_kafka_message
from services.audio_service import analyze_audio
import os
from services.s3_services import download_audio_from_s3
bp = Blueprint('api', __name__)

BUCKET_NAME = os.getenv('BUCKET_NAME', 'music-beats-bucket')
KAFKA_TRACK_TOPIC = os.getenv('KAFKA_TRACK_TOPIC', 'track_for_mfcc')

@bp.route('/api/process/<string:filename>', methods=['GET'])
@swag_from({
    'parameters': [
        {
            'name': 'filename',
            'in': 'path',
            'type': 'string',
            'required': True
        }
    ],
    'responses': {
        202: {
            'description': 'Обработка начата',
            'schema': {
                'type': 'object',
                'properties': {
                    'message': {'type': 'string'},
                    'filename': {'type': 'string'},
                    'beat_id': {'type': 'string', 'format': 'uuid'}
                }
            }
        },
        404: {'description': 'Файл не найден'},
        500: {'description': 'Ошибка сервера'}
    }
})
def process_file(filename):
    if not check_file_in_s3(filename):
        return jsonify({'error': 'File not found'}), 404

    beat_id = str(uuid.uuid4())
    message = {"filename": filename, "beat_id": beat_id}

    if not send_kafka_message(KAFKA_TRACK_TOPIC, message):
        return jsonify({'error': 'Failed to send to Kafka'}), 500

    return jsonify({
        "message": "Processing started",
        "filename": filename,
        "beat_id": beat_id
    }), 202

@bp.route('/api/tracks/<string:filename>', methods=['GET'])
@swag_from({
    'parameters': [
        {
            'name': 'filename',
            'in': 'path',
            'type': 'string',
            'required': True
        }
    ],
    'responses': {
        200: {
            'description': 'Характеристики трека или ошибка',
            'schema': {
                'type': 'object',
                'properties': {
                    'beat_id': {'type': 'string', 'format': 'uuid'},
                    'features': {
                        'type': 'object',
                        'properties': {
                            'mfcc': {
                                'type': 'array',
                                'items': {'type': 'number'}
                            },
                            'chroma': {
                                'type': 'array',
                                'items': {'type': 'number'}
                            },
                            'spectral_centroid': {'type': 'number'},
                            'melspectrogram': {'type': 'number'},
                            'bpm': {'type': 'integer'}
                        }
                    },
                    'error': {'type': 'string'}
                }
            }
        },
        404: {'description': 'Файл не найден'},
        500: {'description': 'Ошибка анализа'}
    }
})
def get_track_features(filename):
    if not check_file_in_s3(filename):
        return jsonify({'error': 'File not found'}), 404

    audio_path = download_audio_from_s3(filename)
    if not audio_path:
        return jsonify({'error': 'Failed to download audio'}), 500

    features = analyze_audio(audio_path)
    if not features:
        return jsonify({'error': 'Audio analysis failed'}), 500

    return jsonify({
        "beat_id": str(uuid.uuid4()),
        "features": features,
        "error": ""
    }), 200

