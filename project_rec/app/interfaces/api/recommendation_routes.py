from flask import Blueprint, jsonify
from flasgger import swag_from
from app.use_cases.recommendation_service import RecommendationService

def create_recommendation_blueprint(service: RecommendationService):
    bp = Blueprint('recommendations', __name__)

    @bp.route('/get_one_recommendation/<user_id>', methods=['GET'])
    @swag_from({
        'tags': ['Recommendations'],
        'parameters': [
            {
                'name': 'user_id',
                'in': 'path',
                'type': 'string',
                'required': True,
                'description': 'User ID'
            }
        ],
        'responses': {
            200: {
                'description': 'Single recommendation',
                'content': {
                    'application/json': {
                        'examples': {
                            'complete': {
                                'value': {
                                    'user_id': 'user123',
                                    'recommendation': {
                                        'title': 'Popular Song',
                                        'artist': 'Famous Artist',
                                        'url': 'https://example.com/track/123'
                                    },
                                    'remaining': 9,
                                    'status': 'complete'
                                }
                            },
                            'partial': {
                                'value': {
                                    'user_id': 'user123',
                                    'recommendation': None,
                                    'remaining': 0,
                                    'status': 'partial'
                                }
                            }
                        }
                    }
                }
            }
        }
    })
    def get_one_recommendation(user_id):
        # Получаем одну рекомендацию для пользователя
        result = service.get_recommendations(user_id, 1)
        return jsonify(result)

    @bp.route('/get_five_recommendations/<user_id>', methods=['GET'])
    @swag_from({
        'tags': ['Recommendations'],
        'parameters': [
            {
                'name': 'user_id',
                'in': 'path',
                'type': 'string',
                'required': True,
                'description': 'User ID'
            }
        ],
        'responses': {
            200: {
                'description': 'Five recommendations',
                'content': {
                    'application/json': {
                        'examples': {
                            'complete': {
                                'value': {
                                    'user_id': 'user123',
                                    'recommendations': [
                                        {'title': 'Song 1', 'artist': 'Artist 1'},
                                        {'title': 'Song 2', 'artist': 'Artist 2'},
                                        {'title': 'Song 3', 'artist': 'Artist 3'},
                                        {'title': 'Song 4', 'artist': 'Artist 4'},
                                        {'title': 'Song 5', 'artist': 'Artist 5'}
                                    ],
                                    'remaining': 5,
                                    'status': 'complete'
                                }
                            },
                            'partial': {
                                'value': {
                                    'user_id': 'user123',
                                    'recommendations': [
                                        {'title': 'Song 1', 'artist': 'Artist 1'},
                                        {'title': 'Song 2', 'artist': 'Artist 2'}
                                    ],
                                    'remaining': 0,
                                    'status': 'partial'
                                }
                            }
                        }
                    }
                }
            }
        }
    })
    def get_five_recommendations(user_id):
        # Получаем 5 рекомендаций для пользователя
        result = service.get_recommendations(user_id, 5)
        return jsonify(result)

    return bp
