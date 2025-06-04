from flask import request, jsonify
from core.use_cases import get_similar_tracks_use_case
from infrastructure.redis_cache import redis_cache
import uuid

def home():
    return """
    <h1>Music Recommendation API</h1>
    <p>Use <a href="/api/docs">/api/docs</a> for Swagger UI</p>
    """

def get_similar_tracks():
    track_id = request.args.get('track_id')
    top_n = request.args.get('top_n', default=10, type=int)

    if not track_id:
        return jsonify({"error": "track_id is required"}), 400

    try:
        uuid.UUID(track_id) 
    except ValueError:
        return jsonify({"error": "track_id must be a valid UUID"}), 400

    try:
        similar_tracks = get_similar_tracks_use_case(track_id, top_n)
        
        if not similar_tracks:
            return jsonify({"error": "Track not found"}), 404
            
        return jsonify(similar_tracks)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# def clear_cache():
#     track_id = request.args.get('track_id')

#     if not track_id:
#         return jsonify({"error": "track_id is required"}), 400

#     try:
#         uuid.UUID(track_id)
#     except ValueError:
#         return jsonify({"error": "track_id must be a valid UUID"}), 400

#     try:
#         redis_cache.delete_similar_tracks(track_id)
#         return jsonify({"message": f"Cache cleared for track_id: {track_id}"}), 200
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500
