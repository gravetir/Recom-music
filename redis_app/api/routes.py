from flask import Blueprint
from api import controllers

def configure_routes(app):
    app.add_url_rule("/", view_func=controllers.home, methods=["GET"])
    app.add_url_rule("/similar_tracks", view_func=controllers.get_similar_tracks, methods=["GET"])
    # app.add_url_rule("/clear_cache", view_func=controllers.clear_cache, methods=["DELETE"])
