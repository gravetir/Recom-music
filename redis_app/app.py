from flask import Flask
from api.routes import configure_routes
from flask_swagger_ui import get_swaggerui_blueprint
from flask_cors import CORS
from services.update_dataset import run_nightly_update, update_dataset
from infrastructure.data_loader import load_data
def create_app():
    app = Flask(__name__)
    CORS(app)
    
    configure_routes(app)
    run_nightly_update()
    update_dataset()
    SWAGGER_URL = '/api/docs'
    API_URL = '/static/swagger.json'
    swaggerui_blueprint = get_swaggerui_blueprint(
        SWAGGER_URL,
        API_URL,
        config={'app_name': "Music Recommendation API"}
    )
    app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)
    
    return app

if __name__ == "__main__":
    df, feature_matrix, df_genres, df_tags, df_moods = load_data()
    print("Данные загружены и обработаны")
    app = create_app()
    app.run(host='0.0.0.0', port=5000, debug=True)
