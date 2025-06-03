from flask import Flask
from flask_cors import CORS
from dotenv import load_dotenv
from flasgger import Swagger

# Загрузка .env в самом начале
load_dotenv()  # Добавьте это перед импортом модулей, которые используют переменные окружения

from api.routes import bp as api_bp
from services.kafka_service import run_kafka_consumer
from threading import Thread
from config import Config

app = Flask(__name__)
app.config.from_object(Config)
CORS(app)
swagger = Swagger(app)
app.register_blueprint(api_bp)

if __name__ == '__main__':
    print("Starting Beat Processor Service...")
    consumer_thread = Thread(target=run_kafka_consumer, daemon=True)
    consumer_thread.start()
    app.run(
        host=app.config['FLASK_APP_HOST'],
        port=app.config['FLASK_APP_PORT'],
        debug=app.config['DEBUG']
    )