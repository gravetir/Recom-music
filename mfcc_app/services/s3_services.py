import boto3
import os
from botocore.exceptions import ClientError
import tempfile

# Загрузка переменных окружения
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME', 'music-beats-bucket')
MP3_FOLDER = os.getenv('MP3_FOLDER', 'mp3/')

# print(f"[DEBUG] AWS_ACCESS_KEY_ID: {AWS_ACCESS_KEY_ID}")
# print(f"[DEBUG] AWS_SECRET_ACCESS_KEY: {AWS_SECRET_ACCESS_KEY}")
# print(f"[DEBUG] BUCKET_NAME: {BUCKET_NAME}")

if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME]):
    raise EnvironmentError("Missing required AWS credentials or S3 bucket name.")

# Инициализация клиента S3
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID.strip(),
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY.strip(),
    region_name='ru-central1'  # Замените на свой регион
)

def check_file_in_s3(filename):
    """Проверка существования файла в S3"""
    try:
        # Проверяем наличие объекта в S3
        s3.head_object(Bucket=BUCKET_NAME, Key=f"{MP3_FOLDER}{filename}")
        return True
    except ClientError as e:
        # Логируем ошибку и возвращаем False, если файл не найден
        print(f"[ERROR] S3 Head Object failed for {filename}: {e}")
        return False

def download_audio_from_s3(filename):
    """Загрузка аудио из S3 в временный файл"""
    try:
        # Создаем временный файл
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            s3.download_fileobj(Bucket=BUCKET_NAME, Key=f"{MP3_FOLDER}{filename}", Fileobj=temp_file)
            # Возвращаем путь к временно созданному файлу
            return temp_file.name
    except Exception as e:
        # Логируем ошибку при загрузке
        print(f"[ERROR] Audio download error for {filename}: {e}")
        return None
