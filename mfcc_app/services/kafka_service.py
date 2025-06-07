from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio
import json
import time
from services.audio_service import analyze_audio
from services.s3_services import download_audio_from_s3
import os
import uuid
from botocore.exceptions import ClientError
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
KAFKA_PUBLISH_TOPIC = os.getenv('KAFKA_PUBLISH_TOPIC', 'publish_beat')
KAFKA_TRACK_TOPIC = os.getenv('KAFKA_TRACK_TOPIC', 'track_for_mfcc')

# Синхронный Kafka Producer для отправки сообщений
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=3
)

def send_kafka_message(topic, data):
    """Синхронная отправка сообщения в Kafka"""
    try:
        future = producer.send(topic, value=data)
        future.get(timeout=10) 
        return True
    except Exception as e:
        print(f"[ERROR] Failed to send Kafka message: {str(e)}")
        return False

# Асинхронный Kafka Consumer Worker
async def kafka_consumer_worker():
    print("[INIT] Starting AIOKafka consumer...")
    
    # Инициализация Kafka producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await producer.start()

    # Инициализация Kafka consumer
    consumer = AIOKafkaConsumer(
        KAFKA_TRACK_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id='beat-processor-group',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        max_poll_interval_ms=300000,  
        max_poll_records=1,           
        session_timeout_ms=30000,     
        heartbeat_interval_ms=3000    
    )

    try:
        await consumer.start()
        print(f"[OK] Consumer ready. Listening to {KAFKA_TRACK_TOPIC}")

        async for msg in consumer:
            try:
                data = msg.value
                filename = data['filename']
                beat_id = data.get('beat_id', str(uuid.uuid4()))
                
                print(f"[PROCESSING] New message received. Filename: {filename}, Beat ID: {beat_id}")

                print(f"[STAGE] Downloading audio for {filename} from S3...")
                audio_path = download_audio_from_s3(filename)
                
                if not audio_path:
                    error_msg = "Audio download failed"
                    print(f"[ERROR] {error_msg} for {filename}")
                    await producer.send_and_wait(KAFKA_PUBLISH_TOPIC, value={
                        "beat_id": beat_id,
                        "filename": filename,
                        "error": error_msg
                    })
                    continue

                print(f"[STAGE] Analyzing audio for {filename}...")
                features = analyze_audio(audio_path)

                if not features:
                    error_msg = "Audio analysis failed"
                    print(f"[ERROR] {error_msg} for {filename}")
                    await producer.send_and_wait(KAFKA_PUBLISH_TOPIC, value={
                        "beat_id": beat_id,
                        "filename": filename,
                        "error": error_msg
                    })
                    continue

                result = {
                    "beat_id": beat_id,
                    "filename": filename,
                    "features": features,
                    "error": ""
                }

                print(f"[STAGE] Sending result for {filename} to {KAFKA_PUBLISH_TOPIC}...")
                await producer.send_and_wait(KAFKA_PUBLISH_TOPIC, value=result)
                print(f"[SUCCESS] Successfully processed and sent result for {filename}")

            except Exception as e:
                print(f"[ERROR] Error processing message {msg.offset}: {str(e)}")
                await asyncio.sleep(1)

    except asyncio.CancelledError:
        print("[SHUTDOWN] Consumer stopped manually.")
    except Exception as e:
        print(f"[CRITICAL] Error in Kafka consumer: {str(e)}")
    finally:
        await consumer.stop()
        await producer.stop()
        print("[SHUTDOWN] Kafka consumer and producer stopped gracefully.")
        
        
def run_kafka_consumer():
    print("[KAFKA] Starting consumer thread...")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        loop.run_until_complete(kafka_consumer_worker())
    except KeyboardInterrupt:
        print("\n[KAFKA] Received shutdown signal")
    except Exception as e:
        print(f"[KAFKA] Unexpected error: {str(e)}")
    finally:
        tasks = asyncio.all_tasks(loop=loop)
        for task in tasks:
            task.cancel()
        loop.close()
        print("[KAFKA] Consumer thread stopped")