class Config:
    KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
    REC_BEATS_TOPIC = "rec_beats_topic2"
    REFILL_TOPIC = "rec_refill_requests"
    REFILL_THRESHOLD = 5
    REFILL_COUNT = 9
    REFILL_TIMEOUT = 60  
    CLEANUP_INTERVAL = 3600  
    SERVICE_PORT = 8002
    SERVICE_HOST = '0.0.0.0'