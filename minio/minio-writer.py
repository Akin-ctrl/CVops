import json
import os
import time
import io
import logging
from datetime import datetime
from kafka import KafkaConsumer
from minio import Minio
from minio.error import S3Error

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka Config
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "yolo-data-output")
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "minio-writer-group")

# MinIO Config
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000") # Inside Docker network
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET_NAME = os.getenv("BUCKET_NAME", "detections-data")
SECURE_CONNECTION = os.getenv("SECURE_CONNECTION", "False").lower() == "true"

# Batch Config
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100")) # Upload after 100 messages...
BATCH_TIMEOUT_SEC = int(os.getenv("BATCH_TIMEOUT_SEC", "60")) # ...or after 60 seconds

def get_minio_client():
    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=SECURE_CONNECTION
        )
        # Check if bucket exists, make if not
        if not client.bucket_exists(BUCKET_NAME):
            client.make_bucket(BUCKET_NAME)
            logging.info(f"Created bucket '{BUCKET_NAME}'")
        else:
            logging.info(f"Bucket '{BUCKET_NAME}' exists")
        return client
    except Exception as e:
        logging.error(f"MinIO connection failed: {e}")
        return None

def get_kafka_consumer():
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            group_id=CONSUMER_GROUP_ID,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        logging.info(f"Kafka consumer connected to {KAFKA_TOPIC}")
        return consumer
    except Exception as e:
        logging.error(f"Kafka connection failed: {e}")
        return None

def upload_batch(minio_client, batch_data):
    if not batch_data:
        return

    try:
        # Create a filename based on the current time: YYYY/MM/DD/hour_min_sec.json
        now = datetime.now()
        object_name = f"{now.year}/{now.month:02d}/{now.day:02d}/{now.strftime('%H-%M-%S')}_batch.json"
        
        # Convert batch list to JSON bytes
        data_bytes = json.dumps(batch_data, indent=2).encode('utf-8')
        data_stream = io.BytesIO(data_bytes)
        
        # Upload
        minio_client.put_object(
            BUCKET_NAME,
            object_name,
            data_stream,
            length=len(data_bytes),
            content_type="application/json"
        )
        logging.info(f"Uploaded batch of {len(batch_data)} records to {object_name}")
        
    except Exception as e:
        logging.error(f"Failed to upload batch to MinIO: {e}")

def main():
    minio_client = get_minio_client()
    consumer = get_kafka_consumer()

    if not minio_client or not consumer:
        return

    batch_buffer = []
    last_upload_time = time.time()

    logging.info("Starting MinIO writer loop...")

    try:
        # We use poll() instead of simple iteration to handle timeouts for batching
        while True:
            # Poll for messages (wait up to 1 second)
            msg_pack = consumer.poll(timeout_ms=1000)

            for tp, messages in msg_pack.items():
                for message in messages:
                    batch_buffer.append(message.value)

            current_time = time.time()
            time_since_upload = current_time - last_upload_time

            # Check Trigger: Batch Size OR Time Limit
            if len(batch_buffer) >= BATCH_SIZE or (time_since_upload >= BATCH_TIMEOUT_SEC and len(batch_buffer) > 0):
                upload_batch(minio_client, batch_buffer)
                batch_buffer = [] # Clear buffer
                last_upload_time = current_time

    except KeyboardInterrupt:
        logging.info("Stopping...")
        # Upload remaining data on exit
        if batch_buffer:
            upload_batch(minio_client, batch_buffer)
    finally:
        consumer.close()

if __name__ == "__main__":
    main()