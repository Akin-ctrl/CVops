import json
import os
import sys
import time
import io
import logging
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from minio import Minio
from minio.error import S3Error
from prometheus_client import Counter, Gauge, Histogram
import threading

# Add parent directory to path for common imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from common.health import start_health_server

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "yolo-data-output")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "dlq-storage-errors")
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "minio-writer-group")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8003"))

# MinIO Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000") 
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET_NAME = os.getenv("BUCKET_NAME", "detections-data")
SECURE_CONNECTION = os.getenv("SECURE_CONNECTION", "False").lower() == "true"

# Batch Configuration
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100")) # Upload after 100 messages...
BATCH_TIMEOUT_SEC = int(os.getenv("BATCH_TIMEOUT_SEC", "60")) # ...or after 60 seconds

# Prometheus metrics
messages_consumed = Counter('corvision_kafka_messages_consumed_total', 'Kafka messages consumed', ['service', 'topic'])
minio_batches_written = Counter('corvision_minio_batches_written_total', 'Batches written to MinIO', ['service'])
minio_records_written = Counter('corvision_minio_records_written_total', 'Records written to MinIO', ['service'])
minio_write_duration = Histogram('corvision_minio_write_duration_seconds', 'MinIO write duration', ['service'],
                                   buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0])
errors_total = Counter('corvision_errors_total', 'Total errors', ['service', 'error_type'])
service_up = Gauge('corvision_service_up', 'Service health', ['service'])

SERVICE_NAME = 'minio-writer'

# Global state for health checks
health_state = {
    'kafka_consumer_connected': False,
    'kafka_producer_connected': False,
    'minio_connected': False,
    'writing_data': False
}

def start_metrics_server():
    """Start HTTP server with health, readiness, and metrics endpoints."""
    try:
        start_health_server(METRICS_PORT, SERVICE_NAME, health_state)
    except Exception as e:
        logging.error(f"Failed to start HTTP server: {e}")
        service_up.labels(service=SERVICE_NAME).set(0)

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
        health_state['minio_connected'] = True
        return client
    except Exception as e:
        health_state['minio_connected'] = False
        logging.error(f"MinIO connection failed: {e}")
        return None

def get_kafka_consumer():
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            group_id=CONSUMER_GROUP_ID,
            auto_offset_reset='latest',
            enable_auto_commit=False,  # Manual commits for reliability
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        health_state['kafka_consumer_connected'] = True
        logging.info(f"Kafka consumer connected to {KAFKA_TOPIC} (manual commits)")
        return consumer
    except Exception as e:
        health_state['kafka_consumer_connected'] = False
        logging.error(f"Kafka connection failed: {e}")
        return None

def get_kafka_producer():
    try:
        producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER])
        health_state['kafka_producer_connected'] = True
        logging.info(f"Kafka producer connected to {KAFKA_BROKER}")
        return producer
    except Exception as e:
        health_state['kafka_producer_connected'] = False
        logging.error(f"Kafka producer connection failed: {e}")
        return None

def upload_batch(minio_client, batch_data):
    if not batch_data:
        return

    start_time = time.time()
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
        
        # Update metrics and health
        duration = time.time() - start_time
        minio_batches_written.labels(service=SERVICE_NAME).inc()
        minio_records_written.labels(service=SERVICE_NAME).inc(len(batch_data))
        minio_write_duration.labels(service=SERVICE_NAME).observe(duration)
        health_state['writing_data'] = True
        
        logging.info(f"Uploaded batch of {len(batch_data)} records to {object_name} in {duration:.2f}s")
        return True
        
    except Exception as e:
        logging.error(f"Failed to upload batch to MinIO: {e}")
        errors_total.labels(service=SERVICE_NAME, error_type=type(e).__name__).inc()
        health_state['minio_connected'] = False
        return False

def main():
    # Start HTTP server in background (health, ready, metrics)
    server_thread = threading.Thread(target=start_metrics_server, daemon=True)
    server_thread.start()
    
    minio_client = get_minio_client()
    consumer = get_kafka_consumer()
    producer = get_kafka_producer()

    if not minio_client or not consumer or not producer:
        service_up.labels(service=SERVICE_NAME).set(0)
        logging.error("Exiting: Cannot connect to required services")
        return

    service_up.labels(service=SERVICE_NAME).set(1)
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
                    messages_consumed.labels(service=SERVICE_NAME, topic=KAFKA_TOPIC).inc()

            current_time = time.time()
            time_since_upload = current_time - last_upload_time

            # Check Trigger: Batch Size OR Time Limit
            if len(batch_buffer) >= BATCH_SIZE or (time_since_upload >= BATCH_TIMEOUT_SEC and len(batch_buffer) > 0):
                try:
                    success = upload_batch(minio_client, batch_buffer)
                    if success:
                        # Manual commit after successful upload
                        consumer.commit()
                        batch_buffer = []  # Clear buffer
                        last_upload_time = current_time
                    else:
                        # Send failed batch metadata to DLQ
                        try:
                            dlq_message = {
                                'error': 'Failed to upload batch to MinIO',
                                'error_type': 'MinIOUploadError',
                                'batch_size': len(batch_buffer),
                                'timestamp': time.time(),
                                'service': SERVICE_NAME
                            }
                            producer.send(KAFKA_DLQ_TOPIC, json.dumps(dlq_message).encode())
                            logging.warning(f"Sent upload failure to DLQ: {KAFKA_DLQ_TOPIC}")
                        except Exception as dlq_error:
                            logging.error(f"Failed to send to DLQ: {dlq_error}")
                        
                        # Still clear buffer to avoid infinite retry loop
                        batch_buffer = []
                        last_upload_time = current_time
                        
                except Exception as batch_error:
                    logging.error(f"Batch processing error: {batch_error}")
                    errors_total.labels(service=SERVICE_NAME, error_type=type(batch_error).__name__).inc()
                    batch_buffer = []  # Clear to prevent blocking

    except KeyboardInterrupt:
        logging.info("Stopping...")
        # Upload remaining data on exit
        if batch_buffer:
            upload_batch(minio_client, batch_buffer)
    except Exception as e:
        logging.error(f"Error in main loop: {e}")
        errors_total.labels(service=SERVICE_NAME, error_type=type(e).__name__).inc()
    finally:
        service_up.labels(service=SERVICE_NAME).set(0)
        health_state['kafka_consumer_connected'] = False
        health_state['kafka_producer_connected'] = False
        health_state['minio_connected'] = False
        health_state['writing_data'] = False
        if consumer:
            consumer.close()
        if producer:
            producer.close()
        logging.info("Cleanup complete")

if __name__ == "__main__":
    main()