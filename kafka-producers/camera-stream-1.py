import cv2
import logging
import time
import os
from kafka import KafkaProducer
from dotenv import load_dotenv
from prometheus_client import Counter, Gauge, start_http_server
import threading

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "esp32-video")
URL = os.getenv("URL")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

logging.info(f"URL: {URL}")
logging.info(f"KAFKA_BROKER: {KAFKA_BROKER}")
logging.info(f"KAFKA_TOPIC: {KAFKA_TOPIC}")

# Prometheus metrics
frames_produced = Counter('corvision_frames_processed_total', 'Total frames produced', ['service'])
messages_produced = Counter('corvision_kafka_messages_produced_total', 'Kafka messages produced', ['service', 'topic'])
processing_latency = Gauge('corvision_processing_latency_ms', 'Processing latency in ms', ['service'])
errors_total = Counter('corvision_errors_total', 'Total errors', ['service', 'error_type'])
service_up = Gauge('corvision_service_up', 'Service health', ['service'])

SERVICE_NAME = 'kafka-producer'

def start_metrics_server():
    """Start Prometheus metrics server in background thread."""
    try:
        start_http_server(METRICS_PORT)
        service_up.labels(service=SERVICE_NAME).set(1)
        logging.info(f"Metrics server started on port {METRICS_PORT}")
    except Exception as e:
        logging.error(f"Failed to start metrics server: {e}")
        service_up.labels(service=SERVICE_NAME).set(0)

def create_kafka_producer(kafka_broker):
    try:
        producer = KafkaProducer(bootstrap_servers=[kafka_broker])
        logging.info(f"Kafka producer connected to {kafka_broker}")
        return producer
    except Exception as e:
        logging.error(f"Kafka producer error: {e}")
        return None

def capture_stream(url):
    cap = cv2.VideoCapture(url)
    if not cap.isOpened():
        logging.error(f"Cannot open stream {url}")
        return None
    logging.info(f"Stream opened: {url}")
    return cap

def read_frame(cap, timeout=5):
    start = time.time()
    while True:
        ret, frame = cap.read()
        if ret:
            return frame
        if time.time() - start > timeout:
            return None

def publish_frame(producer, topic, frame):
    start_time = time.time()
    try:
        _, buffer = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
        producer.send(topic, buffer.tobytes())
        producer.flush()
        
        # Update metrics
        frames_produced.labels(service=SERVICE_NAME).inc()
        messages_produced.labels(service=SERVICE_NAME, topic=topic).inc()
        latency_ms = (time.time() - start_time) * 1000
        processing_latency.labels(service=SERVICE_NAME).set(latency_ms)
        
        return True
    except Exception as e:
        logging.error(f"Publish error: {e}")
        errors_total.labels(service=SERVICE_NAME, error_type=type(e).__name__).inc()
        return False

def main():
    # Start metrics server in background
    metrics_thread = threading.Thread(target=start_metrics_server, daemon=True)
    metrics_thread.start()
    
    producer = create_kafka_producer(KAFKA_BROKER)
    if not producer:
        service_up.labels(service=SERVICE_NAME).set(0)
        return

    cap = capture_stream(URL)
    if not cap:
        service_up.labels(service=SERVICE_NAME).set(0)
        return

    service_up.labels(service=SERVICE_NAME).set(1)
    
    try:
        while True:
            frame = read_frame(cap, timeout=5)
            if frame is None:
                logging.warning("Failed to read frame. Reconnecting...")
                errors_total.labels(service=SERVICE_NAME, error_type='FrameReadError').inc()
                cap = capture_stream(URL)
                if not cap:
                    break
                continue
            publish_frame(producer, KAFKA_TOPIC, frame)
            time.sleep(0.033)  # 30 FPS
    except KeyboardInterrupt:
        logging.info("Stopped by user")
    finally:
        service_up.labels(service=SERVICE_NAME).set(0)
        if cap:
            cap.release()
        if producer:
            producer.close()
        logging.info("Resources released")

if __name__ == "__main__":
    main()
