import cv2
import logging
import time
import os
import sys
from kafka import KafkaProducer
from dotenv import load_dotenv
from prometheus_client import Counter, Gauge
import threading

# Add parent directory to path for common imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from common.health import start_health_server

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

# Global state for health checks
health_state = {
    'kafka_connected': False,
    'camera_connected': False,
    'frames_flowing': False
}

def start_metrics_server():
    """Start HTTP server with health, readiness, and metrics endpoints."""
    try:
        start_health_server(METRICS_PORT, SERVICE_NAME, health_state)
    except Exception as e:
        logging.error(f"Failed to start HTTP server: {e}")
        service_up.labels(service=SERVICE_NAME).set(0)

def create_kafka_producer(kafka_broker):
    try:
        producer = KafkaProducer(bootstrap_servers=[kafka_broker])
        health_state['kafka_connected'] = True
        logging.info(f"Kafka producer connected to {kafka_broker}")
        return producer
    except Exception as e:
        health_state['kafka_connected'] = False
        logging.error(f"Kafka producer error: {e}")
        return None

def capture_stream(url):
    cap = cv2.VideoCapture(url)
    if not cap.isOpened():
        health_state['camera_connected'] = False
        logging.error(f"Cannot open stream {url}")
        return None
    health_state['camera_connected'] = True
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
        
        # Update metrics and health state
        frames_produced.labels(service=SERVICE_NAME).inc()
        messages_produced.labels(service=SERVICE_NAME, topic=topic).inc()
        latency_ms = (time.time() - start_time) * 1000
        processing_latency.labels(service=SERVICE_NAME).set(latency_ms)
        health_state['frames_flowing'] = True
        
        return True
    except Exception as e:
        logging.error(f"Publish error: {e}")
        errors_total.labels(service=SERVICE_NAME, error_type=type(e).__name__).inc()
        health_state['kafka_connected'] = False
        return False

def main():
    # Start HTTP server in background (health, ready, metrics)
    server_thread = threading.Thread(target=start_metrics_server, daemon=True)
    server_thread.start()
    
    producer = create_kafka_producer(KAFKA_BROKER)
    if not producer:
        service_up.labels(service=SERVICE_NAME).set(0)
        logging.error("Exiting: Cannot connect to Kafka")
        return

    cap = capture_stream(URL)
    if not cap:
        service_up.labels(service=SERVICE_NAME).set(0)
        logging.error("Exiting: Cannot open camera stream")
        return

    service_up.labels(service=SERVICE_NAME).set(1)
    
    try:
        while True:
            frame = read_frame(cap, timeout=5)
            if frame is None:
                logging.warning("Failed to read frame. Reconnecting...")
                errors_total.labels(service=SERVICE_NAME, error_type='FrameReadError').inc()
                health_state['camera_connected'] = False
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
        health_state['kafka_connected'] = False
        health_state['camera_connected'] = False
        if cap:
            cap.release()
        if producer:
            producer.close()
        logging.info("Resources released")

if __name__ == "__main__":
    main()
