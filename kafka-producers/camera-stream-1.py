"""
camera-stream-1.py - Kafka Camera Frame Producer
=================================================
Captures video frames from a camera stream and publishes them to Kafka.

Features:
- Real-time frame capture from IP camera or video source
- JPEG compression with configurable quality
- Kafka message publishing with retry logic
- Prometheus metrics for monitoring
- Health check endpoints for orchestration
- Automatic reconnection on stream failures

Environment Variables:
    KAFKA_BROKER: Kafka bootstrap server address
    KAFKA_TOPIC: Topic to publish frames to
    URL: Camera stream URL
    METRICS_PORT: Port for health/metrics HTTP server
"""

import cv2
import logging
import time
import os
import sys
from typing import Optional, Dict, Any
from kafka import KafkaProducer
from dotenv import load_dotenv
from prometheus_client import Counter, Gauge
import threading
import numpy as np

# Add parent directory to path for common imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from common.health import start_health_server

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuration from environment variables
KAFKA_BROKER: str = os.getenv("KAFKA_BROKER", "localhost:29092")
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "esp32-video")
URL: Optional[str] = os.getenv("URL")
METRICS_PORT: int = int(os.getenv("METRICS_PORT", "8000"))
JPEG_QUALITY: int = int(os.getenv("JPEG_QUALITY", "80"))
TARGET_FPS: int = int(os.getenv("TARGET_FPS", "30"))
FRAME_READ_TIMEOUT: int = int(os.getenv("FRAME_READ_TIMEOUT", "5"))

logging.info(f"URL: {URL}")
logging.info(f"KAFKA_BROKER: {KAFKA_BROKER}")
logging.info(f"KAFKA_TOPIC: {KAFKA_TOPIC}")
logging.info(f"TARGET_FPS: {TARGET_FPS}")

# Prometheus metrics
frames_produced = Counter('corvision_frames_processed_total', 'Total frames produced', ['service'])
messages_produced = Counter('corvision_kafka_messages_produced_total', 'Kafka messages produced', ['service', 'topic'])
processing_latency = Gauge('corvision_processing_latency_ms', 'Processing latency in ms', ['service'])
errors_total = Counter('corvision_errors_total', 'Total errors', ['service', 'error_type'])
service_up = Gauge('corvision_service_up', 'Service health', ['service'])

SERVICE_NAME: str = 'kafka-producer'

# Global state for health checks
health_state: Dict[str, bool] = {
    'kafka_connected': False,
    'camera_connected': False,
    'frames_flowing': False
}


def start_metrics_server() -> None:
    """
    Start HTTP server with health, readiness, and metrics endpoints.
    
    Runs in background thread to expose Prometheus metrics and health status.
    Sets service_up metric to 0 on failure.
    """
    try:
        start_health_server(METRICS_PORT, SERVICE_NAME, health_state)
    except Exception as e:
        logging.error(f"Failed to start HTTP server: {e}")
        service_up.labels(service=SERVICE_NAME).set(0)


def create_kafka_producer(kafka_broker: str) -> Optional[KafkaProducer]:
    """
    Create and connect a Kafka producer instance.
    
    Args:
        kafka_broker: Kafka bootstrap server address.
    
    Returns:
        Connected KafkaProducer instance, or None if connection fails.
    
    Side Effects:
        Updates health_state['kafka_connected'] flag.
    """
    try:
        producer = KafkaProducer(bootstrap_servers=[kafka_broker])
        health_state['kafka_connected'] = True
        logging.info(f"Kafka producer connected to {kafka_broker}")
        return producer
    except Exception as e:
        health_state['kafka_connected'] = False
        logging.error(f"Kafka producer error: {e}")
        return None


def capture_stream(url: Optional[str]) -> Optional[cv2.VideoCapture]:
    """
    Open a video stream from camera or file.
    
    Args:
        url: Video stream URL or file path. Can be IP camera stream,
             RTSP URL, video file, or device index.
    
    Returns:
        OpenCV VideoCapture object if successful, None otherwise.
    
    Side Effects:
        Updates health_state['camera_connected'] flag.
    
    Notes:
        - For IP cameras, use format: http://ip:port/stream
        - For RTSP: rtsp://username:password@ip:port/path
        - For webcam: use integer (0, 1, etc.)
    """
    if url is None:
        health_state['camera_connected'] = False
        logging.error("No URL provided for camera stream")
        return None
    
    cap = cv2.VideoCapture(url)
    if not cap.isOpened():
        health_state['camera_connected'] = False
        logging.error(f"Cannot open stream {url}")
        return None
    
    health_state['camera_connected'] = True
    logging.info(f"Stream opened: {url}")
    return cap


def read_frame(cap: cv2.VideoCapture, timeout: int = 5) -> Optional[np.ndarray]:
    """
    Read a single frame from video stream with timeout.
    
    Args:
        cap: OpenCV VideoCapture object.
        timeout: Maximum seconds to wait for a valid frame.
    
    Returns:
        Frame as numpy array (BGR format), or None if timeout exceeded.
    
    Notes:
        - Retries frame reads until timeout
        - Useful for handling temporary stream interruptions
        - Returns None to signal reconnection needed
    """
    start = time.time()
    while True:
        ret, frame = cap.read()
        if ret:
            return frame
        if time.time() - start > timeout:
            return None


def publish_frame(producer: KafkaProducer, topic: str, frame: np.ndarray) -> bool:
    """
    Encode frame as JPEG and publish to Kafka topic.
    
    Args:
        producer: Connected Kafka producer instance.
        topic: Kafka topic name to publish to.
        frame: Frame as numpy array (BGR format from OpenCV).
    
    Returns:
        True if published successfully, False on error.
    
    Side Effects:
        - Increments Prometheus metrics
        - Updates health_state flags
        - Logs errors on failure
    
    Notes:
        - JPEG quality configured via JPEG_QUALITY constant
        - Calls flush() to ensure immediate delivery
        - Tracks latency in milliseconds
    """
    start_time = time.time()
    try:
        _, buffer = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, JPEG_QUALITY])
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


def main() -> None:
    """
    Main producer loop - capture frames and publish to Kafka.
    
    Workflow:
        1. Start metrics server in background
        2. Connect to Kafka
        3. Open camera stream
        4. Loop: read frame -> publish -> sleep
        5. Reconnect on stream failures
        6. Clean up resources on exit
    
    Exit Conditions:
        - KeyboardInterrupt (Ctrl+C)
        - Fatal connection failures
        - Stream reconnection failure
    """
def main() -> None:
    """
    Main producer loop - capture frames and publish to Kafka.
    
    Workflow:
        1. Start metrics server in background
        2. Connect to Kafka
        3. Open camera stream
        4. Loop: read frame -> publish -> sleep
        5. Reconnect on stream failures
        6. Clean up resources on exit
    
    Exit Conditions:
        - KeyboardInterrupt (Ctrl+C)
        - Fatal connection failures
        - Stream reconnection failure
    """
    # Start HTTP server in background (health, ready, metrics)
    server_thread = threading.Thread(target=start_metrics_server, daemon=True)
    server_thread.start()
    
    # Initialize Kafka connection
    producer = create_kafka_producer(KAFKA_BROKER)
    if not producer:
        service_up.labels(service=SERVICE_NAME).set(0)
        logging.error("Exiting: Cannot connect to Kafka")
        return

    # Initialize camera stream
    cap = capture_stream(URL)
    if not cap:
        service_up.labels(service=SERVICE_NAME).set(0)
        logging.error("Exiting: Cannot open camera stream")
        return

    service_up.labels(service=SERVICE_NAME).set(1)
    
    # Calculate frame delay from target FPS
    frame_delay: float = 1.0 / TARGET_FPS if TARGET_FPS > 0 else 0.033
    
    try:
        while True:
            frame = read_frame(cap, timeout=FRAME_READ_TIMEOUT)
            if frame is None:
                logging.warning("Failed to read frame. Reconnecting...")
                errors_total.labels(service=SERVICE_NAME, error_type='FrameReadError').inc()
                health_state['camera_connected'] = False
                cap = capture_stream(URL)
                if not cap:
                    break
                continue
            
            publish_frame(producer, KAFKA_TOPIC, frame)
            time.sleep(frame_delay)
            
    except KeyboardInterrupt:
        logging.info("Stopped by user")
    finally:
        # Clean up resources
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
