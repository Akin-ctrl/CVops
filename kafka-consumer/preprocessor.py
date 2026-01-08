"""
preprocessor.py - Kafka Frame Preprocessing Service
===================================================
Consumes raw frames from Kafka, applies preprocessing, and publishes to YOLO input topic.

Features:
- CLAHE contrast enhancement for varying lighting conditions
- Motion detection to skip redundant frames (adaptive sampling)
- JPEG quality and size optimization
- Dead Letter Queue (DLQ) for failed messages
- Prometheus metrics and health checks
- Manual commit mode for reliable processing
- Passthrough mode option for performance

Processing Modes:
    CLAHE mode: Applies contrast enhancement + resize + normalize (higher quality)
    Passthrough mode: Forwards raw frames (faster, YOLO does preprocessing)

Environment Variables:
    KAFKA_BROKER: Kafka bootstrap server
    KAFKA_INPUT_TOPIC: Topic to consume raw frames from
    KAFKA_OUTPUT_TOPIC: Topic to publish preprocessed frames to
    KAFKA_DLQ_TOPIC: Dead letter queue for failed messages
    CONSUMER_GROUP_ID_1: Consumer group ID
    METRICS_PORT: Port for health/metrics HTTP server
    YOLO_INPUT_SIZE_WIDTH: Target width for YOLO input
    YOLO_INPUT_SIZE_HEIGHT: Target height for YOLO input
    OUTPUT_JPEG_QUALITY: JPEG compression quality (0-100)
    ENABLE_CLAHE: Enable CLAHE preprocessing (true/false)
    ENABLE_MOTION_DETECTION: Enable motion detection (true/false)
    MOTION_THRESHOLD: Motion threshold percentage (0-100)
    MIN_FRAME_INTERVAL: Minimum seconds between frames
"""

import cv2
import numpy as np
from kafka import KafkaConsumer, KafkaProducer 
import logging
import os
import sys
import time
import json
from typing import Optional, Dict, Tuple, List, Any
from dotenv import load_dotenv
from prometheus_client import Counter, Gauge
import threading

# Add parent directory to path for common imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from common.health import start_health_server

load_dotenv() 

# Configure logging
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Kafka configuration
KAFKA_BROKER: str = os.getenv("KAFKA_BROKER", "localhost:29092")
KAFKA_INPUT_TOPIC: str = os.getenv("KAFKA_INPUT_TOPIC", "esp32-video")
KAFKA_OUTPUT_TOPIC: str = os.getenv("KAFKA_OUTPUT_TOPIC", "yolo-input-frames") 
KAFKA_DLQ_TOPIC: str = os.getenv("KAFKA_DLQ_TOPIC", "dlq-preprocessing-errors")
CONSUMER_GROUP_ID: str = os.getenv("CONSUMER_GROUP_ID_1", "preprocessor-group")
METRICS_PORT: int = int(os.getenv("METRICS_PORT", "8001"))

# YOLO input configuration
YOLO_INPUT_SIZE_WIDTH: int = int(os.getenv("YOLO_INPUT_SIZE_WIDTH", "640"))
YOLO_INPUT_SIZE_HEIGHT: int = int(os.getenv("YOLO_INPUT_SIZE_HEIGHT", "640"))
TARGET_YOLO_SIZE: Tuple[int, int] = (YOLO_INPUT_SIZE_WIDTH, YOLO_INPUT_SIZE_HEIGHT)

# Image quality settings
OUTPUT_JPEG_QUALITY: int = int(os.getenv("OUTPUT_JPEG_QUALITY", "85"))

# Preprocessing toggles
ENABLE_CLAHE: bool = os.getenv("ENABLE_CLAHE", "true").lower() == "true"
ENABLE_MOTION_DETECTION: bool = os.getenv("ENABLE_MOTION_DETECTION", "true").lower() == "true"

# Motion detection parameters
MOTION_THRESHOLD: float = float(os.getenv("MOTION_THRESHOLD", "5.0"))
MIN_FRAME_INTERVAL: float = float(os.getenv("MIN_FRAME_INTERVAL", "0.5"))

# Prometheus metrics
frames_processed = Counter('corvision_frames_processed_total', 'Total frames processed', ['service'])
frames_skipped = Counter('corvision_frames_skipped_total', 'Total frames skipped by motion detection', ['service'])
motion_score = Gauge('corvision_motion_score_percent', 'Motion detection score (% pixels changed)', ['service'])
messages_consumed = Counter('corvision_kafka_messages_consumed_total', 'Kafka messages consumed', ['service', 'topic'])
messages_produced = Counter('corvision_kafka_messages_produced_total', 'Kafka messages produced', ['service', 'topic'])
processing_latency = Gauge('corvision_processing_latency_ms', 'Processing latency in ms', ['service'])
errors_total = Counter('corvision_errors_total', 'Total errors', ['service', 'error_type'])
service_up = Gauge('corvision_service_up', 'Service health', ['service'])
kafka_consumer_lag = Gauge('corvision_kafka_consumer_lag', 'Consumer lag (messages behind)', ['service', 'topic', 'partition'])

SERVICE_NAME: str = 'preprocessor'

# Global state for health checks
health_state: Dict[str, Any] = {
    'kafka_consumer_connected': False,
    'kafka_producer_connected': False,
    'processing_messages': False,
    'motion_detection_enabled': ENABLE_MOTION_DETECTION
}

# Motion detection state
previous_frame_gray: Optional[np.ndarray] = None
last_sent_time: float = 0


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


def create_kafka_consumer(kafka_broker: str, topic: str, group_id: str) -> Optional[KafkaConsumer]:
    """
    Create a Kafka consumer instance with manual commit mode.
    
    Args:
        kafka_broker: Kafka bootstrap server address.
        topic: Topic name to subscribe to.
        group_id: Consumer group ID for coordinated consumption.
    
    Returns:
        Connected KafkaConsumer instance, or None if connection fails.
    
    Side Effects:
        Updates health_state['kafka_consumer_connected'] flag.
    
    Notes:
        - Manual commits ensure at-least-once delivery semantics
        - Starts from latest offset on first run
        - Messages are raw bytes (JPEG encoded frames)
    """
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[kafka_broker],
            group_id=group_id,
            api_version=(0, 10, 1),
            auto_offset_reset='latest',
            enable_auto_commit=False,
            value_deserializer=lambda m: m
        )
        health_state['kafka_consumer_connected'] = True
        logging.info(f"Consumer for topic '{topic}' connected to {kafka_broker} (group: '{group_id}', manual commits)")
        return consumer
    except Exception as e:
        health_state['kafka_consumer_connected'] = False
        logging.error(f"Error creating Kafka consumer: {e}")
        return None


def create_kafka_producer(kafka_broker: str) -> Optional[KafkaProducer]:
    """
    Create a Kafka producer instance.
    
    Args:
        kafka_broker: Kafka bootstrap server address.
    
    Returns:
        Connected KafkaProducer instance, or None if connection fails.
    
    Side Effects:
        Updates health_state['kafka_producer_connected'] flag.
    """
    try:
        producer = KafkaProducer(bootstrap_servers=[kafka_broker])
        health_state['kafka_producer_connected'] = True
        logging.info(f"Kafka producer connected to {kafka_broker}")
        return producer
    except Exception as e:
        health_state['kafka_producer_connected'] = False
        logging.error(f"Error creating Kafka producer: {e}")
        return None


def preprocess_frame(frame_bytes: bytes) -> Optional[np.ndarray]:
    """
    Decode JPEG and apply preprocessing for YOLOv11 inference.
    
    Preprocessing Pipeline:
        1. Decode JPEG bytes to BGR image
        2. Apply CLAHE contrast enhancement in LAB color space
        3. Convert BGR -> RGB
        4. Resize to YOLO input dimensions
        5. Normalize to [0, 1] range as float32
    
    Args:
        frame_bytes: Raw JPEG image bytes from Kafka message.
    
    Returns:
        Preprocessed frame as numpy array (RGB, float32, [0,1] range),
        or None if decoding/processing fails.
    
    Notes:
        - CLAHE improves detection in varying lighting conditions
        - Output shape: (YOLO_HEIGHT, YOLO_WIDTH, 3)
        - CLAHE parameters tunable: clipLimit=2.0, tileGridSize=(8,8)
    """
    try:
        # Decode JPEG bytes to OpenCV image (BGR format)
        nparr = np.frombuffer(frame_bytes, np.uint8)
        frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        if frame is None:
            logging.warning("Failed to decode frame from bytes.")
            return None

        # Adaptive Contrast Enhancement (CLAHE) for varying lighting
        lab = cv2.cvtColor(frame, cv2.COLOR_BGR2LAB)
        l_channel, a_channel, b_channel = cv2.split(lab)

        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        cl = clahe.apply(l_channel)

        limg = cv2.merge([cl, a_channel, b_channel])
        frame = cv2.cvtColor(limg, cv2.COLOR_LAB2BGR)

        # Convert BGR to RGB (standard for ML models)
        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)

        # Resize to YOLO's expected input dimensions
        resized_frame = cv2.resize(frame, TARGET_YOLO_SIZE, interpolation=cv2.INTER_AREA)

        # Normalize to [0, 1] range
        normalized_frame = resized_frame.astype(np.float32) / 255.0

        return normalized_frame

    except Exception as e:
        logging.error(f"Error during frame preprocessing: {e}")
        return None


def calculate_motion_score(current_frame_gray: np.ndarray, previous_frame_gray: np.ndarray) -> float:
    """
    Calculate motion score using frame differencing.
    
    Args:
        current_frame_gray: Grayscale current frame.
        previous_frame_gray: Grayscale previous frame.
    
    Returns:
        Motion score as percentage of pixels changed (0-100).
    
    Algorithm:
        1. Compute absolute difference between frames
        2. Apply binary threshold (25) to detect significant changes
        3. Count changed pixels and convert to percentage
    
    Notes:
        - Returns 100.0 on error to prevent indefinite frame skipping
        - Threshold of 25 filters out noise and minor variations
    """
    try:
        frame_diff = cv2.absdiff(current_frame_gray, previous_frame_gray)
        _, thresh = cv2.threshold(frame_diff, 25, 255, cv2.THRESH_BINARY)
        
        total_pixels = thresh.shape[0] * thresh.shape[1]
        changed_pixels = cv2.countNonZero(thresh)
        motion_percentage = (changed_pixels / total_pixels) * 100
        
        return motion_percentage
    except Exception as e:
        logging.error(f"Error calculating motion score: {e}")
        return 100.0


def should_send_frame(
    current_frame_bytes: bytes, 
    previous_frame_gray_ref: List[Optional[np.ndarray]], 
    last_sent_time_ref: List[float]
) -> Tuple[bool, float, Optional[np.ndarray]]:
    """
    Determine if frame should be sent based on motion detection.
    
    Args:
        current_frame_bytes: Raw JPEG bytes of current frame.
        previous_frame_gray_ref: Mutable reference to previous grayscale frame (list with 1 element).
        last_sent_time_ref: Mutable reference to last sent timestamp (list with 1 element).
    
    Returns:
        Tuple of (should_send, motion_score, current_gray):
            - should_send: True if frame should be published
            - motion_score: Percentage of pixels changed (0-100)
            - current_gray: Current grayscale frame for next comparison
    
    Send Conditions:
        - Motion detection disabled: Always send
        - No previous frame exists: Always send
        - Motion score >= threshold: Send
        - Minimum time interval elapsed: Send (prevents indefinite skipping)
    
    Notes:
        - Frame resized to 640x360 for faster motion detection
        - Uses mutable list references to update state
        - Returns (True, 0.0, None) on any decode errors
    """
    if not ENABLE_MOTION_DETECTION:
        return True, 0.0, None
    
    try:
        # Decode frame to grayscale for motion detection
        nparr = np.frombuffer(current_frame_bytes, np.uint8)
        frame = cv2.imdecode(nparr, cv2.IMREAD_GRAYSCALE)
        
        if frame is None:
            return True, 0.0, None  # Send if decode fails
        
        # Resize to smaller size for faster motion detection
        frame_gray = cv2.resize(frame, (640, 360), interpolation=cv2.INTER_AREA)
        
        current_time = time.time()
        
        # Always send if no previous frame
        if previous_frame_gray_ref[0] is None:
            return True, 100.0, frame_gray
        
        # Calculate motion score
        score = calculate_motion_score(frame_gray, previous_frame_gray_ref[0])
        
        # Send if motion detected OR minimum interval elapsed
        time_since_last = current_time - last_sent_time_ref[0]
        should_send = (score >= MOTION_THRESHOLD) or (time_since_last >= MIN_FRAME_INTERVAL)
        
        return should_send, score, frame_gray
        
    except Exception as e:
        logging.error(f"Error in motion detection: {e}")
        return True, 0.0, None


def main() -> None:
    """
    Main preprocessing loop - consume, preprocess, and publish frames.
    
    Workflow:
        1. Start metrics server in background
        2. Connect Kafka consumer and producer
        3. Loop:
           a. Poll for messages (batch of 50, keep latest)
           b. Check motion detection (skip if static scene)
           c. Apply CLAHE preprocessing or passthrough
           d. Publish to output topic
           e. Manual commit offset
           f. Update metrics
        4. Send failed messages to DLQ
        5. Clean up resources on exit
    
    Processing Modes:
        - CLAHE enabled: Full preprocessing pipeline for better quality
        - CLAHE disabled: Passthrough mode for lower latency
    
    Exit Conditions:
        - KeyboardInterrupt (Ctrl+C)
        - Fatal connection failures
    
    Notes:
        - Keeps only latest frame when consumer lags behind
        - Batched producer flush every 100ms for efficiency
        - Tracks consumer lag per partition
    """
    logging.info("Starting Kafka preprocessor consumer script...")
    logging.info(f"Motion detection: {'ENABLED' if ENABLE_MOTION_DETECTION else 'DISABLED'}")
    logging.info(f"CLAHE preprocessing: {'ENABLED' if ENABLE_CLAHE else 'DISABLED (passthrough)'}")
    if ENABLE_MOTION_DETECTION:
        logging.info(f"Motion threshold: {MOTION_THRESHOLD}%, Min interval: {MIN_FRAME_INTERVAL}s")
    
    # Start metrics server in background
    metrics_thread = threading.Thread(target=start_metrics_server, daemon=True)
    metrics_thread.start()

    # Initialize Kafka consumer
    consumer = create_kafka_consumer(KAFKA_BROKER, KAFKA_INPUT_TOPIC, CONSUMER_GROUP_ID)
    if not consumer:
        service_up.labels(service=SERVICE_NAME).set(0)
        return

    # Initialize Kafka producer
    producer = create_kafka_producer(KAFKA_BROKER)
    if not producer:
        if consumer:
            consumer.close()
        service_up.labels(service=SERVICE_NAME).set(0)
        return

    service_up.labels(service=SERVICE_NAME).set(1)
    frame_count: int = 0
    last_flush_time: float = time.time()
    
    # Motion detection state (use lists for mutable references)
    prev_gray_ref: List[Optional[np.ndarray]] = [None]
    last_sent_ref: List[float] = [0.0]
    
    try:
        while True:
            start_time = time.time()
            
            # Poll in batches and only keep the latest frame to catch up when behind
            records = consumer.poll(timeout_ms=100, max_records=50)
            
            if not records:
                continue
            
            # Get only the latest message across all partitions
            latest_message = None
            for topic_partition, partition_records in records.items():
                if partition_records:
                    latest_message = partition_records[-1]  # Keep only the latest
                    messages_consumed.labels(service=SERVICE_NAME, topic=KAFKA_INPUT_TOPIC).inc(len(partition_records))
                    
                    # Track consumer lag
                    try:
                        end_offsets = consumer.end_offsets([topic_partition])
                        high_water_mark = end_offsets[topic_partition]
                        lag = high_water_mark - latest_message.offset - 1
                        kafka_consumer_lag.labels(
                            service=SERVICE_NAME,
                            topic=topic_partition.topic,
                            partition=str(topic_partition.partition)
                        ).set(max(0, lag))
                    except Exception as lag_error:
                        logging.debug(f"Failed to calculate lag: {lag_error}")
            
            if latest_message is None:
                continue
                
            frame_count += 1
            
            try:
                # Check if frame should be sent based on motion detection
                should_send, motion_score_value, current_gray = should_send_frame(
                    latest_message.value, prev_gray_ref, last_sent_ref
                )
                
                # Update motion score metric
                if ENABLE_MOTION_DETECTION:
                    motion_score.labels(service=SERVICE_NAME).set(motion_score_value)
                
                # Skip frame if no significant motion detected
                if not should_send:
                    frames_skipped.labels(service=SERVICE_NAME).inc()
                    consumer.commit()  # Still commit to advance offset
                    
                    if frame_count % 100 == 0:
                        logging.info(f"Skipped frame {frame_count} (motion: {motion_score_value:.2f}%)")
                    continue
                
                # Update motion detection state
                if current_gray is not None:
                    prev_gray_ref[0] = current_gray
                    last_sent_ref[0] = time.time()
                
                frames_processed.labels(service=SERVICE_NAME).inc()
                
                # Choose processing mode
                if ENABLE_CLAHE:
                    # CLAHE mode: Apply full preprocessing pipeline
                    preprocessed_frame = preprocess_frame(latest_message.value)
                    
                    if preprocessed_frame is not None:
                        # Convert back to uint8 [0-255] range
                        frame_uint8 = (preprocessed_frame * 255).astype(np.uint8)
                        
                        # Encode as JPEG with configured quality
                        success, encoded_frame = cv2.imencode(
                            '.jpg', 
                            cv2.cvtColor(frame_uint8, cv2.COLOR_RGB2BGR),
                            [cv2.IMWRITE_JPEG_QUALITY, OUTPUT_JPEG_QUALITY]
                        )
                        
                        if success:
                            producer.send(KAFKA_OUTPUT_TOPIC, encoded_frame.tobytes())
                        else:
                            raise Exception("Failed to encode preprocessed frame")
                    else:
                        raise Exception("Preprocessing returned None")
                else:
                    # Passthrough mode: Forward raw frame (YOLO handles preprocessing)
                    producer.send(KAFKA_OUTPUT_TOPIC, latest_message.value)
                
                messages_produced.labels(service=SERVICE_NAME, topic=KAFKA_OUTPUT_TOPIC).inc()
                health_state['processing_messages'] = True
                
                # Manual commit after successful processing
                consumer.commit()
                
                # Update latency metric
                latency_ms = (time.time() - start_time) * 1000
                processing_latency.labels(service=SERVICE_NAME).set(latency_ms)
                
                # Batch flush every 100ms instead of every frame
                if time.time() - last_flush_time > 0.1:
                    producer.flush()
                    last_flush_time = time.time()
                
                if frame_count % 30 == 0:
                    mode = "CLAHE preprocessing" if ENABLE_CLAHE else "passthrough"
                    motion_info = f", motion: {motion_score_value:.2f}%" if ENABLE_MOTION_DETECTION else ""
                    logging.info(f"Forwarded frame {frame_count} ({mode}), offset: {latest_message.offset}, latency: {latency_ms:.2f}ms{motion_info}")
                    
            except Exception as process_error:
                # Send failed message to DLQ with error metadata
                logging.error(f"Failed to process message at offset {latest_message.offset}: {process_error}")
                errors_total.labels(service=SERVICE_NAME, error_type=type(process_error).__name__).inc()
                
                try:
                    # Create error metadata for DLQ
                    dlq_message: Dict[str, Any] = {
                        'error': str(process_error),
                        'error_type': type(process_error).__name__,
                        'offset': latest_message.offset,
                        'partition': latest_message.partition,
                        'timestamp': time.time(),
                        'service': SERVICE_NAME
                    }
                    producer.send(KAFKA_DLQ_TOPIC, json.dumps(dlq_message).encode())
                    logging.warning(f"Sent failed message to DLQ: {KAFKA_DLQ_TOPIC}")
                except Exception as dlq_error:
                    logging.error(f"Failed to send to DLQ: {dlq_error}")
            
    except KeyboardInterrupt:
        logging.info("Script interrupted by user")
    except Exception as e:
        logging.error(f"Error in main loop: {e}")
        errors_total.labels(service=SERVICE_NAME, error_type=type(e).__name__).inc()
    finally:
        # Clean up resources
        service_up.labels(service=SERVICE_NAME).set(0)
        health_state['kafka_consumer_connected'] = False
        health_state['kafka_producer_connected'] = False
        health_state['processing_messages'] = False
        
        logging.info("Closing Kafka connections...")
        if consumer:
            consumer.close()
        if producer:
            producer.close()
        
        logging.info("Preprocessor script finished")

if __name__ == "__main__":
    main()