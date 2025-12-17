import cv2
import numpy as np
from kafka import KafkaConsumer, KafkaProducer 
import logging
import os
import sys
import time
from dotenv import load_dotenv
from prometheus_client import Counter, Gauge
import threading

# Add parent directory to path for common imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from common.health import start_health_server

load_dotenv() 

# --- Configuration ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "esp32-video")
KAFKA_OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "yolo-input-frames") 
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "dlq-preprocessing-errors")
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID_1", "preprocessor-group")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8001"))

# YOLO's expected input size - IMPORTANT: set these based on your YOLOv11 model config
YOLO_INPUT_SIZE_WIDTH = int(os.getenv("YOLO_INPUT_SIZE_WIDTH", "640"))
YOLO_INPUT_SIZE_HEIGHT = int(os.getenv("YOLO_INPUT_SIZE_HEIGHT", "640"))
TARGET_YOLO_SIZE = (YOLO_INPUT_SIZE_WIDTH, YOLO_INPUT_SIZE_HEIGHT)

# JPEG quality for output frames (0-100)
OUTPUT_JPEG_QUALITY = int(os.getenv("OUTPUT_JPEG_QUALITY", "85"))

# Enable CLAHE preprocessing for better detection quality in varying lighting
# Set to "false" to use passthrough mode (faster but may miss detections in poor lighting)
ENABLE_CLAHE = os.getenv("ENABLE_CLAHE", "true").lower() == "true"

# Prometheus metrics
frames_processed = Counter('corvision_frames_processed_total', 'Total frames processed', ['service'])
messages_consumed = Counter('corvision_kafka_messages_consumed_total', 'Kafka messages consumed', ['service', 'topic'])
messages_produced = Counter('corvision_kafka_messages_produced_total', 'Kafka messages produced', ['service', 'topic'])
processing_latency = Gauge('corvision_processing_latency_ms', 'Processing latency in ms', ['service'])
errors_total = Counter('corvision_errors_total', 'Total errors', ['service', 'error_type'])
service_up = Gauge('corvision_service_up', 'Service health', ['service'])

SERVICE_NAME = 'preprocessor'

# Global state for health checks
health_state = {
    'kafka_consumer_connected': False,
    'kafka_producer_connected': False,
    'processing_messages': False
}

def start_metrics_server():
    """Start HTTP server with health, readiness, and metrics endpoints."""
    try:
        start_health_server(METRICS_PORT, SERVICE_NAME, health_state)
    except Exception as e:
        logging.error(f"Failed to start HTTP server: {e}")
        service_up.labels(service=SERVICE_NAME).set(0)

# --- Kafka Client Creation ---
def create_kafka_consumer(kafka_broker, topic, group_id):
    """Creates a Kafka consumer instance with manual commit."""
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[kafka_broker],
            group_id=group_id,
            api_version=(0, 10, 1),
            auto_offset_reset='latest',
            enable_auto_commit=False,  # Manual commits for reliability
            value_deserializer=lambda m: m # Messages are raw bytes (JPEG)
        )
        health_state['kafka_consumer_connected'] = True
        logging.info(f"Consumer for topic '{topic}' connected to {kafka_broker} (group: '{group_id}', manual commits)")
        return consumer
    except Exception as e:
        health_state['kafka_consumer_connected'] = False
        logging.error(f"Error creating Kafka consumer: {e}")
        return None

def create_kafka_producer(kafka_broker):
    """Creates a Kafka producer instance."""
    try:
        producer = KafkaProducer(bootstrap_servers=[kafka_broker])
        health_state['kafka_producer_connected'] = True
        logging.info(f"Kafka producer connected to {kafka_broker}")
        return producer
    except Exception as e:
        health_state['kafka_producer_connected'] = False
        logging.error(f"Error creating Kafka producer: {e}")
        return None

# --- Preprocessing Logic ---
def preprocess_frame(frame_bytes):
    """
    Decodes JPEG frame bytes and performs a set of preprocessing steps for YOLOv11.

    Args:
        frame_bytes (bytes): Raw JPEG image bytes received from Kafka.

    Returns:
        np.ndarray: The preprocessed frame as a NumPy array (RGB, float32, [0,1] range),
                    or None if decoding fails.
    """
    try:
        #  Decode JPEG bytes to OpenCV image (BGR format)
        nparr = np.frombuffer(frame_bytes, np.uint8)
        frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        if frame is None:
            logging.warning("Failed to decode frame from bytes.")
            return None

        # Adaptive Contrast Enhancement (CLAHE) for varying lighting/weather
        # Convert BGR to LAB color space (L channel for lightness)
        lab = cv2.cvtColor(frame, cv2.COLOR_BGR2LAB)
        l_channel, a_channel, b_channel = cv2.split(lab)

        # Apply CLAHE to the L-channel
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8,8)) # Tunable: clipLimit, tileGridSize
        cl = clahe.apply(l_channel)

        # Merge the CLAHE enhanced L-channel back with original A and B channels
        limg = cv2.merge([cl, a_channel, b_channel])
        frame = cv2.cvtColor(limg, cv2.COLOR_LAB2BGR) # Convert back to BGR

        # Convert Color Space: BGR to RGB (standard for most ML models)
        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)

        # Resizing to YOLO's expected input dimensions
        resized_frame = cv2.resize(frame, TARGET_YOLO_SIZE, interpolation=cv2.INTER_AREA)

        # Normalize pixel values to [0, 1] and convert to float32
        normalized_frame = resized_frame.astype(np.float32) / 255.0

        # Output will be a NumPy array of shape (HEIGHT, WIDTH, CHANNELS), RGB, float32, range [0, 1]
        return normalized_frame

    except Exception as e:
        logging.error(f"Error during frame preprocessing: {e}")
        return None

# --- Main Logic ---
def main():
    """Main function to consume frames from Kafka, preprocess them, and publish to another topic."""
    logging.info("Starting Kafka preprocessor consumer script...")
    
    # Start metrics server in background
    metrics_thread = threading.Thread(target=start_metrics_server, daemon=True)
    metrics_thread.start()

    # Create Kafka Consumer
    consumer = create_kafka_consumer(KAFKA_BROKER, KAFKA_INPUT_TOPIC, CONSUMER_GROUP_ID)
    if not consumer:
        service_up.labels(service=SERVICE_NAME).set(0)
        return

    # Create Kafka Producer for output
    producer = create_kafka_producer(KAFKA_BROKER)
    if not producer:
        if consumer: consumer.close()
        service_up.labels(service=SERVICE_NAME).set(0)
        return

    service_up.labels(service=SERVICE_NAME).set(1)
    frame_count = 0
    last_flush_time = time.time()
    
    try:
        while True:
            start_time = time.time()
            
            # Poll in batches and only keep the latest frame to catch up when behind
            records = consumer.poll(timeout_ms=100, max_records=50)
            
            if not records:
                continue
            
            # Get only the latest message across all partitions
            latest_message = None
            for partition_records in records.values():
                if partition_records:
                    latest_message = partition_records[-1]  # Keep only the latest
                    messages_consumed.labels(service=SERVICE_NAME, topic=KAFKA_INPUT_TOPIC).inc(len(partition_records))
            
            if latest_message is None:
                continue
                
            frame_count += 1
            frames_processed.labels(service=SERVICE_NAME).inc()
            
            try:
                # Choose processing mode based on ENABLE_CLAHE flag
                if ENABLE_CLAHE:
                    # Apply CLAHE preprocessing for better quality in varying lighting
                    preprocessed_frame = preprocess_frame(latest_message.value)
                    
                    if preprocessed_frame is not None:
                        # Convert back to uint8 [0-255] range
                        frame_uint8 = (preprocessed_frame * 255).astype(np.uint8)
                        
                        # Encode as JPEG with quality setting
                        success, encoded_frame = cv2.imencode('.jpg', 
                                                             cv2.cvtColor(frame_uint8, cv2.COLOR_RGB2BGR),
                                                             [cv2.IMWRITE_JPEG_QUALITY, OUTPUT_JPEG_QUALITY])
                        
                        if success:
                            producer.send(KAFKA_OUTPUT_TOPIC, encoded_frame.tobytes())
                        else:
                            raise Exception("Failed to encode preprocessed frame")
                    else:
                        raise Exception("Preprocessing returned None")
                else:
                    # Passthrough mode - forward raw frame (faster, YOLO handles preprocessing)
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
                    logging.info(f"Forwarded frame {frame_count} ({mode}), offset: {latest_message.offset}, latency: {latency_ms:.2f}ms")
                    
            except Exception as process_error:
                # Send failed message to DLQ with error metadata
                logging.error(f"Failed to process message at offset {latest_message.offset}: {process_error}")
                errors_total.labels(service=SERVICE_NAME, error_type=type(process_error).__name__).inc()
                
                try:
                    # Add error metadata to DLQ message
                    import json
                    dlq_message = {
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
        logging.info("Script interrupted by user.")
    except Exception as e:
        logging.error(f"Error in main loop: {e}")
        errors_total.labels(service=SERVICE_NAME, error_type=type(e).__name__).inc()
    finally:
        service_up.labels(service=SERVICE_NAME).set(0)
        health_state['kafka_consumer_connected'] = False
        health_state['kafka_producer_connected'] = False
        health_state['processing_messages'] = False
        logging.info("Closing Kafka consumer and producer...")
        if consumer:
            consumer.close()
        if producer:
            producer.close()
        logging.info("Script finished.")

if __name__ == "__main__":
    main()