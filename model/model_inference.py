"""
model_inference.py - YOLOv11 Object Detection Inference Service
================================================================
Consumes preprocessed frames from Kafka, runs YOLO inference, and publishes results.

Features:
- YOLOv11 object detection with GPU acceleration (if available)
- Real-time frame processing with aggressive catch-up
- Dual output: annotated frames + JSON detection data
- Object tracking support (optional, slower)
- GPU metrics and health monitoring
- Dead Letter Queue (DLQ) for failed frames
- Prometheus metrics for inference performance
- Background frame grabber for low-latency processing

Processing Pipeline:
    1. Background thread continuously grabs latest frame (skip old frames)
    2. Decode JPEG to numpy array
    3. Run YOLO inference (detect/track objects)
    4. Publish annotated frame to visual topic
    5. Publish JSON detections to data topic
    6. Manual commit offset after success

Environment Variables:
    KAFKA_BROKER: Kafka bootstrap server
    KAFKA_INPUT_TOPIC: Topic to consume frames from
    KAFKA_VISUAL_TOPIC: Topic for annotated frames
    KAFKA_DATA_TOPIC: Topic for JSON detection data
    KAFKA_DLQ_TOPIC: Dead letter queue for errors
    CONSUMER_GROUP_ID: Consumer group ID
    MODEL_WEIGHTS_PATH: Path to YOLO weights file
    DEVICE: Inference device (cpu/cuda/cuda:0)
    METRICS_PORT: Port for health/metrics HTTP server
    USE_TRACKING: Enable object tracking (true/false)
    INPUT_SIZE: YOLO input size (smaller = faster)
    CAMERA_ID: Camera identifier for JSON metadata
"""

import cv2
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
import logging
import os
import sys
import json
import time
from typing import Optional, Dict, Any, Tuple, List
from ultralytics import YOLO
from dotenv import load_dotenv
import threading
from queue import Queue, Empty
from prometheus_client import Counter, Gauge, Histogram

# Add parent directory to path for common imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from common.health import start_health_server
from common.gpu_utils import (
    check_cuda_available, get_gpu_info, get_gpu_stats,
    select_best_device, log_device_info, get_device_info_for_health
)

load_dotenv()

# Configure logging
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Kafka configuration
KAFKA_BROKER: str = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_INPUT_TOPIC: str = os.getenv("KAFKA_INPUT_TOPIC", "yolo-input-frames")
KAFKA_VISUAL_TOPIC: str = os.getenv("KAFKA_VISUAL_TOPIC", "yolo-visual-output")
KAFKA_DATA_TOPIC: str = os.getenv("KAFKA_DATA_TOPIC", "yolo-data-output")
KAFKA_DLQ_TOPIC: str = os.getenv("KAFKA_DLQ_TOPIC", "dlq-inference-errors")
CONSUMER_GROUP_ID: str = os.getenv("CONSUMER_GROUP_ID", "yolo-inference-group")

# Model configuration
MODEL_WEIGHTS_PATH: str = os.getenv("MODEL_WEIGHTS_PATH")
DEVICE: str = os.getenv("DEVICE")
METRICS_PORT: int = int(os.getenv("METRICS_PORT", "8002"))

# Performance tuning
USE_TRACKING: bool = os.getenv("USE_TRACKING", "false").lower() == "true"
INPUT_SIZE: int = int(os.getenv("INPUT_SIZE", "320"))
CAMERA_ID: str = os.getenv("CAMERA_ID", "esp32-cam-01")

# Prometheus metrics
frames_processed = Counter('corvision_frames_processed_total', 'Total frames processed', ['service'])
messages_consumed = Counter('corvision_kafka_messages_consumed_total', 'Kafka messages consumed', ['service', 'topic'])
messages_produced = Counter('corvision_kafka_messages_produced_total', 'Kafka messages produced', ['service', 'topic'])
processing_latency = Gauge('corvision_processing_latency_ms', 'Processing latency in ms', ['service'])
inference_fps = Gauge('corvision_inference_fps', 'Inference FPS', ['service'])
detections_total = Counter('corvision_detections_total', 'Total detections', ['class_name'])
detection_confidence = Histogram('corvision_detection_confidence', 'Detection confidence', ['class_name'], 
                                  buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0])
errors_total = Counter('corvision_errors_total', 'Total errors', ['service', 'error_type'])
service_up = Gauge('corvision_service_up', 'Service health', ['service'])

# GPU-specific metrics
gpu_available = Gauge('corvision_gpu_available', 'GPU availability', ['service'])
gpu_utilization = Gauge('corvision_gpu_utilization_percent', 'GPU utilization percentage', ['service', 'gpu_id'])
gpu_memory_used = Gauge('corvision_gpu_memory_used_mb', 'GPU memory used in MB', ['service', 'gpu_id'])
gpu_memory_total = Gauge('corvision_gpu_memory_total_mb', 'GPU memory total in MB', ['service', 'gpu_id'])
gpu_temperature = Gauge('corvision_gpu_temperature_celsius', 'GPU temperature in Celsius', ['service', 'gpu_id'])
inference_speedup = Gauge('corvision_inference_speedup_factor', 'Inference speedup vs CPU', ['service'])
kafka_consumer_lag = Gauge('corvision_kafka_consumer_lag', 'Consumer lag (messages behind)', ['service', 'topic', 'partition'])

SERVICE_NAME: str = 'yolo-inference'

# Global state for health checks
health_state: Dict[str, Any] = {
    'kafka_consumer_connected': False,
    'kafka_producer_connected': False,
    'model_loaded': False,
    'processing_frames': False,
    'gpu_available': False,
    'device_type': 'unknown'
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


def create_kafka_consumer(kafka_broker: str, topic: str, group_id: str) -> Optional[KafkaConsumer]:
    """
    Create a Kafka consumer with optimized settings for frame streaming.
    
    Args:
        kafka_broker: Kafka bootstrap server address.
        topic: Topic name to subscribe to.
        group_id: Consumer group ID.
    
    Returns:
        Connected KafkaConsumer instance, or None if connection fails.
    
    Side Effects:
        Updates health_state['kafka_consumer_connected'] flag.
    
    Notes:
        - Manual commits for at-least-once semantics
        - Optimized fetch sizes for large frames (1MB max)
        - Messages are raw bytes (JPEG encoded)
    """
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[kafka_broker],
            group_id=group_id,
            api_version=(0, 10, 1),
            auto_offset_reset='latest',
            enable_auto_commit=False,
            fetch_max_bytes=1048576,
            max_partition_fetch_bytes=524288,
            value_deserializer=lambda m: m
        )
        health_state['kafka_consumer_connected'] = True
        logging.info(f"Consumer connected to {topic} (manual commits)")
        return consumer
    except Exception as e:
        health_state['kafka_consumer_connected'] = False
        logging.error(f"Error creating consumer: {e}")
        return None


def create_kafka_producer(kafka_broker: str) -> Optional[KafkaProducer]:
    """
    Create a Kafka producer with LZ4 compression.
    
    Args:
        kafka_broker: Kafka bootstrap server address.
    
    Returns:
        Connected KafkaProducer instance, or None if connection fails.
    
    Side Effects:
        Updates health_state['kafka_producer_connected'] flag.
    
    Notes:
        - LZ4 compression for efficient bandwidth usage
        - Used for both visual and data outputs
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=[kafka_broker],
            api_version=(0, 10, 1),
            compression_type='lz4'
        )
        health_state['kafka_producer_connected'] = True
        logging.info(f"Producer connected to {kafka_broker}")
        return producer
    except Exception as e:
        health_state['kafka_producer_connected'] = False
        logging.error(f"Error creating producer: {e}")
        return None


class FrameGrabber(threading.Thread):
    """
    Background thread that continuously grabs the latest frame from Kafka.
    
    Aggressively polls for new frames and keeps only the most recent one,
    discarding older frames to minimize latency and allow inference to catch up.
    
    Attributes:
        consumer: KafkaConsumer instance to poll from.
        latest_frame: Most recent frame bytes (JPEG encoded).
        latest_offset: Kafka offset of latest frame.
        lock: Threading lock for safe concurrent access.
        running: Thread control flag.
    
    Notes:
        - Runs as daemon thread (auto-terminates on process exit)
        - Polls with 10ms timeout for low latency
        - Processes up to 500 records per poll batch
        - Tracks consumer lag per partition
        - Thread-safe access to latest frame via lock
    """
    
    def __init__(self, consumer: KafkaConsumer) -> None:
        """
        Initialize frame grabber thread.
        
        Args:
            consumer: Connected Kafka consumer instance.
        """
        super().__init__(daemon=True)
        self.consumer = consumer
        self.latest_frame: Optional[bytes] = None
        self.latest_offset: int = 0
        self.lock = threading.Lock()
        self.running: bool = True
        
    def run(self) -> None:
        """
        Main thread loop - continuously poll and update latest frame.
        
        Polls Kafka at high frequency and retains only the newest frame,
        allowing inference to skip old frames when it's behind.
        """
        while self.running:
            try:
                # Aggressively poll and keep only the latest frame
                records = self.consumer.poll(timeout_ms=10, max_records=500)
                if records:
                    for topic_partition, partition_records in records.items():
                        if partition_records:
                            latest = partition_records[-1]
                            with self.lock:
                                self.latest_frame = latest.value
                                self.latest_offset = latest.offset
                            
                            # Track consumer lag
                            try:
                                end_offsets = self.consumer.end_offsets([topic_partition])
                                high_water_mark = end_offsets[topic_partition]
                                lag = high_water_mark - latest.offset - 1
                                kafka_consumer_lag.labels(
                                    service=SERVICE_NAME,
                                    topic=topic_partition.topic,
                                    partition=str(topic_partition.partition)
                                ).set(max(0, lag))
                            except Exception as lag_error:
                                logging.debug(f"Failed to calculate lag: {lag_error}")
            except Exception as e:
                logging.error(f"Frame grabber error: {e}")
                time.sleep(0.1)
    
    def get_latest(self) -> Tuple[Optional[bytes], int]:
        """
        Get latest frame without clearing.
        
        Returns:
            Tuple of (frame_bytes, offset). frame_bytes is None if no frame available.
        
        Notes:
            - Thread-safe via lock
            - Does not clear frame (clearing happens after commit)
        """
        with self.lock:
            return self.latest_frame, self.latest_offset
    
    def clear_frame(self) -> None:
        """
        Clear frame after successful commit.
        
        Prevents reprocessing the same frame after it's been committed.
        """
        with self.lock:
            self.latest_frame = None
    
    def stop(self) -> None:
        """Signal thread to stop running."""
        self.running = False


def main() -> None:
    """
    Main inference loop - consume frames, run YOLO, publish results.
    
    Workflow:
        1. Start metrics server in background
        2. Auto-select best device (GPU if available)
        3. Load and warm up YOLO model
        4. Connect Kafka consumer and producer
        5. Start background frame grabber thread
        6. Start GPU metrics updater (if GPU available)
        7. Loop:
           a. Get latest frame from grabber
           b. Decode JPEG to numpy array
           c. Run YOLO inference (detect or track)
           d. Publish annotated frame to visual topic
           e. Publish JSON detections to data topic
           f. Manual commit offset
           g. Clear frame from grabber
           h. Update metrics
        8. Send failed frames to DLQ
        9. Clean up resources on exit
    
    Performance Optimizations:
        - Background thread skips old frames (aggressive catch-up)
        - Half-precision inference on GPU (2x faster)
        - Configurable input size (smaller = faster)
        - Lower JPEG quality for visual output (faster encoding)
        - LZ4 compression for Kafka messages
    
    Exit Conditions:
        - KeyboardInterrupt (Ctrl+C)
        - Fatal connection or model loading failures
    """
    logging.info(f"Starting YOLO Inference on {DEVICE}...")
    logging.info(f"Tracking: {USE_TRACKING}, Input Size: {INPUT_SIZE}")
    
    # Start metrics server in background
    metrics_thread = threading.Thread(target=start_metrics_server, daemon=True)
    metrics_thread.start()
    
    # Log device information
    log_device_info()
    
    # Auto-select best device (GPU if available, CPU fallback)
    selected_device: str = select_best_device(DEVICE)
    logging.info(f"Selected device: {selected_device}")
    
    # Update health state with device info
    gpu_info: Dict[str, Any] = get_gpu_info()
    health_state['gpu_available'] = gpu_info['cuda_available']
    health_state['device_type'] = 'gpu' if 'cuda' in selected_device else 'cpu'
    
    # Set GPU availability metric
    gpu_available.labels(service=SERVICE_NAME).set(1 if gpu_info['cuda_available'] else 0)

    # Initialize Kafka connections
    consumer = create_kafka_consumer(KAFKA_BROKER, KAFKA_INPUT_TOPIC, CONSUMER_GROUP_ID)
    producer = create_kafka_producer(KAFKA_BROKER)
    
    # Load and warm up YOLO model
    try:
        model = YOLO(MODEL_WEIGHTS_PATH)
        model.to(selected_device)
        health_state['model_loaded'] = True
        
        # Warm up model with dummy inference
        logging.info("Warming up model...")
        dummy = np.zeros((INPUT_SIZE, INPUT_SIZE, 3), dtype=np.uint8)
        model.predict(dummy, imgsz=INPUT_SIZE, verbose=False)
        logging.info("Model ready!")
    except Exception as e:
        health_state['model_loaded'] = False
        logging.error(f"Failed to load model: {e}")
        errors_total.labels(service=SERVICE_NAME, error_type='ModelLoadError').inc()
        service_up.labels(service=SERVICE_NAME).set(0)
        return

    if not consumer or not producer:
        service_up.labels(service=SERVICE_NAME).set(0)
        return

    service_up.labels(service=SERVICE_NAME).set(1)
    
    # Start background frame grabber thread
    grabber = FrameGrabber(consumer)
    grabber.start()

    # Processing state variables
    encode_params: List[int] = [cv2.IMWRITE_JPEG_QUALITY, 60]
    frame_count: int = 0
    last_log_time: float = time.time()
    last_offset: int = 0
    
    try:
        logging.info("Processing frames...")
        
        # Background GPU stats updater
        def update_gpu_stats() -> None:
            """Periodically update GPU metrics (runs in background thread)."""
            while True:
                if health_state['gpu_available']:
                    stats = get_gpu_stats()
                    if stats['available']:
                        for gpu in stats['gpus']:
                            gpu_id = str(gpu['id'])
                            gpu_utilization.labels(service=SERVICE_NAME, gpu_id=gpu_id).set(gpu['utilization_percent'])
                            gpu_memory_used.labels(service=SERVICE_NAME, gpu_id=gpu_id).set(gpu['memory_used_mb'])
                            gpu_memory_total.labels(service=SERVICE_NAME, gpu_id=gpu_id).set(gpu['memory_total_mb'])
                            gpu_temperature.labels(service=SERVICE_NAME, gpu_id=gpu_id).set(gpu['temperature_c'])
                time.sleep(5)
        
        if health_state['gpu_available']:
            gpu_stats_thread = threading.Thread(target=update_gpu_stats, daemon=True)
            gpu_stats_thread.start()
        while True:
            loop_start: float = time.time()
            
            # Get the absolute latest frame (skip all old ones)
            frame_bytes, offset = grabber.get_latest()
            
            if frame_bytes is None:
                time.sleep(0.001)
                continue

            messages_consumed.labels(service=SERVICE_NAME, topic=KAFKA_INPUT_TOPIC).inc()
            
            try:
                # Decode JPEG bytes to numpy array
                nparr = np.frombuffer(frame_bytes, np.uint8)
                frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                if frame is None:
                    errors_total.labels(service=SERVICE_NAME, error_type='FrameDecodeError').inc()
                    continue

                frame_count += 1
                frames_processed.labels(service=SERVICE_NAME).inc()
                health_state['processing_frames'] = True
                last_offset = offset

                # Run YOLO inference
                if USE_TRACKING:
                    results = model.track(
                        source=frame, 
                        conf=0.4,  # Lower conf = faster NMS
                        iou=0.5, 
                        persist=True, 
                        verbose=False, 
                        device=selected_device,
                        imgsz=INPUT_SIZE,
                        half=True if selected_device != "cpu" else False
                    )
                else:
                    # Simple detection is MUCH faster than tracking
                    results = model.predict(
                        source=frame, 
                        conf=0.4,
                        iou=0.5, 
                        verbose=False, 
                        device=selected_device,
                        imgsz=INPUT_SIZE,
                        half=True if selected_device != "cpu" else False
                    )
                
                if not results:
                    continue
                result = results[0]

                # --- Visual Output ---
                annotated_frame = result.plot()
                _, buffer = cv2.imencode(".jpg", annotated_frame, encode_params)
                producer.send(KAFKA_VISUAL_TOPIC, buffer.tobytes())
                messages_produced.labels(service=SERVICE_NAME, topic=KAFKA_VISUAL_TOPIC).inc()

                # Extract detection data
                detection_list: List[Dict[str, Any]] = []
                if result.boxes and len(result.boxes):
                    boxes = result.boxes.xyxy.cpu().numpy()
                    classes = result.boxes.cls.cpu().numpy()
                    confs = result.boxes.conf.cpu().numpy()
                    track_ids = result.boxes.id.int().cpu().numpy() if result.boxes.id is not None else [-1] * len(boxes)

                    for box, cls, conf, track_id in zip(boxes, classes, confs, track_ids):
                        class_name = result.names[int(cls)]
                        detection_list.append({
                            "track_id": int(track_id),
                            "class_name": class_name,
                            "confidence": float(conf),
                            "bbox": box.tolist()
                        })
                        
                        # Update detection metrics
                        detections_total.labels(class_name=class_name).inc()
                        detection_confidence.labels(class_name=class_name).observe(float(conf))

                # Create JSON payload with metadata
                json_payload: Dict[str, Any] = {
                    "timestamp": time.time(),
                    "camera_id": CAMERA_ID,
                    "detections": detection_list
                }
                producer.send(KAFKA_DATA_TOPIC, json.dumps(json_payload).encode('utf-8'))
                messages_produced.labels(service=SERVICE_NAME, topic=KAFKA_DATA_TOPIC).inc()
                
                # Flush producer to ensure messages are sent
                producer.flush()
                
                # Manual commit after successful processing
                consumer.commit()
                
                # Clear processed frame from grabber (prevents reprocessing)
                grabber.clear_frame()
                
                # Update latency metric
                latency_ms = (time.time() - loop_start) * 1000
                processing_latency.labels(service=SERVICE_NAME).set(latency_ms)
                
            except Exception as process_error:
                # Send failed message to DLQ
                logging.error(f"Failed to process frame at offset {offset}: {process_error}")
                errors_total.labels(service=SERVICE_NAME, error_type=type(process_error).__name__).inc()
                
                try:
                    dlq_message: Dict[str, Any] = {
                        'error': str(process_error),
                        'error_type': type(process_error).__name__,
                        'offset': offset,
                        'timestamp': time.time(),
                        'service': SERVICE_NAME
                    }
                    producer.send(KAFKA_DLQ_TOPIC, json.dumps(dlq_message).encode())
                    logging.warning(f"Sent failed frame to DLQ: {KAFKA_DLQ_TOPIC}")
                except Exception as dlq_error:
                    logging.error(f"Failed to send to DLQ: {dlq_error}")
            
            # Log FPS every 5 seconds
            if time.time() - last_log_time > 5:
                elapsed = time.time() - last_log_time
                fps = frame_count / elapsed
                inference_fps.labels(service=SERVICE_NAME).set(fps)
                logging.info(f"Processing at {fps:.1f} FPS, offset: {last_offset}")
                frame_count = 0
                last_log_time = time.time()

    except KeyboardInterrupt:
        logging.info("Inference service stopped by user")
    except Exception as e:
        logging.error(f"Error in main loop: {e}")
        errors_total.labels(service=SERVICE_NAME, error_type=type(e).__name__).inc()
    finally:
        # Clean up resources
        service_up.labels(service=SERVICE_NAME).set(0)
        health_state['kafka_consumer_connected'] = False
        health_state['kafka_producer_connected'] = False
        health_state['model_loaded'] = False
        health_state['processing_frames'] = False
        
        # Stop grabber first to prevent new polls
        grabber.stop()
        time.sleep(0.2)
        
        # Close Kafka connections
        if consumer:
            consumer.close()
        if producer:
            producer.close()
        
        logging.info("Inference service cleanup complete")

if __name__ == "__main__":
    main()
