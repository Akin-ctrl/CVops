import cv2
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
import logging
import os
import sys
import json
import time
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

# --- Configuration ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "yolo-input-frames")
KAFKA_VISUAL_TOPIC = os.getenv("KAFKA_VISUAL_TOPIC", "yolo-visual-output")
KAFKA_DATA_TOPIC = os.getenv("KAFKA_DATA_TOPIC", "yolo-data-output")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "dlq-inference-errors")

CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "yolo-inference-group")
MODEL_WEIGHTS_PATH = os.getenv("MODEL_WEIGHTS_PATH", "yolo11n.pt")
DEVICE = os.getenv("DEVICE", "cpu")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8002"))

# Performance tuning
USE_TRACKING = os.getenv("USE_TRACKING", "false").lower() == "true"  # Tracking is slower
INPUT_SIZE = int(os.getenv("INPUT_SIZE", "320"))  # Smaller = faster (320 is very fast)

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

SERVICE_NAME = 'yolo-inference'

# Global state for health checks
health_state = {
    'kafka_consumer_connected': False,
    'kafka_producer_connected': False,
    'model_loaded': False,
    'processing_frames': False,
    'gpu_available': False,
    'device_type': 'unknown'
}

def start_metrics_server():
    """Start HTTP server with health, readiness, and metrics endpoints."""
    try:
        start_health_server(METRICS_PORT, SERVICE_NAME, health_state)
    except Exception as e:
        logging.error(f"Failed to start HTTP server: {e}")
        service_up.labels(service=SERVICE_NAME).set(0)

# --- Kafka Helpers ---
def create_kafka_consumer(kafka_broker, topic, group_id):
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[kafka_broker],
            group_id=group_id,
            api_version=(0, 10, 1),
            auto_offset_reset='latest',
            enable_auto_commit=False,  # Manual commits for reliability
            fetch_max_bytes=1048576,  # 1MB max fetch
            max_partition_fetch_bytes=524288,  # 512KB per partition
            value_deserializer=lambda m: m  # raw bytes
        )
        health_state['kafka_consumer_connected'] = True
        logging.info(f"Consumer connected to {topic} (manual commits)")
        return consumer
    except Exception as e:
        health_state['kafka_consumer_connected'] = False
        logging.error(f"Error creating consumer: {e}")
        return None

def create_kafka_producer(kafka_broker):
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

# --- Frame Consumer Thread ---
class FrameGrabber(threading.Thread):
    """Background thread that continuously grabs the latest frame."""
    def __init__(self, consumer):
        super().__init__(daemon=True)
        self.consumer = consumer
        self.latest_frame = None
        self.latest_offset = 0
        self.lock = threading.Lock()
        self.running = True
        
    def run(self):
        while self.running:
            try:
                # Aggressively poll and keep only the latest frame
                records = self.consumer.poll(timeout_ms=10, max_records=500)
                if records:
                    for partition_records in records.values():
                        if partition_records:
                            latest = partition_records[-1]
                            with self.lock:
                                self.latest_frame = latest.value
                                self.latest_offset = latest.offset
            except Exception as e:
                logging.error(f"Frame grabber error: {e}")
                time.sleep(0.1)
    
    def get_latest(self):
        with self.lock:
            frame = self.latest_frame
            offset = self.latest_offset
            self.latest_frame = None  # Clear after reading
        return frame, offset
    
    def stop(self):
        self.running = False

# --- Main Logic ---
def main():
    logging.info(f"Starting YOLO Inference on {DEVICE}...")
    logging.info(f"Tracking: {USE_TRACKING}, Input Size: {INPUT_SIZE}")
    
    # Start metrics server in background
    metrics_thread = threading.Thread(target=start_metrics_server, daemon=True)
    metrics_thread.start()
    
    # Log device information
    log_device_info()
    
    # Auto-select best device (GPU if available, CPU fallback)
    selected_device = select_best_device(DEVICE)
    logging.info(f"Selected device: {selected_device}")
    
    # Update health state with device info
    gpu_info = get_gpu_info()
    health_state['gpu_available'] = gpu_info['cuda_available']
    health_state['device_type'] = 'gpu' if 'cuda' in selected_device else 'cpu'
    
    # Set GPU availability metric
    gpu_available.labels(service=SERVICE_NAME).set(1 if gpu_info['cuda_available'] else 0)

    consumer = create_kafka_consumer(KAFKA_BROKER, KAFKA_INPUT_TOPIC, CONSUMER_GROUP_ID)
    producer = create_kafka_producer(KAFKA_BROKER)
    
    try:
        model = YOLO(MODEL_WEIGHTS_PATH)
        model.to(selected_device)
        health_state['model_loaded'] = True
        # Warmup the model
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
    
    # Start background frame grabber
    grabber = FrameGrabber(consumer)
    grabber.start()

    # Pre-compile JPEG encoding params
    encode_params = [cv2.IMWRITE_JPEG_QUALITY, 60]  # Lower quality = faster
    frame_count = 0
    last_log_time = time.time()
    last_offset = 0
    
    # Track baseline CPU performance for speedup calculation
    cpu_baseline_latency = None
    
    try:
        logging.info("Processing frames...")
        
        # Background stats updater for GPU metrics
        def update_gpu_stats():
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
                time.sleep(5)  # Update every 5 seconds
        
        if health_state['gpu_available']:
            gpu_stats_thread = threading.Thread(target=update_gpu_stats, daemon=True)
            gpu_stats_thread.start()
        while True:
            loop_start = time.time()
            
            # Get the absolute latest frame (skip all old ones)
            frame_bytes, offset = grabber.get_latest()
            
            if frame_bytes is None:
                time.sleep(0.001)  # Brief sleep if no frame
                continue

            messages_consumed.labels(service=SERVICE_NAME, topic=KAFKA_INPUT_TOPIC).inc()
            
            try:
                nparr = np.frombuffer(frame_bytes, np.uint8)
                frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                if frame is None:
                    errors_total.labels(service=SERVICE_NAME, error_type='FrameDecodeError').inc()
                    continue

                frame_count += 1
                frames_processed.labels(service=SERVICE_NAME).inc()
                health_state['processing_frames'] = True
                last_offset = offset

                # --- Inference with optimized settings ---
                if USE_TRACKING:
                    results = model.track(
                        source=frame, 
                        conf=0.4,  # Lower conf = faster NMS
                        iou=0.5, 
                        persist=True, 
                        verbose=False, 
                        device=DEVICE,
                        imgsz=INPUT_SIZE,
                        half=True if DEVICE != "cpu" else False
                    )
                else:
                    # Simple detection is MUCH faster than tracking
                    results = model.predict(
                        source=frame, 
                        conf=0.4,
                        iou=0.5, 
                        verbose=False, 
                        device=DEVICE,
                        imgsz=INPUT_SIZE,
                        half=True if DEVICE != "cpu" else False
                    )
                
                if not results:
                    continue
                result = results[0]

                # --- Visual Output ---
                annotated_frame = result.plot()
                _, buffer = cv2.imencode(".jpg", annotated_frame, encode_params)
                producer.send(KAFKA_VISUAL_TOPIC, buffer.tobytes())
                messages_produced.labels(service=SERVICE_NAME, topic=KAFKA_VISUAL_TOPIC).inc()

                # --- JSON Data Output (optimized) ---
                detection_list = []
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

                json_payload = {
                    "timestamp": time.time(),
                    "camera_id": "esp32-cam-01",
                    "detections": detection_list
                }
                producer.send(KAFKA_DATA_TOPIC, json.dumps(json_payload).encode('utf-8'))
                messages_produced.labels(service=SERVICE_NAME, topic=KAFKA_DATA_TOPIC).inc()
                
                # Manual commit after successful processing
                consumer.commit()
                
                # Update latency metric
                latency_ms = (time.time() - loop_start) * 1000
                processing_latency.labels(service=SERVICE_NAME).set(latency_ms)
                
            except Exception as process_error:
                # Send failed message to DLQ
                logging.error(f"Failed to process frame at offset {offset}: {process_error}")
                errors_total.labels(service=SERVICE_NAME, error_type=type(process_error).__name__).inc()
                
                try:
                    dlq_message = {
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
        logging.info("Stopping...")
    except Exception as e:
        logging.error(f"Error in main loop: {e}")
        errors_total.labels(service=SERVICE_NAME, error_type=type(e).__name__).inc()
    finally:
        service_up.labels(service=SERVICE_NAME).set(0)
        health_state['kafka_consumer_connected'] = False
        health_state['kafka_producer_connected'] = False
        health_state['model_loaded'] = False
        health_state['processing_frames'] = False
        grabber.stop()
        consumer.close()
        producer.close()
        logging.info("Cleanup complete")

if __name__ == "__main__":
    main()
