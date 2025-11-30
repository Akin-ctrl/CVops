import cv2
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
import logging
import os
import json
import time
from ultralytics import YOLO
from dotenv import load_dotenv
import threading
from queue import Queue, Empty
load_dotenv()

# --- Configuration ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "yolo-input-frames")
KAFKA_VISUAL_TOPIC = os.getenv("KAFKA_VISUAL_TOPIC", "yolo-visual-output")
KAFKA_DATA_TOPIC = os.getenv("KAFKA_DATA_TOPIC", "yolo-data-output")

CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "yolo-inference-group")
MODEL_WEIGHTS_PATH = os.getenv("MODEL_WEIGHTS_PATH", "yolo11n.pt")
DEVICE = os.getenv("DEVICE", "cpu")

# Performance tuning
USE_TRACKING = os.getenv("USE_TRACKING", "false").lower() == "true"  # Tracking is slower
INPUT_SIZE = int(os.getenv("INPUT_SIZE", "320"))  # Smaller = faster (320 is very fast)

# --- Kafka Helpers ---
def create_kafka_consumer(kafka_broker, topic, group_id):
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[kafka_broker],
            group_id=group_id,
            api_version=(0, 10, 1),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            fetch_max_bytes=1048576,  # 1MB max fetch
            max_partition_fetch_bytes=524288,  # 512KB per partition
            value_deserializer=lambda m: m  # raw bytes
        )
        logging.info(f"Consumer connected to {topic}")
        return consumer
    except Exception as e:
        logging.error(f"Error creating consumer: {e}")
        return None

def create_kafka_producer(kafka_broker):
    try:
        producer = KafkaProducer(
            bootstrap_servers=[kafka_broker],
            api_version=(0, 10, 1),
            compression_type='lz4'
        )
        logging.info(f"Producer connected to {kafka_broker}")
        return producer
    except Exception as e:
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

    consumer = create_kafka_consumer(KAFKA_BROKER, KAFKA_INPUT_TOPIC, CONSUMER_GROUP_ID)
    producer = create_kafka_producer(KAFKA_BROKER)
    
    try:
        model = YOLO(MODEL_WEIGHTS_PATH)
        model.to(DEVICE)
        # Warmup the model
        logging.info("Warming up model...")
        dummy = np.zeros((INPUT_SIZE, INPUT_SIZE, 3), dtype=np.uint8)
        model.predict(dummy, imgsz=INPUT_SIZE, verbose=False)
        logging.info("Model ready!")
    except Exception as e:
        logging.error(f"Failed to load model: {e}")
        return

    if not consumer or not producer:
        return

    # Start background frame grabber
    grabber = FrameGrabber(consumer)
    grabber.start()

    # Pre-compile JPEG encoding params
    encode_params = [cv2.IMWRITE_JPEG_QUALITY, 60]  # Lower quality = faster
    frame_count = 0
    last_log_time = time.time()
    last_offset = 0
    
    try:
        logging.info("Processing frames...")
        while True:
            # Get the absolute latest frame (skip all old ones)
            frame_bytes, offset = grabber.get_latest()
            
            if frame_bytes is None:
                time.sleep(0.001)  # Brief sleep if no frame
                continue

            nparr = np.frombuffer(frame_bytes, np.uint8)
            frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            if frame is None:
                continue

            frame_count += 1
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

            # --- JSON Data Output (optimized) ---
            detection_list = []
            if result.boxes and len(result.boxes):
                boxes = result.boxes.xyxy.cpu().numpy()
                classes = result.boxes.cls.cpu().numpy()
                confs = result.boxes.conf.cpu().numpy()
                track_ids = result.boxes.id.int().cpu().numpy() if result.boxes.id is not None else [-1] * len(boxes)

                for box, cls, conf, track_id in zip(boxes, classes, confs, track_ids):
                    detection_list.append({
                        "track_id": int(track_id),
                        "class_name": result.names[int(cls)],
                        "confidence": float(conf),
                        "bbox": box.tolist()
                    })

            json_payload = {
                "timestamp": time.time(),
                "camera_id": "esp32-cam-01",
                "detections": detection_list
            }
            producer.send(KAFKA_DATA_TOPIC, json.dumps(json_payload).encode('utf-8'))
            
            # Log FPS every 5 seconds
            if time.time() - last_log_time > 5:
                elapsed = time.time() - last_log_time
                fps = frame_count / elapsed
                logging.info(f"Processing at {fps:.1f} FPS, offset: {last_offset}")
                frame_count = 0
                last_log_time = time.time()

    except KeyboardInterrupt:
        logging.info("Stopping...")
    finally:
        grabber.stop()
        consumer.close()
        producer.close()

if __name__ == "__main__":
    main()
