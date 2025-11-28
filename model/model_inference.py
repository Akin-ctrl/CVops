import cv2
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
import logging
import os
import json
import time
from ultralytics import YOLO
from dotenv import load_dotenv

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

# --- Main Logic ---
def main():
    logging.info(f"Starting YOLO Inference (Tracking Mode) on {DEVICE}...")

    consumer = create_kafka_consumer(KAFKA_BROKER, KAFKA_INPUT_TOPIC, CONSUMER_GROUP_ID)
    producer = create_kafka_producer(KAFKA_BROKER)
    
    try:
        model = YOLO(MODEL_WEIGHTS_PATH)
        model.to(DEVICE)
    except Exception as e:
        logging.error(f"Failed to load model: {e}")
        return

    if not consumer or not producer:
        return

    frame_buffer = deque(maxlen=1)  # Always keep only the latest frame

    # Pre-compile JPEG encoding params
    encode_params = [cv2.IMWRITE_JPEG_QUALITY, 70]
    frame_count = 0
    last_log_time = time.time()
    
    try:
        logging.info("Processing frames...")
        while True:
            # Aggressively poll to drain the queue and get the latest frame
            records = consumer.poll(timeout_ms=50, max_records=100)
            
            if not records:
                continue

            # Only process the LATEST frame across all partitions (skip old ones)
            latest_record = None
            for partition_records in records.values():
                if partition_records:
                    latest_record = partition_records[-1]
            
            if latest_record is None:
                continue

            nparr = np.frombuffer(latest_record.value, np.uint8)
            frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            if frame is None:
                continue

            frame_count += 1

            # --- Inference with optimized settings ---
            results = model.track(
                source=frame, 
                conf=0.5, 
                iou=0.5, 
                persist=True, 
                verbose=False, 
                device=DEVICE,
                imgsz=416,  # Smaller input = faster inference
                half=True if DEVICE != "cpu" else False  # FP16 on GPU
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
                logging.info(f"Processing at {fps:.1f} FPS, latest offset: {latest_record.offset}")
                frame_count = 0
                last_log_time = time.time()

    except KeyboardInterrupt:
        logging.info("Stopping...")
    finally:
        consumer.close()
        producer.close()

if __name__ == "__main__":
    main()
