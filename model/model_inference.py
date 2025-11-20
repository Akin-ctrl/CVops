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

# Two Output Topics
KAFKA_VISUAL_TOPIC = os.getenv("KAFKA_VISUAL_TOPIC", "yolo-visual-output") # For the Web Viewer
KAFKA_DATA_TOPIC = os.getenv("KAFKA_DATA_TOPIC", "yolo-data-output")     # For MinIO/DB

CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "yolo-inference-group")
MODEL_WEIGHTS_PATH = os.getenv("MODEL_WEIGHTS_PATH", "yolov11n.pt")
DEVICE = os.getenv("DEVICE", "cpu") # Use "cpu" if no GPU

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
            value_deserializer=lambda m: m # Raw bytes (JPEG)
        )
        logging.info(f"Consumer connected to {topic}")
        return consumer
    except Exception as e:
        logging.error(f"Error creating consumer: {e}")
        return None

def create_kafka_producer(kafka_broker):
    try:
        # A producer that can handle both bytes (for video) and strings (for JSON)
        # Encode manually in the send loop
        producer = KafkaProducer(
            bootstrap_servers=[kafka_broker],
            api_version=(0, 10, 1),
            compression_type='lz4' # Good for video frames
        )
        logging.info(f"Producer connected to {kafka_broker}")
        return producer
    except Exception as e:
        logging.error(f"Error creating producer: {e}")
        return None

# --- Main Logic ---
def main():
    logging.info(f"Starting YOLOv11 Inference (Tracking Mode) on {DEVICE}...")

    consumer = create_kafka_consumer(KAFKA_BROKER, KAFKA_INPUT_TOPIC, CONSUMER_GROUP_ID)
    producer = create_kafka_producer(KAFKA_BROKER)
    
    # Load Model
    try:
        model = YOLO(MODEL_WEIGHTS_PATH)
        model.to(DEVICE)
    except Exception as e:
        logging.error(f"Failed to load model: {e}")
        return

    if not consumer or not producer:
        return

    try:
        for message in consumer:
            # Decode Image
            nparr = np.frombuffer(message.value, np.uint8)
            frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

            if frame is None:
                continue

            # Run Tracking (Inference + ID assignment)
            # persist=True is CRITICAL for tracking to remember objects between frames
            results = model.track(source=frame, conf=0.5, iou=0.5, persist=True, verbose=False, device=DEVICE)
            
            if not results:
                continue

            result = results[0]
            
            # --- A. Prepare Visual Output (Annotated Frame) ---
            # plot() draws the boxes and labels on the frame
            annotated_frame = result.plot() 
            
            # Encode back to JPEG
            _, buffer = cv2.imencode(".jpg", annotated_frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
            visual_bytes = buffer.tobytes()
            
            # Send to Visual Topic (Viewer)
            producer.send(KAFKA_VISUAL_TOPIC, visual_bytes)

            # --- B. Prepare Data Output (JSON) ---
            detection_list = []
            
            # Check if we have detections
            if result.boxes:
                # extract boxes, classes, and track_ids (if available)
                boxes = result.boxes.xyxy.cpu().numpy()
                classes = result.boxes.cls.cpu().numpy()
                confs = result.boxes.conf.cpu().numpy()
                
                # Track IDs might be None if detection exists but tracking failed momentarily
                track_ids = result.boxes.id.int().cpu().numpy() if result.boxes.id is not None else [-1] * len(boxes)

                for box, cls, conf, track_id in zip(boxes, classes, confs, track_ids):
                    detection_list.append({
                        "track_id": int(track_id),
                        "class_name": result.names[int(cls)],
                        "confidence": float(conf),
                        "bbox": box.tolist() # [x1, y1, x2, y2]
                    })

            json_payload = {
                "timestamp": time.time(),
                "frame_offset": message.offset,
                "camera_id": "esp32-cam-01",
                "detections": detection_list
            }

            # Send to Data Topic (MinIO/Analytics)
            producer.send(KAFKA_DATA_TOPIC, json.dumps(json_payload).encode('utf-8'))
            
            # logging.info(f"Processed frame {message.offset}: {len(detection_list)} objects tracked.")

    except KeyboardInterrupt:
        logging.info("Stopping...")
    finally:
        consumer.close()
        producer.close()

if __name__ == "__main__":
    main()