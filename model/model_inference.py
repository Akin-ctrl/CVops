import cv2
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
import logging
import os
import json
import time
from ultralytics import YOLO # Import YOLO from ultralytics
from dotenv import load_dotenv
load_dotenv()
# --- Configuration ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "yolo-input-frames")
KAFKA_OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "yolo-detections") 
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID_3", "yolo-inference-group")

# Model path  (It's copied into the container)
MODEL_WEIGHTS_PATH = os.getenv("MODEL_WEIGHTS_PATH", "yolov11n.pt") # <-- UPDATE THIS TO YOUR TRAINED MODEL NAME (e.g., "best.pt")
DEVICE = os.getenv("DEVICE", "0") # "0" for GPU, "cpu" for CPU

# --- Kafka Client Creation ---
def create_kafka_consumer(kafka_broker, topic, group_id):
    """Creates a Kafka consumer instance."""
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[kafka_broker],
            group_id=group_id,
            api_version=(0, 10, 1),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda m: m # Messages are raw bytes (JPEG)
        )
        logging.info(f"Consumer for topic '{topic}' connected to {kafka_broker} (group: '{group_id}')")
        return consumer
    except Exception as e:
        logging.error(f"Error creating Kafka consumer: {e}")
        return None

def create_kafka_producer(kafka_broker):
    """Creates a Kafka producer instance for sending detection results."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[kafka_broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'), # Serialize results to JSON
        )
        logging.info(f"Kafka producer for results connected to {kafka_broker}")
        return producer
    except Exception as e:
        logging.error(f"Error creating Kafka producer for results: {e}")
        return None

# --- Model Loading ---
def load_yolo_model(model_path, device):
    """Loads the YOLO model."""
    try:
        model = YOLO(model_path)
        # Ensure the model is moved to the specified device (CPU or GPU)
        model.to(device)
        logging.info(f"YOLOv11 model '{model_path}' loaded successfully on device: {device}")
        return model
    except Exception as e:
        logging.error(f"Error loading YOLOv11 model '{model_path}': {e}")
        return None

# --- Main Inference Logic ---
def main():
    logging.info("Starting YOLOv11 inference service...")

    # Create Kafka Consumer
    consumer = create_kafka_consumer(KAFKA_BROKER, KAFKA_INPUT_TOPIC, CONSUMER_GROUP_ID)
    if not consumer:
        return

    # Create Kafka Producer for results
    producer = create_kafka_producer(KAFKA_BROKER)
    if not producer:
        if consumer: consumer.close()
        return

    # Load YOLO Model
    model = load_yolo_model(MODEL_WEIGHTS_PATH, DEVICE)
    if not model:
        if consumer: consumer.close()
        if producer: producer.close()
        return

    try:
        for message in consumer:
            # logging.debug(f"Received frame for inference from topic: {message.topic}, offset: {message.offset}")
            
            # 1. Decode JPEG bytes back to OpenCV image (BGR format for Ultralytics if not already RGB)
            # The preprocessor sends RGB, float32, [0,1]. YOLO's predict expects uint8, BGR for its internal processing if reading from raw,
            # but if you feed it a numpy array it often handles normalization internally.
            # Let's decode to BGR uint8, as that's a common input for ultralytics.
            nparr = np.frombuffer(message.value, np.uint8)
            frame_bgr_uint8 = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

            if frame_bgr_uint8 is None:
                logging.warning(f"Failed to decode image from message at offset {message.offset}. Skipping.")
                continue

            # 2. Perform Inference
            # The .predict() method handles preprocessing (resizing, normalization, BGR to RGB) internally if given a raw frame.
            # Make sure your custom preprocessing in the preprocessor matches what you trained with OR
            # YOLO's internal preprocessing, otherwise, you might need to adjust.
            # Assuming here that `model.predict` will handle its own internal preproc from a raw image array.
            # If your preprocessor produces a specific format, you might need to convert it before passing.
            # However, `predict` is smart enough to handle a numpy array usually.
            
            results = model.predict(source=frame_bgr_uint8, conf=0.5, iou=0.7, verbose=False, device=DEVICE) # conf & iou are tunable thresholds
            
            # 3. Process and Serialize Results
            detection_list = []
            if results and len(results) > 0:
                # Assuming batch_size=1, we take the first result object
                res = results[0] 
                
                # Get names of classes
                class_names = res.names 

                # Iterate through detected objects
                for *xyxy, conf, cls in res.boxes.data:
                    x1, y1, x2, y2 = map(float, xyxy)
                    class_id = int(cls)
                    confidence = float(conf)
                    
                    detection_list.append({
                        "timestamp": int(time.time() * 1000), # Unix timestamp in milliseconds
                        "bbox": [x1, y1, x2, y2], # Bounding box in [x_min, y_min, x_max, y_max] format
                        "class_id": class_id,
                        "class_name": class_names.get(class_id, "unknown"),
                        "confidence": confidence
                    })
            
            # Prepare message for Kafka: a dictionary of detections and optionally original frame metadata
            output_message = {
                "frame_offset": message.offset,
                "camera_id": "esp32-cam-01", # You might want to pass this from producer/preprocessor
                "detections": detection_list
            }

            # 4. Publish Results to Kafka
            producer.send(KAFKA_OUTPUT_TOPIC, output_message)
            producer.flush() # Ensure message is sent

            logging.info(f"Frame offset {message.offset}: Detected {len(detection_list)} objects and published to '{KAFKA_OUTPUT_TOPIC}'.")

    except KeyboardInterrupt:
        logging.info("YOLO inference service interrupted by user.")
    except Exception as e:
        logging.error(f"An unhandled error occurred in main loop: {e}", exc_info=True)
    finally:
        logging.info("Closing YOLO inference service resources...")
        if consumer: consumer.close()
        if producer: producer.close()
        logging.info("YOLO inference service finished.")

if __name__ == "__main__":
    main()