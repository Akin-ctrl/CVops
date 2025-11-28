import cv2
import numpy as np
from kafka import KafkaConsumer, KafkaProducer 
import logging
import os
import time
from dotenv import load_dotenv

load_dotenv() 

# --- Configuration ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "esp32-video")
KAFKA_OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "yolo-input-frames") 
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID_1", "preprocessor-group")

# YOLO's expected input size - IMPORTANT: set these based on your YOLOv11 model config
YOLO_INPUT_SIZE_WIDTH = int(os.getenv("YOLO_INPUT_SIZE_WIDTH", "640"))
YOLO_INPUT_SIZE_HEIGHT = int(os.getenv("YOLO_INPUT_SIZE_HEIGHT", "640"))
TARGET_YOLO_SIZE = (YOLO_INPUT_SIZE_WIDTH, YOLO_INPUT_SIZE_HEIGHT)

# JPEG quality for output frames (0-100)
OUTPUT_JPEG_QUALITY = int(os.getenv("OUTPUT_JPEG_QUALITY", "85"))

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
    """Creates a Kafka producer instance."""
    try:
        producer = KafkaProducer(bootstrap_servers=[kafka_broker])
        logging.info(f"Kafka producer connected to {kafka_broker}")
        return producer
    except Exception as e:
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

    # Create Kafka Consumer
    consumer = create_kafka_consumer(KAFKA_BROKER, KAFKA_INPUT_TOPIC, CONSUMER_GROUP_ID)
    if not consumer:
        return

    # Create Kafka Producer for output
    producer = create_kafka_producer(KAFKA_BROKER)
    if not producer:
        if consumer: consumer.close()
        return

    frame_count = 0
    last_flush_time = time.time()
    
    try:
        while True:
            # Poll in batches and only keep the latest frame to catch up when behind
            records = consumer.poll(timeout_ms=100, max_records=50)
            
            if not records:
                continue
            
            # Get only the latest message across all partitions
            latest_message = None
            for partition_records in records.values():
                if partition_records:
                    latest_message = partition_records[-1]  # Keep only the latest
            
            if latest_message is None:
                continue
                
            frame_count += 1
            
            # Skip preprocessing - just forward the raw frame (YOLO handles its own preprocessing)
            # This removes redundant decode->process->encode cycle
            producer.send(KAFKA_OUTPUT_TOPIC, latest_message.value)
            
            # Batch flush every 100ms instead of every frame
            if time.time() - last_flush_time > 0.1:
                producer.flush()
                last_flush_time = time.time()
            
            if frame_count % 30 == 0:
                logging.info(f"Forwarded frame {frame_count}, offset: {latest_message.offset}")
            
    except KeyboardInterrupt:
        logging.info("Script interrupted by user.")
    finally:
        logging.info("Closing Kafka consumer and producer...")
        if consumer:
            consumer.close()
        if producer:
            producer.close()
        logging.info("Script finished.")

if __name__ == "__main__":
    main()