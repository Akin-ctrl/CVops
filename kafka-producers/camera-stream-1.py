import cv2
import logging
import time
import os
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "esp32-video")
URL = os.getenv("URL")

logging.info(f"URL: {URL}")
logging.info(f"KAFKA_BROKER: {KAFKA_BROKER}")
logging.info(f"KAFKA_TOPIC: {KAFKA_TOPIC}")

def create_kafka_producer(kafka_broker):
    try:
        producer = KafkaProducer(bootstrap_servers=[kafka_broker])
        logging.info(f"Kafka producer connected to {kafka_broker}")
        return producer
    except Exception as e:
        logging.error(f"Kafka producer error: {e}")
        return None

def capture_stream(url):
    cap = cv2.VideoCapture(url)
    if not cap.isOpened():
        logging.error(f"Cannot open stream {url}")
        return None
    logging.info(f"Stream opened: {url}")
    return cap

def read_frame(cap, timeout=5):
    start = time.time()
    while True:
        ret, frame = cap.read()
        if ret:
            return frame
        if time.time() - start > timeout:
            return None

def publish_frame(producer, topic, frame):
    try:
        _, buffer = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
        producer.send(topic, buffer.tobytes())
        producer.flush()
        return True
    except Exception as e:
        logging.error(f"Publish error: {e}")
        return False

def main():
    producer = create_kafka_producer(KAFKA_BROKER)
    if not producer:
        return

    cap = capture_stream(URL)
    if not cap:
        return

    try:
        while True:
            frame = read_frame(cap, timeout=5)
            if frame is None:
                logging.warning("Failed to read frame. Reconnecting...")
                cap = capture_stream(URL)
                if not cap:
                    break
                continue
            publish_frame(producer, KAFKA_TOPIC, frame)
            time.sleep(0.033)  # 30 FPS
    except KeyboardInterrupt:
        logging.info("Stopped by user")
    finally:
        if cap:
            cap.release()
        if producer:
            producer.close()
        logging.info("Resources released")

if __name__ == "__main__":
    main()
