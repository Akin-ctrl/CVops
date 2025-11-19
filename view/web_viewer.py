# kafka-viewer/web_viewer.py
from flask import Flask, Response
from kafka import KafkaConsumer
import logging
import os
import threading
import time
import cv2
import numpy as np
from dotenv import load_dotenv


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

load_dotenv()
# --- Configuration ---

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092") 
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "yolo-input-frames")
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "web-viewer-group")

# Shared variable to hold the latest frame
current_frame_bytes = None
frame_lock = threading.Lock()

def kafka_consumer_thread():
    """Kafka consumer that continuously reads frames and updates current_frame_bytes."""
    consumer = None
    while True:
        try:
            if consumer is None:
                consumer = KafkaConsumer(
                    KAFKA_TOPIC,
                    bootstrap_servers=[KAFKA_BROKER],
                    group_id=CONSUMER_GROUP_ID,
                    api_version=(0, 10, 1),
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    value_deserializer=lambda m: m
                )
                logging.info(f"Web Viewer consumer for topic '{KAFKA_TOPIC}' connected to {KAFKA_BROKER} (group: '{CONSUMER_GROUP_ID}')")

            for message in consumer:
                with frame_lock:
                    global current_frame_bytes
                    current_frame_bytes = message.value
                # logging.debug(f"Received frame at offset {message.offset}")

        except Exception as e:
            logging.error(f"Error in Kafka consumer thread: {e}")
            consumer = None # Reset consumer to attempt reconnection
            time.sleep(5) # Wait before retrying

@app.route('/video_feed')
def video_feed():
    """Streams the latest preprocessed frame as an MJPEG stream."""
    def generate():
        while True:
            with frame_lock:
                frame_to_send = current_frame_bytes
            
            if frame_to_send is not None:
                yield (b'--frame\r\n'
                       b'Content-Type: image/jpeg\r\n\r\n' + frame_to_send + b'\r\n')
            else:
                # Send a blank/error image if no frame is available
                blank_image = np.zeros((480, 640, 3), np.uint8)
                cv2.putText(blank_image, "No Stream", (200, 240), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)
                _, blank_buffer = cv2.imencode(".jpg", blank_image)
                yield (b'--frame\r\n'
                       b'Content-Type: image/jpeg\r\n\r\n' + blank_buffer.tobytes() + b'\r\n')

            time.sleep(0.05) # Control stream rate

    return Response(generate(), mimetype='multipart/x-mixed-replace; boundary=frame')

@app.route('/')
def index():
    """Simple HTML page to embed the video feed."""
    return """
    <html>
        <head><title>Preprocessed Frame Viewer</title></head>
        <body>
            <h1>Preprocessed Frame Feed (YOLO Input)</h1>
            <img src="/video_feed" width="640" height="480" />
        </body>
    </html>
    """

if __name__ == '__main__':
    # Start the Kafka consumer in a separate thread
    consumer_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    consumer_thread.start()

    # Start the Flask web server
    app.run(host='0.0.0.0', port=5000) # Listen on all interfaces