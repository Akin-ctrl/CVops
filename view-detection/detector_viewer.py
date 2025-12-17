from flask import Flask, Response
from kafka import KafkaConsumer
import logging
import os
import threading
import time
from collections import deque

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

app = Flask(__name__)

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "yolo-visual-output")
CONSUMER_GROUP_ID = os.environ.get("CONSUMER_GROUP_ID", "web-viewer-group")

# Global frame buffer (always keep only the latest)
frame_buffer = deque(maxlen=1)
lock = threading.Lock()

def kafka_consumer_loop():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        group_id=CONSUMER_GROUP_ID,
        api_version=(0, 10, 1),
        auto_offset_reset='latest',
        value_deserializer=lambda m: m  # raw bytes
    )
    logging.info(f"Viewer consuming from {KAFKA_TOPIC}")

    for message in consumer:
        with lock:
            frame_buffer.append(message.value)  # keep only the latest frame

def generate_stream():
    while True:
        with lock:
            if frame_buffer:
                frame_bytes = frame_buffer.pop()
            else:
                frame_bytes = None

        if frame_bytes:
            yield (b'--frame\r\n'
                   b'Content-Type: image/jpeg\r\n\r\n' + frame_bytes + b'\r\n')

        time.sleep(0.04)  # ~25 FPS

@app.route('/')
def index():
    return """
    <html>
    <body style="background: black; color: white; text-align: center;">
        <h1>Real-Time Tracking Results</h1>
        <img src="/video_feed" style="border: 2px solid red; width: 80%;">
    </body>
    </html>
    """

@app.route('/health')
def health():
    """Health check endpoint."""
    return {'status': 'healthy', 'service': 'detection-viewer'}, 200

@app.route('/ready')
def ready():
    """Readiness check - has frames?"""
    with lock:
        has_frames = len(frame_buffer) > 0
    return {
        'status': 'ready' if has_frames else 'not_ready',
        'service': 'detection-viewer',
        'checks': {'frames_available': has_frames}
    }, 200 if has_frames else 503

@app.route('/video_feed')
def video_feed():
    return Response(generate_stream(), mimetype='multipart/x-mixed-replace; boundary=frame')

if __name__ == '__main__':
    t = threading.Thread(target=kafka_consumer_loop, daemon=True)
    t.start()
    app.run(host='0.0.0.0', port=7000)
