import cv2
import time

# URL = "http://192.168.0.104:8080/stream"
URL = "http://192.168.0.175:8080/stream"
cap = cv2.VideoCapture(URL)
start = time.time()
while True:
    ret, frame = cap.read()
    if not ret:
        if time.time() - start > 5:  # 5s timeout
            print("Failed to read stream after 5s. Check URL/firewall.")
            break
        continue

    cv2.imshow("ESP32-CAM", frame)
    if cv2.waitKey(1) == 27:  # ESC to quit
        break

cap.release()
cv2.destroyAllWindows()



# RTSP_URL = os.getenv("RTSP_URL") 
# print(f"RTSP_URL: {RTSP_URL}")
# cap = cv2.VideoCapture(RTSP_URL)


# if not cap.isOpened():
#     print("Failed to open RTSP stream")
#     exit()
# else:
#     while True:
#         ret, frame = cap.read()
#         if ret:
#             print("Stream working, frame received")
#         else:
#             print("Could not read frame")
# cap.release()

# # import cv2
# # from kafka import KafkaProducer
# # import logging
# # import time
# # import os

# # # 1. Configure Logging
# # logging.basicConfig(level=logging.INFO,
# #                     format='%(asctime)s - %(levelname)s - %(message)s')

# # # 2. Load Configuration from Environment Variables
# # KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")  
# # KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC") 
# # RTSP_URL = os.environ.get("RTSP_URL") 

# # def create_kafka_producer(kafka_broker):
# #     """Creates a Kafka producer instance."""
# #     try:
# #         producer = KafkaProducer(
# #             bootstrap_servers=[kafka_broker],
# #             api_version=(0, 10, 1),  # Specify API version for compatibility
# #             # Add other producer configurations as needed (e.g., compression)
# #         )
# #         logging.info(f"Kafka producer connected to {kafka_broker}")
# #         return producer
# #     except Exception as e:
# #         logging.error(f"Error creating Kafka producer: {e}")
# #         return None

# # def capture_rtsp_stream(rtsp_url):
# #     """Captures the RTSP stream using OpenCV."""
# #     try:
# #         cap = cv2.VideoCapture(rtsp_url)
# #         if not cap.isOpened():
# #             logging.error(f"Error: Could not open RTSP stream at {rtsp_url}")
# #             return None
# #         logging.info(f"Successfully opened RTSP stream at {rtsp_url}")
# #         return cap
# #     except Exception as e:
# #         logging.error(f"Error capturing RTSP stream: {e}")
# #         return None

# import cv2
# import requests
# import numpy as np
# import time

# url = "http://10.31.13.123:81/stream"

# while True:
#     try:
#         r = requests.get(url, timeout=10)  # increase timeout
#         img_arr = np.asarray(bytearray(r.content), dtype=np.uint8)
#         frame = cv2.imdecode(img_arr, cv2.IMREAD_COLOR)

#         if frame is None:
#             print("Failed to decode frame")
#             continue

#         cv2.imshow("ESP Stream", frame)
#         if cv2.waitKey(1) & 0xFF == ord('q'):
#             break

#     except requests.exceptions.Timeout:
#         print("⏱ Timeout, retrying...")
#         time.sleep(1)
#         continue

# cv2.destroyAllWindows()

# import cv2
# import requests
# import numpy as np

# url = "http://192.168.0.106:81/stream"  # ESP32 MJPEG stream

# # Open a streaming session
# session = requests.Session()
# response = session.get(url, stream=True, timeout=10)

# if response.status_code != 200:
#     print("Failed to connect to stream")
#     exit()

# bytes_data = b''

# try:
#     for chunk in response.iter_content(chunk_size=1024):
#         bytes_data += chunk

#         # Look for JPEG frame boundaries
#         a = bytes_data.find(b'\xff\xd8')  # JPEG start
#         b = bytes_data.find(b'\xff\xd9')  # JPEG end

#         if a != -1 and b != -1:
#             jpg = bytes_data[a:b+2]
#             bytes_data = bytes_data[b+2:]

#             # Decode the frame
#             frame = cv2.imdecode(np.frombuffer(jpg, dtype=np.uint8), cv2.IMREAD_COLOR)
#             if frame is None:
#                 continue

#             cv2.imshow("ESP32-CAM Stream", frame)
#             if cv2.waitKey(1) & 0xFF == ord('q'):
#                 break

# except requests.exceptions.RequestException as e:
#     print("Stream error:", e)

# cv2.destroyAllWindows()
