# CVops - Real-Time Computer Vision Pipeline

[![Python 3.12](https://img.shields.io/badge/Python-3.12-blue.svg)](https://www.python.org/downloads/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://docs.docker.com/compose/)
[![YOLO](https://img.shields.io/badge/YOLO-v11-00FFFF.svg)](https://docs.ultralytics.com/)
[![Kafka](https://img.shields.io/badge/Apache-Kafka-231F20.svg)](https://kafka.apache.org/)

A **distributed, real-time computer vision pipeline** designed for edge-to-cloud video analytics. The system ingests video streams from IoT cameras (ESP32-CAM), processes frames through a series of microservices, performs **YOLO object detection**, and stores detection data for later analysis.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Data Flow](#data-flow)
- [Components](#components)
  - [Kafka Producer](#1-kafka-producer)
  - [Preprocessor](#2-preprocessor)
  - [YOLO Inference](#3-yolo-inference)
  - [MinIO Writer](#4-minio-writer)
  - [Web Viewers](#5-web-viewers)
- [Infrastructure](#infrastructure)
- [Kafka Topics](#kafka-topics)
- [Technology Stack](#technology-stack)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Exposed Ports](#exposed-ports)
- [Performance Optimizations](#performance-optimizations)
- [Recommended Improvements](#recommended-improvements)
- [Use Cases](#use-cases)
- [License](#license)

---

## Architecture Overview

```
┌─────────────────┐
│   ESP32-CAM     │  (Physical camera streaming via HTTP)
│  (IoT Device)   │
└────────┬────────┘
         │ HTTP Stream
         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           DOCKER COMPOSE INFRASTRUCTURE                         │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────────┐     ┌─────────────────┐                                    │
│  │  kafka-producer │────▶│   Kafka Topic   │  "esp32-video"                     │
│  │ (camera-stream) │     │  (Raw Frames)   │                                    │
│  └─────────────────┘     └────────┬────────┘                                    │
│                                   │                                             │
│                                   ▼                                             │
│                          ┌─────────────────┐     ┌─────────────────┐            │
│                          │   preprocessor  │────▶│   Kafka Topic   │            │
│                          │  (frame relay)  │     │ "yolo-input-frames"          │
│                          └─────────────────┘     └────────┬────────┘            │
│                                                           │                     │
│                                    ┌──────────────────────┴──────────────┐      │
│                                    ▼                                     │      │
│  ┌───────────────────────────────────────────────────────────────────┐   │      │
│  │                    yolo-inference                                  │   │      │
│  │  (YOLO11n Model - Object Detection/Tracking)                      │   │      │
│  └───────────────────────────┬───────────────────────────────────────┘   │      │
│                              │                                           │      │
│              ┌───────────────┴───────────────┐                           │      │
│              ▼                               ▼                           │      │
│  ┌─────────────────────┐         ┌─────────────────────┐                 │      │
│  │ "yolo-visual-output"│         │ "yolo-data-output"  │                 │      │
│  │  (Annotated JPEG)   │         │  (JSON Detections)  │                 │      │
│  └──────────┬──────────┘         └──────────┬──────────┘                 │      │
│             │                               │                            │      │
│             ▼                               ▼                            │      │
│  ┌─────────────────────┐         ┌─────────────────────┐                 │      │
│  │  detector-viewer    │         │    minio-writer     │                 │      │
│  │  (Flask Web UI)     │         │  (Batch Storage)    │                 │      │
│  │   Port: 7000        │         └──────────┬──────────┘                 │      │
│  └─────────────────────┘                    │                            │      │
│                                             ▼                            │      │
│                                  ┌─────────────────────┐                 │      │
│                                  │       MinIO         │                 │      │
│                                  │  (S3-Compatible)    │                 │      │
│                                  │  API: 9000          │                 │      │
│                                  │  Console: 9001      │                 │      │
│                                  └─────────────────────┘                 │      │
│                                                                          │      │
│    ┌─────────────────┐◀──────────────────────────────────────────────────┘      │
│    │   kafka-viewer  │  (consumes "yolo-input-frames")                          │
│    │ (Flask Web UI)  │                                                          │
│    │   Port: 5000    │                                                          │
│    └─────────────────┘                                                          │
│                                                                                 │
│    ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐          │
│    │    Zookeeper    │◀───▶│      Kafka      │◀───▶│     Kafdrop     │          │
│    │   (Port 2181)   │     │  (9092, 29092)  │     │  (Port 19000)   │          │
│    └─────────────────┘     └─────────────────┘     └─────────────────┘          │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow

1. **Ingestion**: ESP32-CAM streams MJPEG video over HTTP
2. **Capture**: Kafka producer captures frames at ~30 FPS and publishes to Kafka
3. **Preprocessing**: Preprocessor consumes frames and forwards to YOLO input topic
4. **Inference**: YOLO model performs object detection on each frame
5. **Output**: 
   - Annotated frames → `yolo-visual-output` → Web viewer (port 7000)
   - Detection JSON → `yolo-data-output` → MinIO storage
6. **Storage**: MinIO writer batches detections and stores as JSON files

---

## Components

### 1. Kafka Producer

**Location**: `kafka-producers/camera-stream-1.py`

| Aspect | Details |
|--------|---------|
| **Purpose** | Ingests video stream from ESP32-CAM and publishes frames to Kafka |
| **Input** | HTTP MJPEG stream (e.g., `http://192.168.x.x:8080/stream`) |
| **Output Topic** | `esp32-video` |
| **Frame Rate** | ~30 FPS (33ms interval) |
| **Encoding** | JPEG @ 80% quality |
| **Dependencies** | `opencv-python`, `kafka-python`, `python-dotenv` |

---

### 2. Preprocessor

**Location**: `kafka-consumer/preprocessor.py`

| Aspect | Details |
|--------|---------|
| **Purpose** | Consumes raw frames and forwards to YOLO inference |
| **Input Topic** | `esp32-video` |
| **Output Topic** | `yolo-input-frames` |
| **Consumer Group** | `preprocessor-group` |
| **Optimization** | Polls batches of 50, keeps only **latest frame** to prevent lag |

**Available Preprocessing** (currently bypassed for performance):
- CLAHE contrast enhancement
- BGR → RGB conversion
- Resize to 640×640
- Normalize to [0,1] float32

---

### 3. YOLO Inference

**Location**: `model/model_inference.py`

| Aspect | Details |
|--------|---------|
| **Purpose** | Real-time object detection using YOLOv11 nano model |
| **Model** | `yolo11n.pt` (Ultralytics YOLO11 Nano) |
| **Input Topic** | `yolo-input-frames` |
| **Output Topics** | `yolo-visual-output` (annotated JPEGs), `yolo-data-output` (JSON) |
| **Device** | CPU (configurable for GPU) |
| **Input Size** | 320×320 (configurable) |
| **Features** | Optional tracking, background frame grabber, LZ4 compression |

**JSON Detection Output Format:**
```json
{
  "timestamp": 1701799200.123,
  "camera_id": "esp32-cam-01",
  "detections": [
    {
      "track_id": 1,
      "class_name": "person",
      "confidence": 0.87,
      "bbox": [100.5, 200.3, 300.7, 450.2]
    }
  ]
}
```

---

### 4. MinIO Writer

**Location**: `minio/minio-writer.py`

| Aspect | Details |
|--------|---------|
| **Purpose** | Batches detection JSON data and stores in MinIO |
| **Input Topic** | `yolo-data-output` |
| **Storage** | MinIO (S3-compatible object storage) |
| **Bucket** | `detections-data` |
| **Batch Strategy** | Upload after **50 detections** OR **30 seconds** |
| **File Naming** | `YYYY/MM/DD/HH-MM-SS_batch.json` |

---

### 5. Web Viewers

#### Preprocessed Frame Viewer
**Location**: `view-process/web_viewer.py`

| Aspect | Details |
|--------|---------|
| **Port** | 5000 |
| **Topic** | `yolo-input-frames` |
| **Purpose** | View preprocessed frames before YOLO inference |
| **Endpoint** | `/video_feed` (MJPEG stream) |

#### Detection Viewer
**Location**: `view-detection/detector_viewer.py`

| Aspect | Details |
|--------|---------|
| **Port** | 7000 |
| **Topic** | `yolo-visual-output` |
| **Purpose** | View annotated frames with bounding boxes |
| **Frame Rate** | ~25 FPS |

---

## Infrastructure

### Apache Kafka (Confluent Platform 7.5.0)
- **Internal Broker**: `kafka:9092`
- **External Broker**: `localhost:29092`
- **Zookeeper**: Port 2181
- **Topics**: Auto-created, single partition, replication factor 1
- **Management UI**: Kafdrop at port 19000

### MinIO (S3-Compatible Object Storage)
- **API Port**: 9000
- **Console UI**: 9001
- **Default Credentials**: `minioadmin / minioadmin`
- **Persistent Volume**: `minio_data`

### Docker Volumes
```yaml
volumes:
  zookeeper_data:    # Zookeeper state
  zookeeper_log:     # Zookeeper logs
  kafka_data:        # Kafka message storage
  minio_data:        # MinIO object storage
```

---

## Kafka Topics

| Topic Name | Producer | Consumer(s) | Payload Type |
|------------|----------|-------------|--------------|
| `esp32-video` | kafka-producer | preprocessor | Raw JPEG bytes |
| `yolo-input-frames` | preprocessor | yolo-inference, kafka-viewer | JPEG bytes |
| `yolo-visual-output` | yolo-inference | detector-viewer | Annotated JPEG bytes |
| `yolo-data-output` | yolo-inference | minio-writer | JSON detection metadata |

---

## Technology Stack

| Layer | Technology |
|-------|------------|
| **Language** | Python 3.12 |
| **ML Framework** | Ultralytics YOLO11 |
| **Message Broker** | Apache Kafka (Confluent 7.5.0) |
| **Object Storage** | MinIO |
| **Web Framework** | Flask |
| **Computer Vision** | OpenCV |
| **Containerization** | Docker, Docker Compose |
| **Compression** | LZ4 |

---

## Quick Start

### Prerequisites
- Docker & Docker Compose
- ESP32-CAM or IP camera with HTTP stream

### 1. Clone and Configure

```bash
git clone https://github.com/Akin-ctrl/corvision.git
cd corvision

# Edit .env with your camera URL
nano .env
```

### 2. Start All Services

```bash
docker compose up -d
```

### 3. Access Web Interfaces

| Service | URL |
|---------|-----|
| Preprocessed Frames | http://localhost:5000 |
| Detection Results | http://localhost:7000 |
| Kafka Management | http://localhost:19000 |
| MinIO Console | http://localhost:9001 |

### 4. View Logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f yolo_inference
```

### 5. Stop Services

```bash
docker compose down

# With volume cleanup
docker compose down -v
```

---

## Configuration

All configuration is managed via the `.env` file:

```env
# Camera Configuration
URL=http://192.168.x.x:8080/stream

# Kafka Configuration
KAFKA_BROKER=kafka:9092
KAFKA_TOPIC_1=esp32-video
KAFKA_TOPIC_2=yolo-input-frames
KAFKA_TOPIC_3=yolo-data-output
KAFKA_TOPIC_4=yolo-visual-output

# Consumer Groups
CONSUMER_GROUP_ID_1=preprocessor-group
CONSUMER_GROUP_ID_2=web-viewer-group
CONSUMER_GROUP_ID_3=yolo-inference-group
CONSUMER_GROUP_ID_4=detector-viewer-group
CONSUMER_GROUP_ID_5=minio-writer-group

# YOLO Configuration
YOLO_INPUT_SIZE_WIDTH=640
YOLO_INPUT_SIZE_HEIGHT=640
MODEL_WEIGHTS_PATH=yolo11n.pt

# MinIO Configuration
MINIO_HOST=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
```

---

## Exposed Ports

| Port | Service | Purpose |
|------|---------|---------|
| 2181 | Zookeeper | Kafka coordination |
| 9092 | Kafka (internal) | Internal broker |
| 29092 | Kafka (external) | Host access |
| 19000 | Kafdrop | Kafka web UI |
| 5000 | kafka-viewer | Preprocessed frame stream |
| 7000 | detector-viewer | Detection result stream |
| 9000 | MinIO | S3 API |
| 9001 | MinIO | Console UI |

---

## Performance Optimizations

The system includes several optimizations for real-time processing:

1. **Frame Skipping**: Preprocessor and YOLO inference keep only the **latest frame** to prevent queue buildup and lag
2. **Background Thread**: `FrameGrabber` daemon decouples Kafka polling from inference processing
3. **Batched Kafka Flush**: Producer flushes every 100ms instead of per-frame
4. **Reduced Input Size**: YOLO uses 320×320 instead of 640×640 for ~2x speedup
5. **Detection-Only Mode**: Object tracking disabled by default (faster than tracking)
6. **LZ4 Compression**: Kafka producer uses LZ4 for fast message compression
7. **Offline Wheel Files**: Pre-downloaded pip wheels for air-gapped deployments

---

## Recommended Improvements

### 🔴 High Priority

#### Reliability & Fault Tolerance

| Issue | Recommendation |
|-------|----------------|
| **No message persistence on failure** | Enable Kafka manual commits with proper offset management. Currently using `enable_auto_commit=True` which can lose messages on crash |
| **No dead-letter queue** | Add a DLQ topic for failed messages (bad frames, inference errors) instead of silently dropping them |
| **Single points of failure** | Run multiple replicas of stateless services (preprocessor, inference) with Kafka consumer groups for load balancing |
| **No health endpoints** | Add `/health` and `/ready` endpoints to Flask apps for container orchestration |

#### Observability & Monitoring

| Gap | Solution |
|-----|----------|
| **No centralized logging** | Add ELK stack (Elasticsearch/Logstash/Kibana) or Loki + Grafana |
| **No metrics** | Integrate Prometheus + Grafana for FPS, latency, queue depth, detection counts |
| **No distributed tracing** | Add OpenTelemetry to trace a frame from ingestion to storage |
| **Scattered logging** | Use structured JSON logging with correlation IDs per frame |

---

### 🟡 Medium Priority

#### Performance & Scalability

| Issue | Recommendation |
|-------|----------------|
| **Single Kafka partition** | Increase partitions to enable parallel consumption. Currently bottlenecked at 1 consumer per topic |
| **CPU-only inference** | Add GPU support with NVIDIA Container Toolkit for 10-50x speedup |
| **Frame skipping is aggressive** | Implement adaptive frame sampling based on scene changes (motion detection) rather than always taking latest |
| **No backpressure handling** | Add metrics to detect when consumers fall behind and trigger alerts |
| **Preprocessor does nothing** | Either remove it (direct producer→YOLO) or enable the CLAHE preprocessing that's currently bypassed |

#### Data & Storage

| Issue | Recommendation |
|-------|----------------|
| **JSON batches hard to query** | Store in Parquet format or use a time-series DB (TimescaleDB, InfluxDB) for analytics |
| **No frame storage** | Optionally save annotated frames to MinIO for review/debugging |
| **No data retention policy** | Add lifecycle rules to MinIO to auto-expire old data |
| **Hardcoded camera ID** | Make `camera_id` dynamic, support multiple cameras |

---

### 🟢 Lower Priority

#### Security

| Risk | Mitigation |
|------|------------|
| **MinIO credentials in plain text** | Use Docker secrets or HashiCorp Vault |
| **No authentication on viewers** | Add basic auth or OAuth to Flask apps |
| **No TLS** | Enable HTTPS for web viewers, TLS for Kafka |
| **Network mode host** | Avoid `network_mode: host` on producer; use proper Docker networking |

#### Developer Experience

| Pain Point | Fix |
|------------|-----|
| **Long rebuild times** | Use multi-stage Dockerfiles, separate dependency layers |
| **No local development mode** | Add `docker-compose.override.yml` with hot-reload (volume mounts) |
| **No tests** | Add unit tests for preprocessing, integration tests for Kafka flow |
| **No CI/CD** | Add GitHub Actions for build, test, and push to registry |

#### Architecture Enhancements

| Enhancement | Benefit |
|-------------|---------|
| **Add Redis for caching** | Cache model weights, share state between replicas |
| **Kubernetes migration** | Better scaling, self-healing, resource management |
| **Add API gateway** | Unified entry point with rate limiting (Kong, Traefik) |
| **Model versioning** | Store models in MLflow or MinIO with version tracking |
| **Event sourcing** | Store raw events for replay/reprocessing during model updates |

---

### ⚡ Quick Wins (Low Effort, High Impact)

1. **Enable Kafdrop authentication** - currently open to anyone
2. **Add `restart: unless-stopped`** to MinIO service
3. **Pin image versions** - avoid `latest` tags for reproducibility
4. **Add `.dockerignore`** - exclude `pip_wheels/` from unnecessary copies
5. **Externalize all configs** - move hardcoded values like ports to `.env`
6. **Add graceful shutdown** - handle SIGTERM properly in all Python services

---

### 📅 Improvement Roadmap

```
Phase 1 (Immediate):   Observability → Prometheus + Grafana dashboards
Phase 2 (Short-term):  Reliability   → DLQ, health checks, manual Kafka commits
Phase 3 (Medium-term): Performance   → GPU support, multi-partition Kafka
Phase 4 (Long-term):   Scale         → Kubernetes migration, multi-camera support
```

---

## Use Cases

- **Security/Surveillance**: Real-time detection of people, vehicles, objects
- **IoT Edge Analytics**: Process ESP32-CAM streams for smart home/building
- **Traffic Monitoring**: Detect and count vehicles/pedestrians
- **Industrial Inspection**: Monitor production lines for defects
- **Research & Development**: ML model experimentation with live video feeds
- **Retail Analytics**: Customer counting and behavior analysis

---

## Project Structure

```
corvision/
├── docker-compose.yml          # Main orchestration file
├── .env                        # Environment configuration
├── test.py                     # Camera stream testing utility
│
├── kafka-producers/            # Video stream ingestion
│   ├── Dockerfile.producer
│   ├── camera-stream-1.py
│   └── requirements.txt
│
├── kafka-consumer/             # Frame preprocessing
│   ├── Dockerfile.consumer
│   ├── preprocessor.py
│   └── requirements.txt
│
├── model/                      # YOLO inference service
│   ├── Dockerfile.model
│   ├── model_inference.py
│   ├── yolo11n.pt             # YOLO model weights
│   ├── requirements.txt
│   └── pip_wheels/            # Offline dependencies
│
├── minio/                      # Detection data storage
│   ├── Dockerfile.minio-writer
│   ├── minio-writer.py
│   └── requirements.txt
│
├── view-process/               # Preprocessed frame viewer
│   ├── Dockerfile.viewer
│   ├── web_viewer.py
│   └── requirements.txt
│
└── view-detection/             # Detection result viewer
    ├── Dockerfile.viewer-d
    ├── detector_viewer.py
    ├── requirements.txt
    └── pip_wheels/            # Offline dependencies
```

---

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Acknowledgments

- [Ultralytics YOLO](https://docs.ultralytics.com/) for the object detection model
- [Apache Kafka](https://kafka.apache.org/) for the message streaming platform
- [MinIO](https://min.io/) for S3-compatible object storage
- [Confluent](https://www.confluent.io/) for Kafka Docker images
