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
- [Observability & Monitoring](#observability--monitoring)
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
│  ┌─────────────────┐      ┌─────────────────┐                                   │
│  │  kafka-producer │────▶ │   Kafka Topic   │                                  │
│  │ (camera-stream) │      │  (Raw Frames)   │                                   │
│  └─────────────────┘      └────────┬────────┘                                   │
│                                   │                                             │
│                                   ▼                                             │
│                          ┌─────────────────┐       ┌─────────────────┐          │
│                          │   preprocessor  │────▶ │   Kafka Topic    │         │
│                          │  (frame relay)  │       │ "yolo-input-frames"        │
│                          └─────────────────┘       └──────┬──────────┘          │
│                                                           │                     │
│                                    ┌──────────────────────┴──────────────┐      │
│                                    ▼                                     │      │
│  ┌───────────────────────────────────────────────────────────────────┐   │      │
│  │                    yolo-inference                                 │   │      │
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
│    ┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐          │
│    │    Zookeeper    │◀───▶ │      Kafka      │◀───▶ │     Kafdrop     │          │
│    │   (Port 2181)   │       │  (9092, 29092)  │       │  (Port 19000)   │          │
│    └─────────────────┘       └─────────────────┘       └─────────────────┘             │
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

### 2. Build Services (First Time Only)

```bash
# Build all services to include metrics support
docker compose build

# Or build specific services
docker compose build kafka-producer kafka-preprocessor yolo-inference minio-writer
```

### 3. Start All Services

```bash
docker compose up -d
```

### 4. Access Web Interfaces

| Service | URL | Credentials |
|---------|-----|-------------|
| **Grafana Dashboard** | http://localhost:3000 | admin / admin |
| **Prometheus** | http://localhost:9090 | (none) |
| Preprocessed Frames | http://localhost:5000 | (none) |
| Detection Results | http://localhost:7000 | (none) |
| Kafka Management | http://localhost:19000 | (none) |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |

**First-time Grafana setup:**
1. Login with `admin` / `admin`
2. Change password when prompted
3. Go to **Dashboards** → **CoRVision Overview** to see real-time metrics

### 5. Verify Observability

```bash
# Check if metrics are being collected
curl http://localhost:8000/metrics  # Producer
curl http://localhost:8002/metrics  # YOLO Inference

# Check Prometheus targets (all should show "UP")
# Open http://localhost:9090 → Status → Targets
```

### 6. View Logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f yolo_inference

# Observability services
docker compose logs -f prometheus grafana
```

### 7. Stop Services

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

### Core Services

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

### Observability & Metrics

| Port | Service | Purpose |
|------|---------|---------|
| 3000 | Grafana | Dashboards & visualization |
| 9090 | Prometheus | Metrics database |
| 8000 | Producer Metrics | Prometheus scrape endpoint |
| 8001 | Preprocessor Metrics | Prometheus scrape endpoint |
| 8002 | YOLO Metrics | Prometheus scrape endpoint |
| 8003 | MinIO Writer Metrics | Prometheus scrape endpoint |
| 8004 | Viewer Metrics | Prometheus scrape endpoint |
| 8005 | Detector Viewer Metrics | Prometheus scrape endpoint |

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

## Observability & Monitoring

CoRVision includes comprehensive observability with **Prometheus** for metrics collection and **Grafana** for visualization, providing real-time monitoring of the entire pipeline from ingestion to storage.

### What's Included

 **Prometheus** - Metrics collection and time-series database  
 **Grafana** - Beautiful dashboards with pre-configured CoRVision Overview  
 **Real-time Metrics** - FPS, latency, detections, errors, and more  
 **Service Health Monitoring** - Instant visibility into service status  
 **Automated Setup** - Pre-provisioned datasources and dashboards  

### Quick Access

After starting the system with `docker compose up -d`, access:

| Service | URL | Credentials |
|---------|-----|-------------|
| **Grafana Dashboard** | http://localhost:3000 | admin / admin |
| **Prometheus** | http://localhost:9090 | (none) |

**First-time Grafana setup:**
1. Login with `admin` / `admin`
2. Change password when prompted  
3. Navigate to **Dashboards** → **CoRVision Overview**
4. View real-time metrics across all services

### Dashboard Panels

The **CoRVision Overview** dashboard provides:

 **Processing FPS by Service** - Real-time frames per second for each microservice  
 **Processing Latency** - Gauge showing current latency with color-coded thresholds  
 **Total Detections by Class** - Time series of detected objects (person, car, etc.)  
 **Service Health** - Status indicators showing which services are up/down  
 **Kafka Message Throughput** - Messages consumed/produced per second  
 **Error Rate** - Errors per service over time, grouped by error type  

### Observability Architecture
```
┌──────────────────────────────────────────────────────────────┐
│                     CoRVision Services                       │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐      │
│  │ Producer │  │Preprocess│  │   YOLO   │  │  MinIO   │      │
│  │  :8000   │  │  :8001   │  │  :8002   │  │ Writer   │      │
│  │          │  │          │  │          │  │  :8003   │      │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘      │
│       │             │             │             │            │
│       └─────────────┴─────────────┴─────────────┘            │
│                         │ Metrics (HTTP)                     │
│                         ▼                                    │
│              ┌─────────────────────┐                         │
│              │    Prometheus       │                         │
│              │      :9090          │                         │
│              └──────────┬──────────┘                         │
│                         │ PromQL                             │
│                         ▼                                    │
│              ┌─────────────────────┐                         │
│              │      Grafana        │                         │
│              │       :3000         │                         │
│              └─────────────────────┘                         │
└──────────────────────────────────────────────────────────────┘
```

### Metrics Collected

#### System-Wide Metrics
- `corvision_frames_processed_total` - Total frames processed by each service
- `corvision_processing_latency_ms` - Processing latency in milliseconds
- `corvision_kafka_messages_consumed_total` - Messages consumed per topic
- `corvision_kafka_messages_produced_total` - Messages produced per topic
- `corvision_service_up` - Service health status (1=up, 0=down)
- `corvision_errors_total` - Total errors by type and service

#### YOLO Inference Metrics
- `corvision_detections_total{class_name}` - Total detections per class (person, car, etc.)
- `corvision_detection_confidence{class_name}` - Confidence score histogram
- `corvision_inference_fps` - Real-time inference frames per second

#### MinIO Writer Metrics
- `corvision_minio_batches_written_total` - Total batches written to storage
- `corvision_minio_records_written_total` - Total records written
- `corvision_minio_write_duration_seconds` - Write operation duration

### Verify Metrics Collection

```bash
# Check if metrics are being collected
curl http://localhost:8000/metrics  # Producer
curl http://localhost:8001/metrics  # Preprocessor
curl http://localhost:8002/metrics  # YOLO Inference
curl http://localhost:8003/metrics  # MinIO Writer

# Check Prometheus targets (all should show "UP")
# Open http://localhost:9090 → Status → Targets
```

---



