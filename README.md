# CVops - Real-Time Computer Vision Pipeline

[![Python 3.12](https://img.shields.io/badge/Python-3.12-blue.svg)](https://www.python.org/downloads/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://docs.docker.com/compose/)
[![YOLO](https://img.shields.io/badge/YOLO-v11-00FFFF.svg)](https://docs.ultralytics.com/)
[![Kafka](https://img.shields.io/badge/Apache-Kafka-231F20.svg)](https://kafka.apache.org/)

A **distributed, real-time computer vision pipeline** designed for edge-to-cloud video analytics. The system ingests video streams from IoT cameras (ESP32-CAM), processes frames through a series of microservices, performs **YOLO object detection**, and stores detection data for later analysis.

---

## Sandbox Station (Submission)

This repository serves as a sandbox station baseline for **Edge AI Vision** exploration. Attendees can tune inference parameters, sampling strategies, and preprocessing to explore real-time performance/accuracy trade-offs on edge hardware.

- **Station Overview**: [SANDBOX_STATION.md](SANDBOX_STATION.md)
- **Decision Log**: [sandbox/LEDGER.md](sandbox/LEDGER.md)

---

## Documentation

- **[System Architecture](doc/architecture/README.md)** - Complete architecture overview, components, and data flow
- **[Quick Start Guide](doc/guides/OBSERVABILITY_QUICKSTART.md)** - Get up and running in 5 minutes
- **[GPU Setup](doc/guides/GPU_SETUP.md)** - Hardware acceleration configuration
- **[Observability Guide](doc/guides/OBSERVABILITY.md)** - Prometheus and Grafana setup
- **[Performance Tuning](doc/guides/PERFORMANCE_TUNING.md)** - CPU optimization strategies
- **[V2 Proposals](doc/proposals/)** - Future enhancement plans

---

## Table of Contents

- [Quick Start](#quick-start)
- [Technology Stack](#technology-stack)
- [Configuration](#configuration)
- [Web Interfaces](#web-interfaces)
- [Common Operations](#common-operations)
- [Documentation](#documentation)


---

## Quick Start

### Prerequisites
- Docker & Docker Compose
- ESP32-CAM or IP camera with HTTP stream

### 1. Clone and Configure

```bash
git clone https://github.com/Akin-ctrl/CVops.git
cd CVops

# Configure your camera URL
nano .env
```

### 2. Build and Start

```bash
# Build all services
docker compose build

# Start infrastructure
docker compose up -d
```

### 3. Access Web Interfaces

| Service | URL | Credentials |
|---------|-----|-------------|
| **Grafana Dashboard** | http://localhost:3000 | admin / admin |
| **Prometheus** | http://localhost:9090 | (none) |
| Preprocessed Frames | http://localhost:5000 | (none) |
| Detection Results | http://localhost:7000 | (none) |
| Kafka Management | http://localhost:19000 | (none) |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |

### 4. Common Operations

```bash
# View logs
docker compose logs -f

# Restart a service
docker compose restart yolo_inference

# Stop all services
docker compose down

# Full cleanup (removes volumes)
docker compose down -v
```

For detailed setup instructions, see the [Quick Start Guide](doc/guides/OBSERVABILITY_QUICKSTART.md).

---

## Technology Stack

| Layer | Technology |
|-------|------------|
| **Language** | Python 3.12 |
| **ML Framework** | Ultralytics YOLO11 |
| **Message Broker** | Apache Kafka (Confluent 7.5.0) |
| **Object Storage** | MinIO (S3-compatible) |
| **Web Framework** | Flask |
| **Computer Vision** | OpenCV |
| **Monitoring** | Prometheus + Grafana |
| **Containerization** | Docker, Docker Compose |

---

## Configuration

Core settings in `.env`:

```env
# Camera
URL=http://192.168.x.x:8080/stream

# Kafka
KAFKA_BROKER=kafka:9092

# YOLO
YOLO_INPUT_SIZE_WIDTH=640
YOLO_INPUT_SIZE_HEIGHT=640
MODEL_WEIGHTS_PATH=yolo11n.pt

# MinIO
MINIO_HOST=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
```

For advanced configuration and tuning, see:
- [Performance Tuning Guide](doc/guides/PERFORMANCE_TUNING.md)
- [GPU Setup Guide](doc/guides/GPU_SETUP.md)
- [Architecture Documentation](doc/architecture/README.md)

---

## License

MIT License - See LICENSE file for details.

---

## Additional Resources

- **System Architecture**: [doc/architecture/README.md](doc/architecture/README.md)
- **GPU Acceleration**: [doc/guides/GPU_SETUP.md](doc/guides/GPU_SETUP.md)  
- **Monitoring Setup**: [doc/guides/OBSERVABILITY.md](doc/guides/OBSERVABILITY.md)
- **CPU Optimization**: [doc/guides/PERFORMANCE_TUNING.md](doc/guides/PERFORMANCE_TUNING.md)
- **Future Plans**: [doc/proposals/](doc/proposals/)

For questions or issues, please open a GitHub issue.
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

The **Overview** dashboard provides:

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

### Useful Prometheus Queries

```promql
# Current FPS by service
rate(corvision_frames_processed_total[1m])

# Average processing latency
avg(corvision_processing_latency_ms) by (service)

# Total detections in last hour
increase(corvision_detections_total[1h])

# Service health status
corvision_service_up == 0  # Shows down services

# Top detected classes
topk(5, sum by (class_name) (corvision_detections_total))
```

### Metrics Endpoints

Each service exposes metrics at:

| Service | Metrics Port | Endpoint |
|---------|--------------|----------|
| kafka-producer | 8000 | http://localhost:8000/metrics |
| preprocessor | 8001 | http://localhost:8001/metrics |
| yolo-inference | 8002 | http://localhost:8002/metrics |
| minio-writer | 8003 | http://localhost:8003/metrics |
| kafka-viewer | 8004 | http://localhost:8004/metrics |
| detector-viewer | 8005 | http://localhost:8005/metrics |

### Monitoring Best Practices

**Key Metrics to Watch:**
- **FPS** - Ensures real-time processing capability
- **Latency** - Detects performance degradation early
- **Error Rate** - Catches failures before they cascade
- **Consumer Lag** - Prevents Kafka queue buildup

**Recommended Alerts:**
- Service downtime (immediate notification)
- High latency (> 500ms for 5 minutes)
- Error spikes (> 1 error/sec for 2 minutes)
- Consumer lag (> 5000 messages for 5 minutes)

### Troubleshooting

#### Services Won't Start
```bash
# Check logs
docker compose logs prometheus grafana

# Verify configuration
docker compose config

# Restart observability stack
docker compose restart prometheus grafana
```

#### No Data in Grafana
1. Wait 30 seconds for first Prometheus scrape
2. Check time range in Grafana (top-right) - set to "Last 5 minutes"
3. Verify Prometheus datasource: **Configuration** → **Data Sources**
4. Ensure all services are running: `docker compose ps`

#### Prometheus Targets Show "DOWN"
```bash
# Check if service is exposing metrics
curl http://localhost:8002/metrics

# Verify service is running
docker compose ps yolo_inference

# Check Prometheus configuration
docker compose exec prometheus cat /etc/prometheus/prometheus.yml
```

### Common Issues

**Problem: Low FPS**
```promql
# Check which service is the bottleneck
rate(corvision_frames_processed_total[1m]) by (service)
```

**Problem: High Latency**
```promql
# Identify slow services
corvision_processing_latency_ms > 500
```

**Problem: Detection Quality Issues**
```promql
# Check confidence distribution
histogram_quantile(0.5, corvision_detection_confidence)
```

### Additional Resources

 **Full Documentation**: [Observability Guide](doc/guides/OBSERVABILITY.md) - Complete guide with advanced queries, alerting, and best practices  
 **Quick Reference**: [Quick Start Guide](doc/guides/OBSERVABILITY_QUICKSTART.md) - 5-minute setup guide  

