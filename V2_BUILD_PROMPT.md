# CVops-OilGas V2 Build Prompt

## Project Overview
Build an oil & gas safety monitoring system (CVops-OilGas V2) that extends the proven architecture from CVops V1 while adding multi-modal camera support, specialized safety detection models, and critical alerting capabilities for industrial environments.

## Source Material
**Base Framework (V1 - CVops):** 
- Repository: https://github.com/Akin-ctrl/CVops
- Core Architecture: Event-driven microservices with Apache Kafka message broker
- Proven Components: Observability stack (Prometheus/Grafana), Docker orchestration, MinIO storage, metrics collection
- Technologies: Python 3.12, Docker Compose, Kafka 7.5.0, YOLO11, Flask

**Target Use Case (V2):**
- Industrial oil & gas facility safety monitoring
- Multi-camera types: RGB (PPE detection), Thermal/FLIR (fire/heat detection), Gas detection cameras
- Real-time safety alerts with emergency shutdown capability
- Compliance logging for OSHA/API regulatory requirements

## Technical Requirements

### 1. Multi-Camera Producer Architecture
**Retain from V1:**
- Kafka producer pattern with prometheus_client metrics
- JPEG encoding for visual streams
- Frame rate control and error handling
- Metrics: frames_produced, messages_produced, processing_latency_ms, errors_total, service_up

**New Components:**
Create three specialized producers:

**A. RGB Camera Producer** (`cameras/rgb-producer.py`)
- HTTP MJPEG stream ingestion (similar to V1 camera-stream-1.py)
- Target: PPE compliance monitoring (hardhats, safety vests, gloves)
- Output Topic: `rgb-frames`
- Metrics Port: 8000
- Frame Rate: 30 FPS
- Enhancements: Motion detection to reduce unnecessary frames, ROI marking for high-risk zones

**B. Thermal Camera Producer** (`cameras/thermal-producer.py`)
- FLIR/thermal camera SDK integration (FLIR Lepton 3.5 or similar)
- Target: Fire detection, hot spot monitoring, temperature anomalies
- Output Topic: `thermal-frames`
- Metrics Port: 8010
- Frame Rate: 9 FPS (typical thermal camera rate)
- Additional Metrics: temperature_reading_celsius, hot_spot_detected
- Payload: Include temperature matrix alongside visual representation

**C. Gas Detection Producer** (`cameras/gas-producer.py`)
- Gas sensor camera integration (multispectral imaging)
- Target: Pipeline leak detection (methane, volatile compounds)
- Output Topic: `gas-frames`
- Metrics Port: 8020
- Frame Rate: 5 FPS (sufficient for gas detection)
- Additional Metrics: gas_concentration_ppm, leak_detected_bool
- Payload: Include spectral data and concentration readings

### 2. Enhanced Preprocessing Architecture
**Retain from V1:**
- Kafka consumer pattern with batch processing
- Background metrics server on port 8001
- Batch flush configuration (linger_ms=100)

**New Multi-Pipeline Preprocessor** (`preprocessor/multi_modal_preprocessor.py`):

**Architecture:**
- Three parallel consumer groups, one per camera type
- Separate preprocessing pipelines for each modality
- Unified output to respective YOLO input topics

**Pipeline A: RGB Pipeline** (`preprocessor/pipelines/rgb_pipeline.py`)
- Input Topic: `rgb-frames`
- Output Topic: `ppe-yolo-input`
- Processing Steps:
  1. CLAHE enhancement for low-light conditions
  2. High-visibility color enhancement (orange/yellow boost for PPE)
  3. ROI extraction for worker zones
  4. Resize to 640×640 (larger than V1 for better PPE detection)
  5. Normalization
- Metrics Port: 8001

**Pipeline B: Thermal Pipeline** (`preprocessor/pipelines/thermal_pipeline.py`)
- Input Topic: `thermal-frames`
- Output Topic: `fire-yolo-input`
- Processing Steps:
  1. Temperature matrix normalization (map sensor values to 0-255)
  2. Hot spot enhancement (amplify high-temperature regions)
  3. Colormap application (ironbow or jet colormap)
  4. Edge detection for flame contours
  5. Resize to 320×320 (V1 size sufficient for heat signatures)
  6. Temporal filtering (reduce false positives from brief heat sources)
- Metrics Port: 8002
- Additional: temperature_threshold_exceeded counter

**Pipeline C: Gas Pipeline** (`preprocessor/pipelines/gas_pipeline.py`)
- Input Topic: `gas-frames`
- Output Topic: `leak-yolo-input`
- Processing Steps:
  1. Spectral band isolation (focus on methane absorption wavelengths)
  2. Anomaly detection preprocessing (baseline subtraction)
  3. Concentration gradient enhancement
  4. Resize to 320×320
  5. Moving average filter (3-frame window to reduce sensor noise)
- Metrics Port: 8003
- Additional: concentration_above_threshold counter (LEL - Lower Explosive Limit)

### 3. Specialized YOLO Model Services
**Retain from V1:**
- FrameGrabber background thread pattern
- Kafka consumer/producer with LZ4 compression
- Dual output: visual annotations + JSON detections
- Prometheus metrics for detections_total, detection_confidence, inference_fps

**New Model Services:**

**A. PPE Detection Service** (`models/ppe-detection/ppe_inference.py`)
- Model: `ppe_yolo11m.pt` (medium model for accuracy)
- Input Topic: `ppe-yolo-input`
- Output Topics: `ppe-visual-output`, `ppe-data-output`
- Detection Classes: hardhat, safety_vest, safety_gloves, safety_goggles, no_ppe
- Metrics Port: 8100
- Additional Metrics: 
  - `ppe_compliance_violations` (counter by class)
  - `workers_without_ppe` (gauge)
- Inference: Confidence threshold 0.6 (higher than V1's 0.25 for fewer false positives)
- Alert Trigger: Detect "no_ppe" or missing required equipment in restricted zones

**B. Fire Detection Service** (`models/fire-detection/fire_inference.py`)
- Model: `fire_yolo11l.pt` (large model for critical safety)
- Input Topic: `fire-yolo-input`
- Output Topics: `fire-visual-output`, `fire-data-output`
- Detection Classes: flames, smoke, heat_signature, sparks
- Metrics Port: 8101
- Additional Metrics:
  - `fire_incidents_detected` (counter by severity)
  - `high_temperature_zones` (gauge)
- Inference: Confidence threshold 0.5, track objects across frames
- Alert Trigger: Any fire/smoke detection → immediate critical alert

**C. Leak Detection Service** (`models/leak-detection/leak_inference.py`)
- Model: `leak_yolo11m.pt` (medium model)
- Input Topic: `leak-yolo-input`
- Output Topics: `leak-visual-output`, `leak-data-output`
- Detection Classes: gas_leak, liquid_leak, vapor_cloud, pipeline_damage
- Metrics Port: 8102
- Additional Metrics:
  - `leak_incidents_detected` (counter by type)
  - `active_leaks` (gauge)
- Inference: Confidence threshold 0.55, temporal consistency (3-frame confirmation)
- Alert Trigger: Any leak detection → critical alert + location

### 4. Alert & Emergency Management System
**New Component** (does not exist in V1):

**A. Emergency Manager** (`alert-system/emergency_manager.py`)
- Consumes: `ppe-data-output`, `fire-data-output`, `leak-data-output`
- Purpose: Real-time alert routing and emergency response coordination
- Metrics Port: 8200

**Alert Severity Levels:**
1. **CRITICAL** - Fire detected, major gas leak (>25% LEL), unauthorized no-PPE in hazardous zone
   - Action: Trigger emergency shutdown protocols, send to SCADA system
2. **HIGH** - Minor leak, smoke detected, repeated PPE violations
   - Action: Notify supervisors, log incident, increase monitoring frequency  
3. **MEDIUM** - Single PPE violation, elevated temperature, minor anomalies
   - Action: Log warning, notify on-site personnel
4. **LOW** - Informational alerts, system health issues
   - Action: Log only

**Notification Channels:**
- SMS/Email for CRITICAL and HIGH alerts
- SCADA system integration for emergency shutdowns
- Web dashboard real-time notifications
- Kafka topic: `emergency-alerts` (consumed by all notification systems)

**De-duplication Logic:**
- 30-second window to prevent alert storms
- Incident correlation (fire + smoke = single incident)

**Metrics:**
- `alerts_sent_total` (counter by severity)
- `emergency_shutdowns_triggered` (counter)
- `alert_processing_latency_ms` (histogram)

**B. Notification Hub** (`alert-system/notification_hub.py`)
- Consumes: `emergency-alerts`
- Integrations: Twilio (SMS), SendGrid (email), custom SCADA API
- Metrics Port: 8201
- Configuration: Recipient lists by alert severity, retry logic

**C. Compliance Logger** (`alert-system/compliance_logger.py`)
- Consumes: All detection outputs + alerts
- Purpose: Audit trail for regulatory compliance (OSHA 1910.119, API RP 754)
- Storage: MinIO + PostgreSQL for structured query
- Metrics Port: 8202
- Log Format: ISO 8601 timestamps, incident UUID, detection confidence, video frame reference, response actions
- Retention: 7 years (regulatory requirement)

### 5. Enhanced Storage Architecture
**Retain from V1:**
- MinIO object storage pattern
- Batch writing (50 detections or 30 seconds)
- Metrics: batches_written, records_written, write_duration

**Enhanced MinIO Writer** (`storage/compliance_storage.py`)
- Additional Buckets:
  - `ppe-detections`: RGB camera detection records
  - `fire-incidents`: Fire/thermal detection records  
  - `leak-incidents`: Gas leak detection records
  - `compliance-logs`: Full audit trail
  - `video-evidence`: 30-second clips before/after incidents (new requirement)
- File Naming: `{incident_severity}/{YYYY}/{MM}/{DD}/{HH-MM-SS}_{incident_uuid}.json`
- Video Clip Storage: Triggered by CRITICAL alerts, store raw frames from 30s before detection
- Metrics Port: 8300

**PostgreSQL Integration** (new):
- Service: `postgres:15-alpine`
- Purpose: Structured incident database for compliance queries
- Schema:
  ```sql
  CREATE TABLE incidents (
    incident_id UUID PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    severity VARCHAR(20) NOT NULL,
    incident_type VARCHAR(50) NOT NULL,
    camera_id VARCHAR(50),
    detection_class VARCHAR(100),
    confidence FLOAT,
    alert_sent BOOLEAN,
    response_action TEXT,
    minio_reference TEXT,
    resolved_at TIMESTAMPTZ,
    INDEX idx_timestamp (timestamp),
    INDEX idx_severity (severity)
  );
  ```
- Connection: `alert-system/compliance_logger.py` writes to both MinIO and PostgreSQL

### 6. Observability Stack (Migrate from V1)
**Direct Migration:**
- Copy entire `prometheus/` directory from V1 (prometheus.yml with all scrape configs)
- Copy entire `grafana/` directory from V1 (provisioning configs, datasources, dashboards)
- Copy `common/metrics.py` utility for consistent metrics patterns

**Enhancements:**
- Update `prometheus.yml` with new scrape targets:
  - RGB producer: 8000, Thermal producer: 8010, Gas producer: 8020
  - RGB pipeline: 8001, Thermal pipeline: 8002, Gas pipeline: 8003
  - PPE model: 8100, Fire model: 8101, Leak model: 8102
  - Emergency manager: 8200, Notification hub: 8201, Compliance logger: 8202
  - Storage: 8300, PostgreSQL exporter: 9187

**New Grafana Dashboards:**
1. **Safety Overview Dashboard:**
   - Active incidents gauge (fire, leak, PPE violations)
   - Alert rate by severity (time series)
   - Emergency shutdown history (bar chart)
   - Detection confidence heatmap by model

2. **Compliance Dashboard:**
   - Total incidents by type (30-day rolling)
   - Response time histogram (alert → action)
   - PPE compliance rate (percentage gauge)
   - Regulatory KPIs (TRIR - Total Recordable Incident Rate)

3. **Camera Health Dashboard:**
   - Frame rates by camera type (multi-line graph)
   - Camera uptime percentage
   - Processing latency by pipeline (gauges)
   - Dropped frames counter

### 7. Docker Compose Architecture
**Retain from V1:**
- Kafka + Zookeeper infrastructure (same configuration)
- Volume management pattern
- Health checks and restart policies
- Network: bridge mode

**New docker-compose.yml Structure:**

```yaml
version: '3.8'

services:
  # Kafka Infrastructure (same as V1)
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    # ... V1 configuration ...

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    # ... V1 configuration ...
    # Enhancement: Increase partitions for high-volume topics
    environment:
      - KAFKA_NUM_PARTITIONS=3

  # Camera Producers (new)
  rgb-producer:
    build:
      context: ./cameras
      dockerfile: Dockerfile.rgb-producer
    ports:
      - "8000:8000"  # Metrics
    environment:
      - CAMERA_URL=${RGB_CAMERA_URL}
      - KAFKA_TOPIC=rgb-frames
    depends_on:
      - kafka

  thermal-producer:
    build:
      context: ./cameras
      dockerfile: Dockerfile.thermal-producer
    ports:
      - "8010:8010"  # Metrics
    devices:
      - /dev/video1:/dev/video1  # Thermal camera device
    depends_on:
      - kafka

  gas-producer:
    build:
      context: ./cameras
      dockerfile: Dockerfile.gas-producer
    ports:
      - "8020:8020"  # Metrics
    depends_on:
      - kafka

  # Multi-Modal Preprocessors (new)
  rgb-preprocessor:
    build:
      context: ./preprocessor
      dockerfile: Dockerfile.rgb-pipeline
    ports:
      - "8001:8001"
    depends_on:
      - kafka

  thermal-preprocessor:
    build:
      context: ./preprocessor
      dockerfile: Dockerfile.thermal-pipeline
    ports:
      - "8002:8002"
    depends_on:
      - kafka

  gas-preprocessor:
    build:
      context: ./preprocessor
      dockerfile: Dockerfile.gas-pipeline
    ports:
      - "8003:8003"
    depends_on:
      - kafka

  # YOLO Model Services (new)
  ppe-detection:
    build:
      context: ./models/ppe-detection
      dockerfile: Dockerfile
    ports:
      - "8100:8100"
    volumes:
      - ./models/ppe-detection/ppe_yolo11m.pt:/app/model.pt:ro
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: 1
              capabilities: [gpu]
    depends_on:
      - kafka

  fire-detection:
    build:
      context: ./models/fire-detection
      dockerfile: Dockerfile
    ports:
      - "8101:8101"
    volumes:
      - ./models/fire-detection/fire_yolo11l.pt:/app/model.pt:ro
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: 1
              capabilities: [gpu]
    depends_on:
      - kafka

  leak-detection:
    build:
      context: ./models/leak-detection
      dockerfile: Dockerfile
    ports:
      - "8102:8102"
    volumes:
      - ./models/leak-detection/leak_yolo11m.pt:/app/model.pt:ro
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: 1
              capabilities: [gpu]
    depends_on:
      - kafka

  # Alert & Emergency System (new)
  emergency-manager:
    build:
      context: ./alert-system
      dockerfile: Dockerfile.emergency-manager
    ports:
      - "8200:8200"
    environment:
      - SCADA_API_URL=${SCADA_API_URL}
      - EMERGENCY_SHUTDOWN_ENABLED=true
    depends_on:
      - kafka

  notification-hub:
    build:
      context: ./alert-system
      dockerfile: Dockerfile.notification-hub
    ports:
      - "8201:8201"
    environment:
      - TWILIO_ACCOUNT_SID=${TWILIO_SID}
      - TWILIO_AUTH_TOKEN=${TWILIO_TOKEN}
      - SENDGRID_API_KEY=${SENDGRID_KEY}
    depends_on:
      - kafka

  compliance-logger:
    build:
      context: ./alert-system
      dockerfile: Dockerfile.compliance-logger
    ports:
      - "8202:8202"
    depends_on:
      - kafka
      - postgres
      - minio

  # Storage Layer (enhanced)
  minio:
    image: minio/minio:latest
    # ... V1 configuration ...
    # Add new buckets in entrypoint script

  postgres:
    image: postgres:15-alpine
    ports:
      - "5432:5432"
    environment:
      - POSTGRES_DB=compliance
      - POSTGRES_USER=${POSTGRES_USER}
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./storage/init.sql:/docker-entrypoint-initdb.d/init.sql

  postgres-exporter:
    image: prometheuscommunity/postgres-exporter
    ports:
      - "9187:9187"
    environment:
      - DATA_SOURCE_NAME=postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/compliance?sslmode=disable

  # Storage Writer (from V1, enhanced)
  compliance-storage:
    build:
      context: ./storage
      dockerfile: Dockerfile
    ports:
      - "8300:8300"
    depends_on:
      - kafka
      - minio

  # Observability (from V1, same configuration)
  prometheus:
    image: prom/prometheus:v2.48.0
    # ... V1 configuration ...

  grafana:
    image: grafana/grafana:10.2.2
    # ... V1 configuration ...

  # Web Viewers (optional, from V1)
  # Can be adapted for multi-camera viewing

volumes:
  zookeeper_data:
  kafka_data:
  minio_data:
  postgres_data:
  prometheus_data:
  grafana_data:
```

### 8. Configuration & Environment Variables
**Create `.env` file:**
```bash
# Camera Configuration
RGB_CAMERA_URL=http://192.168.1.100/stream
THERMAL_CAMERA_DEVICE=/dev/video1
GAS_CAMERA_URL=http://192.168.1.102/stream

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_PARTITIONS=3
KAFKA_REPLICATION_FACTOR=1

# MinIO Configuration
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=<strong-password>
MINIO_ENDPOINT=minio:9000

# PostgreSQL Configuration
POSTGRES_USER=compliance_user
POSTGRES_PASSWORD=<strong-password>
POSTGRES_DB=compliance

# Alert System Configuration
TWILIO_ACCOUNT_SID=<your-twilio-sid>
TWILIO_AUTH_TOKEN=<your-twilio-token>
TWILIO_PHONE_FROM=+1234567890
TWILIO_PHONE_TO=+1987654321
SENDGRID_API_KEY=<your-sendgrid-key>
EMERGENCY_EMAIL=safety@oilgas-company.com

# SCADA Integration
SCADA_API_URL=http://scada-system:8080/api/v1
SCADA_API_KEY=<scada-api-key>
EMERGENCY_SHUTDOWN_ENABLED=true

# Model Configuration
PPE_MODEL_PATH=/app/models/ppe_yolo11m.pt
FIRE_MODEL_PATH=/app/models/fire_yolo11l.pt
LEAK_MODEL_PATH=/app/models/leak_yolo11m.pt
INFERENCE_DEVICE=cuda  # or cpu for testing

# Alert Thresholds
GAS_CONCENTRATION_THRESHOLD_PPM=5000
TEMPERATURE_THRESHOLD_CELSIUS=80
PPE_CONFIDENCE_THRESHOLD=0.6
FIRE_CONFIDENCE_THRESHOLD=0.5
LEAK_CONFIDENCE_THRESHOLD=0.55

# Compliance Configuration
LOG_RETENTION_DAYS=2555  # 7 years
INCIDENT_VIDEO_CLIP_SECONDS=30
```

### 9. Documentation Requirements
**Create comprehensive README.md:**
- Project overview emphasizing safety-critical nature
- Architecture diagram showing all 3 camera types → preprocessing → 3 YOLO models → alert system → storage
- Quick start guide (similar to V1 but with camera setup instructions)
- Safety alert configuration guide
- Compliance logging and reporting section
- Troubleshooting section for camera connectivity, model performance, alert system

**Create SAFETY_PROTOCOLS.md:**
- Emergency shutdown procedures
- Alert escalation matrix
- Incident response workflows
- False positive handling
- System maintenance procedures during operations

**Create COMPLIANCE.md:**
- Regulatory standards reference (OSHA 1910.119, API RP 754)
- Data retention policies
- Audit trail access procedures
- Reporting templates

### 10. Testing & Validation Requirements
**Unit Tests:**
- Each preprocessing pipeline with synthetic thermal/gas data
- Alert severity classification logic
- Emergency shutdown trigger conditions

**Integration Tests:**
- End-to-end flow: mock camera → preprocessor → YOLO → alert → storage
- Alert de-duplication under high-frequency detections
- Database writes during Kafka outages (buffering)

**Safety-Critical Tests:**
- Fire detection response time (<2 seconds from frame to alert)
- Emergency shutdown trigger reliability (99.9% success rate)
- False positive rate benchmarking (target: <1% for fire detection)

**Load Testing:**
- 3 cameras at maximum FPS simultaneously
- Alert storm handling (100 alerts/second)
- 30-day continuous operation stability

### 11. Deployment Considerations
**Hardware Requirements:**
- GPU: NVIDIA RTX 3060 or better (12GB VRAM minimum for 3 simultaneous models)
- RAM: 32GB minimum
- Storage: 2TB SSD (1-year video evidence + logs)
- Network: Gigabit Ethernet for camera streams

**Production Checklist:**
- [ ] All camera connections tested and validated
- [ ] YOLO models trained and benchmarked (mAP >0.7 for each)
- [ ] Emergency shutdown integrated with SCADA system
- [ ] Alert notification phone numbers/emails configured
- [ ] PostgreSQL backups configured (daily)
- [ ] MinIO replication configured (if multi-site)
- [ ] Grafana dashboards accessible to safety team
- [ ] 72-hour burn-in test completed
- [ ] Regulatory compliance review completed

## Implementation Strategy

### Phase 1: Infrastructure Setup (Week 1)
1. Create new repository: CVops-OilGas
2. Copy observability stack from V1 (prometheus/, grafana/, common/)
3. Set up Docker Compose with Kafka, MinIO, PostgreSQL
4. Configure environment variables and secrets management

### Phase 2: Camera Integration (Week 2)
1. Implement RGB producer (adapt from V1 camera-stream-1.py)
2. Implement thermal producer with FLIR SDK integration
3. Implement gas camera producer with spectral data handling
4. Test all producers independently with metrics validation

### Phase 3: Preprocessing Pipelines (Week 3)
1. Implement RGB pipeline with CLAHE and PPE-optimized enhancements
2. Implement thermal pipeline with temperature normalization
3. Implement gas pipeline with spectral processing
4. Integration test: camera → preprocessor → Kafka output verification

### Phase 4: YOLO Model Deployment (Week 4-5)
1. Train/fine-tune PPE detection model on oil & gas datasets
2. Train/fine-tune fire detection model with thermal imagery
3. Train/fine-tune leak detection model with gas camera data
4. Deploy all three models with GPU optimization
5. Benchmark inference performance and accuracy

### Phase 5: Alert System (Week 6)
1. Implement emergency_manager.py with severity classification
2. Implement notification_hub.py with Twilio/SendGrid integration
3. Implement compliance_logger.py with dual storage (MinIO + PostgreSQL)
4. Test alert workflows end-to-end

### Phase 6: Integration & Testing (Week 7-8)
1. Full system integration test with all components
2. Safety-critical scenario testing (fire, leak, PPE violations)
3. Load testing and performance optimization
4. Security audit and penetration testing
5. Documentation finalization

### Phase 7: Deployment & Monitoring (Week 9)
1. Production deployment with staged rollout
2. 72-hour burn-in test with monitoring
3. Safety team training on dashboards and alert responses
4. Regulatory compliance review and sign-off

## Success Criteria
- **Detection Accuracy:** mAP >0.7 for each model (PPE, fire, leak)
- **Response Time:** Alert sent within 2 seconds of critical detection
- **Uptime:** 99.5% system availability (excluding planned maintenance)
- **False Positive Rate:** <1% for fire detection, <3% for PPE detection, <2% for leak detection
- **Compliance:** Pass regulatory audit with complete audit trail
- **Scalability:** Handle 5 additional cameras without infrastructure changes

## Migration from V1
**Components to Reuse Directly:**
- `prometheus/prometheus.yml` (update scrape targets)
- `grafana/provisioning/` (add new dashboards)
- `common/metrics.py` (unchanged)
- Kafka/Zookeeper Docker Compose configuration
- MinIO Docker Compose configuration

**Components to Adapt:**
- `kafka-producers/camera-stream-1.py` → `cameras/rgb-producer.py` (add camera type metadata)
- `kafka-consumer/preprocessor.py` → `preprocessor/multi_modal_preprocessor.py` (activate preprocessing, add pipeline routing)
- `model/model_inference.py` → `models/{ppe,fire,leak}-detection/*_inference.py` (add severity metrics, alert triggers)
- `minio/minio-writer.py` → `storage/compliance_storage.py` (add video clip storage, PostgreSQL writes)

**Components to Build New:**
- `cameras/thermal-producer.py` (thermal camera SDK integration)
- `cameras/gas-producer.py` (gas camera integration)
- `preprocessor/pipelines/*.py` (three specialized preprocessing pipelines)
- `alert-system/*.py` (entire alert and emergency management system)
- PostgreSQL schema and integration

## Key Differences from V1
| Aspect | V1 (CVops) | V2 (CVops-OilGas) |
|--------|-----------|-------------------|
| **Purpose** | Generic object detection demo | Safety-critical industrial monitoring |
| **Cameras** | Single RGB HTTP stream | 3 types: RGB, thermal, gas detection |
| **Preprocessing** | Passthrough (YOLO-internal) | Active multi-modal pipelines |
| **Models** | 1 YOLO11n (80 COCO classes) | 3 specialized models (PPE, fire, leak) |
| **Alerts** | None | Real-time with emergency shutdown |
| **Storage** | MinIO only | MinIO + PostgreSQL for compliance |
| **GPU** | Optional (CPU fallback) | Required (3 models simultaneously) |
| **Compliance** | Not applicable | OSHA/API regulatory requirements |
| **Testing** | Basic functionality | Safety-critical validation |

## Final Notes
- **Safety First:** All critical alerts (fire, major leaks) must trigger immediate notifications. Test emergency shutdown integration thoroughly before production.
- **Regulatory Compliance:** Ensure all audit logs are tamper-proof and retained for 7 years.
- **Performance:** GPU is mandatory for real-time performance with 3 concurrent models.
- **Maintenance:** Schedule weekly system health checks and monthly model performance reviews.
- **Scalability:** Architecture supports up to 10 cameras per site with current design.

---

**Prompt Usage:**
Provide this entire prompt to an AI coding agent or use it as a comprehensive specification document for building CVops-OilGas V2. The prompt contains sufficient technical detail to generate all necessary code, configuration files, and documentation while maintaining consistency with the proven V1 architecture.
