# CoRVision Observability Guide

## Overview

CoRVision now includes comprehensive observability with **Prometheus** for metrics collection and **Grafana** for visualization. This allows real-time monitoring of the entire pipeline from ingestion to storage.

---

## Quick Start

### 1. Start the System with Observability

```bash
# Start all services including Prometheus and Grafana
docker compose up -d

# Verify observability services are running
docker compose ps prometheus grafana
```

### 2. Access Dashboards

| Service | URL | Credentials |
|---------|-----|-------------|
| **Grafana** | http://localhost:3000 | admin / admin |
| **Prometheus** | http://localhost:9090 | (none required) |

### 3. View Pre-configured Dashboard

1. Open Grafana at http://localhost:3000
2. Login with `admin / admin` (change password on first login)
3. Navigate to **Dashboards** → **CoRVision Overview**

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     CoRVision Services                       │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐     │
│  │ Producer │  │Preprocess│  │   YOLO   │  │  MinIO   │     │
│  │  :8000   │  │  :8001   │  │  :8002   │  │ Writer   │     │
│  │          │  │          │  │          │  │  :8003   │     │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘     │
│       │             │             │             │           │
│       └─────────────┴─────────────┴─────────────┘           │
│                         │                                    │
│                         │ Metrics (HTTP)                     │
│                         ▼                                    │
│              ┌─────────────────────┐                         │
│              │    Prometheus       │                         │
│              │      :9090          │                         │
│              └──────────┬──────────┘                         │
│                         │                                    │
│                         │ PromQL                             │
│                         ▼                                    │
│              ┌─────────────────────┐                         │
│              │      Grafana        │                         │
│              │       :3000         │                         │
│              └─────────────────────┘                         │
└──────────────────────────────────────────────────────────────┘
```

---

## Metrics Collected

### System-Wide Metrics

#### Frame Processing
- `corvision_frames_processed_total` - Total frames processed by each service
- `corvision_processing_latency_ms` - Processing latency in milliseconds
- `corvision_processing_duration_seconds` - Histogram of processing times

#### Kafka Metrics
- `corvision_kafka_messages_consumed_total` - Messages consumed per topic
- `corvision_kafka_messages_produced_total` - Messages produced per topic
- `corvision_kafka_consumer_lag` - Consumer lag by partition

#### Health & Errors
- `corvision_service_up` - Service health status (1=up, 0=down)
- `corvision_errors_total` - Total errors by type and service

---

### Service-Specific Metrics

#### YOLO Inference (`yolo-inference:8002`)
- `corvision_detections_total{class_name}` - Total detections per class
- `corvision_detection_confidence{class_name}` - Confidence score histogram
- `corvision_inference_fps` - Real-time inference FPS

#### MinIO Writer (`minio-writer:8003`)
- `corvision_minio_batches_written_total` - Total batches written
- `corvision_minio_records_written_total` - Total records written
- `corvision_minio_write_duration_seconds` - Write operation duration

---

## Grafana Dashboard

The pre-configured **CoRVision Overview** dashboard includes:

### Panels

1. **Processing FPS by Service**
   - Real-time FPS for each microservice
   - Helps identify bottlenecks

2. **Processing Latency**
   - Gauge showing current latency per service
   - Color-coded thresholds (green < 100ms, yellow < 500ms, red > 500ms)

3. **Total Detections by Class**
   - Time series of detected objects
   - Breakdown by YOLO class (person, car, etc.)

4. **Service Health**
   - Status indicators for all services
   - Instant visibility of outages

5. **Kafka Message Throughput**
   - Messages consumed/produced per second
   - Monitor pipeline flow

6. **Error Rate**
   - Errors per service over time
   - Grouped by error type

---

## Prometheus Queries

### Useful PromQL Examples

```promql
# Current FPS by service
rate(corvision_frames_processed_total[1m])

# Average processing latency
avg(corvision_processing_latency_ms) by (service)

# Total detections in last hour
increase(corvision_detections_total[1h])

# Service health status
corvision_service_up == 0  # Shows down services

# Kafka consumer lag alert
corvision_kafka_consumer_lag > 1000

# Error rate spike detection
rate(corvision_errors_total[5m]) > 0.1

# Top detected classes
topk(5, sum by (class_name) (corvision_detections_total))
```

---

## Custom Dashboards

### Create a New Dashboard

1. Go to Grafana → **Dashboards** → **New Dashboard**
2. Add panel with PromQL query
3. Configure visualization (Graph, Gauge, Table, etc.)
4. Save dashboard

### Example: Detection Confidence Dashboard

```json
{
  "title": "Detection Confidence",
  "targets": [{
    "expr": "histogram_quantile(0.95, corvision_detection_confidence)",
    "legendFormat": "{{class_name}} - 95th percentile"
  }]
}
```

---

## Alerting (Future Enhancement)

### Recommended Alerts

```yaml
groups:
  - name: corvision_alerts
    rules:
      - alert: ServiceDown
        expr: corvision_service_up == 0
        for: 1m
        annotations:
          summary: "CoRVision service {{ $labels.service }} is down"

      - alert: HighProcessingLatency
        expr: corvision_processing_latency_ms > 1000
        for: 5m
        annotations:
          summary: "High latency in {{ $labels.service }}"

      - alert: HighErrorRate
        expr: rate(corvision_errors_total[5m]) > 1
        for: 2m
        annotations:
          summary: "Error spike in {{ $labels.service }}"

      - alert: KafkaConsumerLag
        expr: corvision_kafka_consumer_lag > 5000
        for: 5m
        annotations:
          summary: "Consumer lag detected in {{ $labels.topic }}"
```

To enable alerting:
1. Configure Alertmanager in Prometheus
2. Set up notification channels (Slack, email, PagerDuty)
3. Add alert rules to `prometheus/alerts.yml`

---

## Metrics API Endpoints

Each service exposes metrics at:

```
http://<service>:<port>/metrics
```

| Service | Port | Example |
|---------|------|---------|
| kafka-producer | 8000 | http://localhost:8000/metrics |
| preprocessor | 8001 | http://localhost:8001/metrics |
| yolo-inference | 8002 | http://localhost:8002/metrics |
| minio-writer | 8003 | http://localhost:8003/metrics |
| kafka-viewer | 8004 | http://localhost:8004/metrics |
| detector-viewer | 8005 | http://localhost:8005/metrics |

### Example: Query Metrics Directly

```bash
# Get all metrics from YOLO inference
curl http://localhost:8002/metrics

# Filter specific metric
curl http://localhost:8002/metrics | grep corvision_detections_total
```

---

## Troubleshooting

### Prometheus Can't Scrape Service

**Problem**: Target shows as "DOWN" in Prometheus

**Solutions**:
```bash
# Check service is running
docker compose ps

# Check metrics port is exposed
docker compose logs <service-name>

# Verify metrics endpoint
curl http://localhost:<port>/metrics

# Check Prometheus config
docker compose exec prometheus cat /etc/prometheus/prometheus.yml
```

### Grafana Shows "No Data"

**Problem**: Dashboard panels are empty

**Solutions**:
1. Verify Prometheus datasource: **Configuration** → **Data Sources** → **Prometheus**
2. Test query in **Explore** tab
3. Check time range (top-right corner)
4. Verify services are producing metrics

### High Memory Usage

**Problem**: Prometheus consuming too much memory

**Solutions**:
```yaml
# Add to docker-compose.yml under prometheus service
command:
  - '--storage.tsdb.retention.time=7d'  # Keep only 7 days
  - '--storage.tsdb.retention.size=1GB' # Limit size
```

---

## Best Practices

### 1. Monitor Key Metrics

Focus on:
- **FPS** - Ensures real-time processing
- **Latency** - Detects performance degradation
- **Error Rate** - Catches failures early
- **Consumer Lag** - Prevents queue buildup

### 2. Set Up Alerts

Configure alerts for:
- Service downtime (immediate)
- High latency (> 500ms for 5 min)
- Error spikes (> 1 error/sec)
- Consumer lag (> 5000 messages)

### 3. Regular Review

- Check dashboard weekly
- Analyze trends (FPS degradation, detection patterns)
- Optimize based on metrics (adjust batch sizes, buffer limits)

### 4. Retention Policy

```yaml
# Prometheus retention (add to prometheus.yml)
global:
  scrape_interval: 15s
  evaluation_interval: 15s

# Storage settings (add to docker-compose)
--storage.tsdb.retention.time=30d
--storage.tsdb.retention.size=5GB
```

---

## Performance Impact

Metrics collection has **minimal overhead**:

- **Prometheus client**: ~1-2% CPU, ~10MB RAM per service
- **Metrics export**: ~100KB/s network traffic
- **Scraping**: 15-second intervals (configurable)

---

## Advanced: Custom Metrics

To add custom metrics to any service:

```python
from prometheus_client import Counter, Gauge, Histogram

# Define metric
custom_metric = Counter('corvision_custom_total', 'Custom metric', ['label'])

# Update metric
custom_metric.labels(label='value').inc()
```

---

## Next Steps

1. **Explore Dashboard**: Familiarize yourself with all panels
2. **Set Baselines**: Run for 24 hours to establish normal metrics
3. **Configure Alerts**: Set up notifications for critical issues
4. **Optimize**: Use metrics to tune performance (batch sizes, FPS targets)
5. **Extend**: Add custom metrics for business-specific KPIs

---

## Resources

- [Prometheus Documentation](https://prometheus.io/docs/)
- [Grafana Documentation](https://grafana.com/docs/)
- [PromQL Cheat Sheet](https://promlabs.com/promql-cheat-sheet/)
- [Prometheus Best Practices](https://prometheus.io/docs/practices/naming/)
