# Quick Start: Enable Observability

## What's Included

✅ **Prometheus** - Metrics collection (port 9090)  
✅ **Grafana** - Visualization dashboards (port 3000)  
✅ **Pre-configured Dashboard** - CoRVision Overview  
✅ **Metrics Integration** - All services instrumented  

---

## Setup (5 minutes)

### 1. Rebuild Services with Metrics Support

```bash
# Rebuild all services to include prometheus-client
docker compose build

# Or rebuild specific services
docker compose build kafka-producer kafka-preprocessor yolo-inference minio-writer
```

### 2. Start Everything

```bash
docker compose up -d
```

### 3. Access Grafana

1. Open http://localhost:3000
2. Login: `admin` / `admin`
3. Change password when prompted
4. Go to **Dashboards** → **CoRVision Overview**

---

## What You'll See

### CoRVision Overview Dashboard

📊 **Processing FPS** - Real-time frames per second for each service  
⏱️ **Processing Latency** - How long each service takes  
🎯 **Detections** - Object detection counts by class  
✅ **Service Health** - Which services are up/down  
📨 **Kafka Throughput** - Message flow through pipeline  
❌ **Error Rate** - Errors by service and type  

---

## Quick Health Check

### View Metrics Directly

```bash
# Check if metrics are being collected
curl http://localhost:8000/metrics  # Producer
curl http://localhost:8001/metrics  # Preprocessor
curl http://localhost:8002/metrics  # YOLO Inference
curl http://localhost:8003/metrics  # MinIO Writer
```

### Check Prometheus Targets

1. Open http://localhost:9090
2. Go to **Status** → **Targets**
3. All should show "UP" (green)

---

## Exposed Ports

| Service | Port | Purpose |
|---------|------|---------|
| Grafana | 3000 | Dashboards |
| Prometheus | 9090 | Metrics database |
| Producer Metrics | 8000 | Prometheus scrape endpoint |
| Preprocessor Metrics | 8001 | Prometheus scrape endpoint |
| YOLO Metrics | 8002 | Prometheus scrape endpoint |
| MinIO Writer Metrics | 8003 | Prometheus scrape endpoint |
| Viewer Metrics | 8004 | Prometheus scrape endpoint |
| Detector Viewer Metrics | 8005 | Prometheus scrape endpoint |

---

## Troubleshooting

### Services Won't Start

```bash
# Check logs
docker compose logs prometheus
docker compose logs grafana

# Verify configuration
docker compose config
```

### No Data in Grafana

1. Wait 30 seconds for first scrape
2. Check time range (top-right) - set to "Last 5 minutes"
3. Verify Prometheus datasource in Grafana settings
4. Ensure services are running: `docker compose ps`

### Grafana Shows "No Datasource"

```bash
# Restart Grafana
docker compose restart grafana

# Check datasource provisioning
docker compose exec grafana ls /etc/grafana/provisioning/datasources/
```

---

## Next Steps

📖 Read full documentation: [OBSERVABILITY.md](./OBSERVABILITY.md)  
🎯 Customize dashboards in Grafana  
🔔 Set up alerts for production use  
📊 Analyze metrics to optimize performance  

---

## Key Metrics to Watch

```promql
# FPS (frames per second)
rate(corvision_frames_processed_total[1m])

# Average latency
avg(corvision_processing_latency_ms) by (service)

# Total detections
sum(corvision_detections_total) by (class_name)

# Service health
corvision_service_up
```

---

## Files Added

```
corvision/
├── prometheus/
│   └── prometheus.yml              # Prometheus configuration
├── grafana/
│   └── provisioning/
│       ├── datasources/
│       │   └── prometheus.yml      # Auto-configure Prometheus datasource
│       └── dashboards/
│           ├── dashboard.yml       # Dashboard provider config
│           └── corvision-overview.json  # Pre-built dashboard
├── OBSERVABILITY.md               # Full documentation
└── OBSERVABILITY_QUICKSTART.md    # This file
```

---

**That's it!** Your CoRVision system now has full observability. 🎉
