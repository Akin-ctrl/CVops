# Sandbox Station: Edge AI Vision (CVops)

## Station
**Edge AI Vision (CVops)**

## Baseline
Camera stream → Kafka ingestion → YOLO object detection → annotated output viewer + Grafana metrics dashboard. Full pipeline runs on Jetson with ~30 FPS inference.

## Open Seams (Modifiable)
Attendees can tune the following parameters to explore performance/accuracy trade-offs:

- **Inference input size** (320×320, 416×416, 640×640)
- **Confidence threshold** (0.25 - 0.9)
- **Tracking on/off** (ByteTrack)
- **Frame sampling / FPS** (1-30 FPS, adaptive motion detection)
- **Preprocessing toggles** (CLAHE, normalization)
- **Buffering policy** (drop latest vs queue with backpressure)

## How Attendees Modify Seams
Edit configuration in `.env` file or service environment variables, then restart only the relevant Docker container (e.g., `docker compose restart yolo_inference`).

## Constraint
Target p95 inference latency < 200ms; power consumption ~1W where possible.

## Measurement
Grafana dashboard displays real-time inference latency p95 and FPS metrics. Access at `http://localhost:3000` → "CVops Overview" dashboard.

## Decision Log
- **Ledger**: [sandbox/LEDGER.md](sandbox/LEDGER.md)
- **Decisions**: [sandbox/DECISIONS.md](sandbox/DECISIONS.md)
- **Failure Modes**: [sandbox/FAILURE_MODES.md](sandbox/FAILURE_MODES.md)
