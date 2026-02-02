# Sandbox Ledger

Track all configuration changes, measurements, and trade-offs during sandbox exploration.

---

## Entry Template

### Entry #

- **Change**: 
- **Constraint targeted**: 
- **Metric before → after**: 
- **Trade-off**: 
- **Risk introduced**: 
- **Rollback**: 

---

## Example Entry

### Entry #1

- **Change**: Reduced YOLO input size from 640×640 to 416×416
- **Constraint targeted**: Inference latency < 200ms
- **Metric before → after**: p95 latency 250ms → 85ms, mAP@0.5 0.89 → 0.82
- **Trade-off**: 66% latency reduction at cost of 7% accuracy drop
- **Risk introduced**: Small objects (<30px) may be missed
- **Rollback**: Set `YOLO_INPUT_SIZE_WIDTH=640` and `YOLO_INPUT_SIZE_HEIGHT=640` in `.env`, restart yolo_inference

---

## Your Entries

### Entry #1

- **Change**: 
- **Constraint targeted**: 
- **Metric before → after**: 
- **Trade-off**: 
- **Risk introduced**: 
- **Rollback**: 
