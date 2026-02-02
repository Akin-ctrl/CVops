# Sandbox Failure Modes

Document observed failures, their detection, and mitigation strategies.

---

## Failure Mode Template

### Failure mode: 

- **Symptom**: 
- **Detection**: 
- **Mitigation**: 
- **Decision**: 

---

## Example Failure Mode

### Failure mode: Inference latency spike under high object count

- **Symptom**: p95 latency jumps from 80ms to 350ms when scene has >20 detected objects
- **Detection**: Grafana dashboard shows latency spike correlated with detection count increase
- **Mitigation**: 
  1. Reduce confidence threshold to filter low-confidence detections
  2. Disable tracking (ByteTrack adds overhead)
  3. Reduce input size to 320×320
- **Decision**: Disabled tracking, kept 416×416 input; latency dropped to 120ms with acceptable accuracy

---

## Your Failure Modes

### Failure mode: 

- **Symptom**: 
- **Detection**: 
- **Mitigation**: 
- **Decision**: 
