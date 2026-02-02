# Sandbox Decisions

Document key architectural and configuration decisions made during sandbox exploration.

---

## Decision Template

### Decision: 

- **Context**: 
- **Options**: 
- **Choice**: 
- **Why**: 
- **Consequences**: 

---

## Example Decision

### Decision: Use frame dropping vs queuing for backpressure

- **Context**: YOLO inference slower than camera FPS (30 FPS camera, 15 FPS inference)
- **Options**: 
  1. Queue all frames (builds lag, increases memory)
  2. Drop oldest frames (always process latest)
  3. Adaptive sampling (skip static frames)
- **Choice**: Drop oldest frames + adaptive sampling
- **Why**: Real-time monitoring prioritizes latest state over complete history; motion detection reduces redundant processing
- **Consequences**: Some frames never analyzed, but latency stays bounded; 60-80% CPU savings in static scenes

---

## Your Decisions

### Decision: 

- **Context**: 
- **Options**: 
- **Choice**: 
- **Why**: 
- **Consequences**: 
