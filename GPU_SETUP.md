# GPU Acceleration Setup Guide

## Overview

CVops supports NVIDIA GPU acceleration for YOLO inference, providing **10-50x speedup** over CPU inference. This guide covers setup, configuration, and troubleshooting.

---

## Performance Comparison

| Configuration | Device | Input Size | FPS | Inference Time | Speedup |
|--------------|--------|------------|-----|----------------|---------|
| **CPU Only** | Intel Core i5/Ryzen 5 | 416x416 | 15-20 | ~55ms | 1x (baseline) |
| **GPU (Basic)** | NVIDIA GTX 1660 | 416x416 | 120-150 | ~7ms | ~8x |
| **GPU (Mid)** | NVIDIA RTX 3060 | 640x640 | 180-220 | ~5ms | ~20x |
| **GPU (High-End)** | NVIDIA RTX 4090 | 640x640 | 400+ | ~2-3ms | ~40-50x |

---

## Prerequisites

### Hardware Requirements
- NVIDIA GPU with CUDA Compute Capability 7.0+ (Pascal architecture or newer)
- Recommended: 4GB+ VRAM for 640x640, 2GB+ for 416x416
- Supported GPUs: GTX 1050 Ti and newer, RTX series, Tesla, A100, etc.

### Software Requirements
- Linux (Ubuntu 20.04+, Debian 11+, or compatible)
- Docker 20.10+
- Docker Compose 1.28+
- NVIDIA GPU drivers 470.0+ installed
- NVIDIA Container Toolkit

---

## Step 1: Install NVIDIA GPU Drivers

### Check Current Driver

```bash
nvidia-smi
```

If this command works and shows your GPU, drivers are installed. Skip to Step 2.

### Install Drivers (Ubuntu/Debian)

```bash
# Update package list
sudo apt update

# Install NVIDIA drivers (latest recommended)
sudo apt install -y nvidia-driver-535

# Reboot to load drivers
sudo reboot

# After reboot, verify installation
nvidia-smi
```

**Expected Output:**
```
+-----------------------------------------------------------------------------+
| NVIDIA-SMI 535.xx.xx    Driver Version: 535.xx.xx    CUDA Version: 12.2   |
|-------------------------------+----------------------+----------------------+
| GPU  Name        Persistence-M| Bus-Id        Disp.A | Volatile Uncorr. ECC |
| Fan  Temp  Perf  Pwr:Usage/Cap|         Memory-Usage | GPU-Util  Compute M. |
|===============================+======================+======================|
|   0  NVIDIA GeForce ...  Off  | 00000000:01:00.0  On |                  N/A |
| 30%   45C    P8    10W / 150W |    500MiB /  8192MiB |      0%      Default |
+-------------------------------+----------------------+----------------------+
```

---

## Step 2: Install NVIDIA Container Toolkit

The NVIDIA Container Toolkit enables Docker containers to access GPUs.

### Ubuntu/Debian Installation

```bash
# Add NVIDIA package repository
distribution=$(. /etc/os-release;echo $ID$VERSION_ID)
curl -s -L https://nvidia.github.io/nvidia-docker/gpgkey | sudo apt-key add -
curl -s -L https://nvidia.github.io/nvidia-docker/$distribution/nvidia-docker.list | \
  sudo tee /etc/apt/sources.list.d/nvidia-docker.list

# Update and install
sudo apt update
sudo apt install -y nvidia-container-toolkit

# Restart Docker daemon
sudo systemctl restart docker

# Verify installation
docker run --rm --gpus all nvidia/cuda:12.0.0-base-ubuntu22.04 nvidia-smi
```

**If successful**, you should see `nvidia-smi` output inside the container.

---

## Step 3: Enable GPU in CVops

### Option A: Docker Compose (Recommended)

Edit `docker-compose.yml` and uncomment the GPU configuration:

```yaml
  yolo-inference:
    # ... existing configuration ...
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: 1  # or 'all' for all GPUs
              capabilities: [gpu]
```

### Option B: Manual Docker Run

```bash
docker run --gpus all \
  -e DEVICE=cuda \
  -e KAFKA_BROKER=kafka:9092 \
  # ... other environment variables ...
  cvops-yolo-inference
```

### Start Services

```bash
# Stop existing services if running
docker compose down

# Rebuild with GPU support
docker compose build yolo-inference

# Start all services
docker compose up -d

# Check logs
docker compose logs -f yolo-inference
```

---

## Step 4: Verify GPU Usage

### Check Service Health

```bash
curl http://localhost:8002/health | jq
```

**Expected Response:**
```json
{
  "status": "healthy",
  "service": "yolo-inference",
  "device_type": "gpu",
  "gpu_available": true,
  "gpu_name": "NVIDIA GeForce RTX 3060",
  "gpu_memory_gb": 12.0,
  "cuda_version": "12.2",
  "timestamp": 1702854321.123
}
```

### Monitor GPU Metrics

```bash
# Prometheus metrics
curl http://localhost:8002/metrics | grep corvision_gpu

# Expected output:
# corvision_gpu_available{service="yolo-inference"} 1.0
# corvision_gpu_utilization_percent{gpu_id="0",service="yolo-inference"} 85.0
# corvision_gpu_memory_used_mb{gpu_id="0",service="yolo-inference"} 3456.0
# corvision_gpu_temperature_celsius{gpu_id="0",service="yolo-inference"} 65.0
# corvision_inference_speedup_factor{service="yolo-inference"} 12.5
```

### Watch nvidia-smi

```bash
watch -n 1 nvidia-smi
```

You should see GPU utilization spike during inference (70-100%).

---

## Configuration Options

### Environment Variables

| Variable | Values | Description | Default |
|----------|--------|-------------|---------|
| `DEVICE` | `auto`, `cuda`, `cpu` | Device selection | `auto` |
| `INPUT_SIZE` | `320`, `416`, `640` | Input resolution | `416` |
| `USE_TRACKING` | `true`, `false` | Enable object tracking | `false` |

### Recommended Settings

**For Maximum Speed (GPU):**
```yaml
DEVICE: "cuda"
INPUT_SIZE: "416"  # Good balance
USE_TRACKING: "false"
```

**For Maximum Quality (GPU):**
```yaml
DEVICE: "cuda"
INPUT_SIZE: "640"  # Higher resolution
USE_TRACKING: "true"  # Track objects across frames
```

**For CPU Fallback:**
```yaml
DEVICE: "auto"  # Auto-detect, fallback to CPU
```

---

## Grafana Dashboard

### Add GPU Panels

1. Open Grafana: http://localhost:3000
2. Create new dashboard or edit existing
3. Add these panels:

**GPU Utilization:**
```promql
corvision_gpu_utilization_percent{service="yolo-inference"}
```

**GPU Memory Usage:**
```promql
corvision_gpu_memory_used_mb{service="yolo-inference"}
```

**GPU Temperature:**
```promql
corvision_gpu_temperature_celsius{service="yolo-inference"}
```

**Inference Speedup:**
```promql
corvision_inference_speedup_factor{service="yolo-inference"}
```

**FPS Comparison:**
```promql
rate(corvision_frames_processed_total{service="yolo-inference"}[1m]) * 60
```

---

## Troubleshooting

### Problem: "CUDA not available" in logs

**Cause:** GPU not accessible to container

**Solutions:**
1. Verify NVIDIA drivers: `nvidia-smi`
2. Check NVIDIA Container Toolkit: `docker run --rm --gpus all nvidia/cuda:12.0.0-base-ubuntu22.04 nvidia-smi`
3. Ensure `deploy.resources` is uncommented in docker-compose.yml
4. Restart Docker: `sudo systemctl restart docker`

---

### Problem: "RuntimeError: CUDA out of memory"

**Cause:** GPU VRAM exhausted

**Solutions:**
1. Reduce `INPUT_SIZE`: 640 → 416 → 320
2. Use smaller model: YOLO11n (current) is already the smallest
3. Stop other GPU processes: `nvidia-smi` to identify
4. Check memory usage: `curl http://localhost:8002/metrics | grep gpu_memory`

---

### Problem: GPU detected but low utilization

**Cause:** Bottleneck elsewhere (Kafka, preprocessing, etc.)

**Solutions:**
1. Check preprocessing FPS: `curl http://localhost:8001/metrics | grep frames_per_second`
2. Enable CLAHE preprocessing to utilize GPU for enhancement
3. Increase Kafka partitions for parallel processing
4. Scale preprocessor: `docker compose up --scale kafka-preprocessor=2`

---

### Problem: Container fails to start with GPU enabled

**Cause:** Docker Compose version or syntax issue

**Solutions:**
1. Update Docker Compose: `sudo apt install docker-compose-plugin`
2. Use alternative syntax:
```yaml
yolo-inference:
  runtime: nvidia
  environment:
    NVIDIA_VISIBLE_DEVICES: all
```

3. Check Docker Compose version: `docker compose version` (need 1.28+)

---

### Problem: Multiple GPUs, wrong one selected

**Cause:** Default GPU selection

**Solutions:**
1. Specify GPU by ID:
```yaml
environment:
  DEVICE: "cuda:1"  # Use second GPU
```

2. Or use `CUDA_VISIBLE_DEVICES`:
```yaml
environment:
  CUDA_VISIBLE_DEVICES: "1"  # Only expose second GPU
```

3. Auto-select GPU with most free memory (already implemented):
```yaml
DEVICE: "auto"
```

---

## Multi-GPU Setup

### Using All GPUs

```yaml
yolo-inference:
  deploy:
    resources:
      reservations:
        devices:
          - driver: nvidia
            count: all  # Use all GPUs
            capabilities: [gpu]
```

### Scaling with Multiple GPUs

```bash
# Start multiple instances, each on different GPU
docker compose up --scale yolo-inference=2

# Or specify GPU per instance
docker compose run -e CUDA_VISIBLE_DEVICES=0 yolo-inference  # GPU 0
docker compose run -e CUDA_VISIBLE_DEVICES=1 yolo-inference  # GPU 1
```

---

## Performance Tuning

### Optimize for Throughput

```yaml
INPUT_SIZE: "416"       # Medium resolution
USE_TRACKING: "false"   # Faster without tracking
ENABLE_CLAHE: "false"   # Skip preprocessing overhead
```

**Expected:** 150-200+ FPS on RTX 3060

### Optimize for Quality

```yaml
INPUT_SIZE: "640"       # High resolution
USE_TRACKING: "true"    # Track objects
ENABLE_CLAHE: "true"    # Better lighting adaptation
```

**Expected:** 80-120 FPS on RTX 3060

### Mixed Precision (FP16)

Already enabled automatically when using CUDA:
```python
# In model_inference.py
model.predict(frame, half=True)  # FP16 on GPU, FP32 on CPU
```

---

## Cost-Benefit Analysis

### Cloud GPU Instances

| Provider | Instance | GPU | Cost/Hour | FPS | Cost/1M Frames |
|----------|----------|-----|-----------|-----|----------------|
| AWS | g4dn.xlarge | T4 | $0.526 | 100 | $1.46 |
| GCP | n1-standard-4 + T4 | T4 | $0.50 | 100 | $1.39 |
| Azure | NC4as_T4_v3 | T4 | $0.526 | 100 | $1.46 |
| Local CPU | - | i5 | $0 | 18 | $0 (slower) |

**Recommendation:** Use GPU for real-time applications (>30 FPS), CPU for batch processing.

---

## Testing GPU Setup

### Run Benchmark

```bash
# From host machine
docker compose exec yolo-inference python -c "
import torch
import time
from ultralytics import YOLO

# Check CUDA
print(f'CUDA Available: {torch.cuda.is_available()}')
if torch.cuda.is_available():
    print(f'GPU: {torch.cuda.get_device_name()}')

# Load model
model = YOLO('yolo11n.pt')
model.to('cuda' if torch.cuda.is_available() else 'cpu')

# Warm up
import numpy as np
dummy = np.zeros((640, 640, 3), dtype=np.uint8)
model.predict(dummy, verbose=False)

# Benchmark
start = time.time()
for i in range(100):
    model.predict(dummy, verbose=False)
elapsed = time.time() - start

print(f'Average inference time: {elapsed/100*1000:.2f}ms')
print(f'FPS: {100/elapsed:.1f}')
"
```

**Expected Output (GPU):**
```
CUDA Available: True
GPU: NVIDIA GeForce RTX 3060
Average inference time: 6.5ms
FPS: 153.8
```

**Expected Output (CPU):**
```
CUDA Available: False
Average inference time: 55.2ms
FPS: 18.1
```

---

## Next Steps

After GPU setup:
1. ✅ Monitor metrics in Grafana
2. ✅ Tune `INPUT_SIZE` for your use case
3. ✅ Consider scaling horizontally with multiple GPUs
4. ✅ Enable CLAHE preprocessing (GPU accelerated)
5. ✅ Test multi-camera support with GPU sharing

---

## Support Matrix

| GPU Series | CUDA Capability | Supported | Recommended |
|------------|----------------|-----------|-------------|
| GTX 1000 | 6.1 | ⚠️ Yes | For testing |
| RTX 2000 | 7.5 | ✅ Yes | Good |
| RTX 3000 | 8.6 | ✅ Yes | **Excellent** |
| RTX 4000 | 8.9 | ✅ Yes | **Best** |
| Tesla T4 | 7.5 | ✅ Yes | Cloud |
| A100 | 8.0 | ✅ Yes | Enterprise |

---

## References

- [NVIDIA Container Toolkit Documentation](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html)
- [Docker GPU Support](https://docs.docker.com/config/containers/resource_constraints/#gpu)
- [YOLO Ultralytics GPU Guide](https://docs.ultralytics.com/)
- [CUDA Compatibility Matrix](https://docs.nvidia.com/deploy/cuda-compatibility/)

---

## Summary

✅ **GPU acceleration provides 10-50x speedup**  
✅ **Automatic CPU fallback ensures reliability**  
✅ **Full metrics tracking for GPU utilization**  
✅ **Docker Compose makes deployment simple**  
✅ **Supports multi-GPU and cloud GPUs**

GPU acceleration transforms CVops from a demonstration project into a **production-ready, real-time vision system**.
