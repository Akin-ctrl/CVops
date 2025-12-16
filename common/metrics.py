"""
Shared metrics utilities for CoRVision services.
Provides Prometheus metrics and HTTP server for scraping.
"""
from prometheus_client import Counter, Gauge, Histogram, start_http_server
import logging
import time
from functools import wraps

logger = logging.getLogger(__name__)

# Common metrics across services
frames_processed = Counter(
    'corvision_frames_processed_total',
    'Total number of frames processed',
    ['service']
)

processing_latency = Gauge(
    'corvision_processing_latency_ms',
    'Processing latency in milliseconds',
    ['service']
)

processing_duration = Histogram(
    'corvision_processing_duration_seconds',
    'Time spent processing frames',
    ['service'],
    buckets=[0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0]
)

kafka_messages_consumed = Counter(
    'corvision_kafka_messages_consumed_total',
    'Total Kafka messages consumed',
    ['service', 'topic']
)

kafka_messages_produced = Counter(
    'corvision_kafka_messages_produced_total',
    'Total Kafka messages produced',
    ['service', 'topic']
)

kafka_consumer_lag = Gauge(
    'corvision_kafka_consumer_lag',
    'Kafka consumer lag',
    ['service', 'topic', 'partition']
)

errors_total = Counter(
    'corvision_errors_total',
    'Total errors by type',
    ['service', 'error_type']
)

service_up = Gauge(
    'corvision_service_up',
    'Service health status (1=up, 0=down)',
    ['service']
)

# YOLO-specific metrics
detections_total = Counter(
    'corvision_detections_total',
    'Total object detections',
    ['class_name']
)

detection_confidence = Histogram(
    'corvision_detection_confidence',
    'Detection confidence scores',
    ['class_name'],
    buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
)

inference_fps = Gauge(
    'corvision_inference_fps',
    'Inference frames per second',
    ['service']
)

# MinIO-specific metrics
minio_batches_written = Counter(
    'corvision_minio_batches_written_total',
    'Total batches written to MinIO',
    ['service']
)

minio_records_written = Counter(
    'corvision_minio_records_written_total',
    'Total records written to MinIO',
    ['service']
)

minio_write_duration = Histogram(
    'corvision_minio_write_duration_seconds',
    'MinIO write duration',
    ['service'],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0]
)


def start_metrics_server(port: int, service_name: str):
    """Start Prometheus metrics HTTP server."""
    try:
        start_http_server(port)
        service_up.labels(service=service_name).set(1)
        logger.info(f"Metrics server started on port {port} for service '{service_name}'")
    except Exception as e:
        logger.error(f"Failed to start metrics server: {e}")
        service_up.labels(service=service_name).set(0)


def track_processing_time(service_name: str):
    """Decorator to track processing time."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                processing_duration.labels(service=service_name).observe(duration)
                processing_latency.labels(service=service_name).set(duration * 1000)  # Convert to ms
                return result
            except Exception as e:
                errors_total.labels(service=service_name, error_type=type(e).__name__).inc()
                raise
        return wrapper
    return decorator


class MetricsContext:
    """Context manager for tracking metrics during processing."""
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        processing_duration.labels(service=self.service_name).observe(duration)
        processing_latency.labels(service=self.service_name).set(duration * 1000)
        
        if exc_type is not None:
            errors_total.labels(service=self.service_name, error_type=exc_type.__name__).inc()
        
        return False  # Don't suppress exceptions
