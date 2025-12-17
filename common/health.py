"""
Common health check utilities for CVops services.
Provides standardized HTTP endpoints for health, readiness, and metrics.
"""

import time
import json
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST


class HealthCheckHandler(BaseHTTPRequestHandler):
    """
    HTTP handler for health and readiness endpoints.
    
    Usage:
        health_state = {
            'kafka_connected': False,
            'dependencies_ready': True
        }
        handler = create_health_handler('my-service', health_state)
        server = HTTPServer(('0.0.0.0', 8000), handler)
        server.serve_forever()
    """
    
    service_name = 'unknown'
    health_state = {}
    
    def log_message(self, format, *args):
        """Suppress default HTTP logging."""
        pass
    
    def do_GET(self):
        if self.path == '/health':
            self._handle_health()
        elif self.path == '/ready':
            self._handle_ready()
        elif self.path == '/metrics':
            self._handle_metrics()
        else:
            self.send_response(404)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'Not Found. Available: /health, /ready, /metrics')
    
    def _handle_health(self):
        """
        Health check: Is the service running?
        Always returns 200 if the HTTP server is responding.
        """
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        response = {
            'status': 'healthy',
            'service': self.service_name,
            'timestamp': time.time()
        }
        self.wfile.write(json.dumps(response).encode())
    
    def _handle_ready(self):
        """
        Readiness check: Can the service handle requests?
        Returns 200 if ready, 503 if not ready.
        """
        # Check if all dependencies are ready
        is_ready = all(self.health_state.values())
        status_code = 200 if is_ready else 503
        
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        response = {
            'status': 'ready' if is_ready else 'not_ready',
            'service': self.service_name,
            'checks': dict(self.health_state),
            'timestamp': time.time()
        }
        self.wfile.write(json.dumps(response).encode())
    
    def _handle_metrics(self):
        """Prometheus metrics endpoint."""
        self.send_response(200)
        self.send_header('Content-type', CONTENT_TYPE_LATEST)
        self.end_headers()
        self.wfile.write(generate_latest())


def create_health_handler(service_name, health_state):
    """
    Factory function to create a health handler with specific service name and state.
    
    Args:
        service_name: Name of the service for identification
        health_state: Dictionary with boolean values indicating component health
                     Example: {'kafka_connected': True, 'model_loaded': False}
    
    Returns:
        Handler class configured with service name and health state
    """
    class ServiceHealthHandler(HealthCheckHandler):
        pass
    
    ServiceHealthHandler.service_name = service_name
    ServiceHealthHandler.health_state = health_state
    
    return ServiceHealthHandler


def start_health_server(port, service_name, health_state):
    """
    Start HTTP server with health, readiness, and metrics endpoints.
    Blocks until server is stopped.
    
    Args:
        port: Port to listen on
        service_name: Name of the service
        health_state: Dictionary with component health status
    """
    handler = create_health_handler(service_name, health_state)
    server = HTTPServer(('0.0.0.0', port), handler)
    logging.info(f"HTTP server started on port {port} (/health, /ready, /metrics)")
    server.serve_forever()
