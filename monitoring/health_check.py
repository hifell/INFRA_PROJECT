"""
monitoring/health_check.py
HTTP Health Check endpoint + Prometheus metrics exporter.

Berjalan sebagai service mandiri (port 8000).
Grafana meng-scrape metrics dari endpoint /metrics.

Metrics yang di-expose:
- pipeline_kafka_messages_total: Total pesan Kafka yang diproses
- pipeline_cassandra_rows_total: Total baris di Cassandra per token
- pipeline_model_train_duration_seconds: Durasi training terakhir
- pipeline_last_ingest_timestamp: Unix timestamp ingest terakhir
- pipeline_data_quality_score: Skor kualitas data per token (0-100)
- pipeline_signal_count_total: Total sinyal LONG yang terdeteksi
"""

import os
import time
import threading
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler

from cassandra.cluster import Cluster

try:
    from prometheus_client import (
        Counter, Gauge, Histogram, Summary,
        generate_latest, CONTENT_TYPE_LATEST,
        CollectorRegistry, REGISTRY
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    print("[WARNING] prometheus_client tidak terinstall. Metrics tidak akan tersedia.")

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.utils.logger import get_logger

logger = get_logger("health_check")

# ─── Prometheus Metrics ───────────────────────────────────────────────────────
if PROMETHEUS_AVAILABLE:
    kafka_messages_total = Counter(
        "pipeline_kafka_messages_total",
        "Total pesan yang diproses dari Kafka",
        ["token", "status"]
    )
    cassandra_rows = Gauge(
        "pipeline_cassandra_rows_total",
        "Total baris data di Cassandra per token",
        ["token"]
    )
    model_train_duration = Gauge(
        "pipeline_model_train_duration_seconds",
        "Durasi training model terakhir per token",
        ["token"]
    )
    last_ingest_ts = Gauge(
        "pipeline_last_ingest_timestamp",
        "Unix timestamp ingest data terakhir per token",
        ["token"]
    )
    data_quality_score = Gauge(
        "pipeline_data_quality_score",
        "Skor kualitas data (0-100) per token",
        ["token"]
    )
    signal_count = Counter(
        "pipeline_signal_count_total",
        "Total sinyal trading yang terdeteksi",
        ["token", "signal_type"]
    )
    pipeline_errors = Counter(
        "pipeline_errors_total",
        "Total error yang terjadi per komponen",
        ["component", "error_type"]
    )
    grafana_alert_sent = Counter(
        "pipeline_alert_sent_total",
        "Total alert yang dikirim ke Grafana/Telegram",
        ["channel", "token"]
    )


# ─── Service Status Check ─────────────────────────────────────────────────────
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", 9042))
TOKENS = ["BTC", "ETH", "SOL", "XRP", "BNB"]


def check_cassandra_health() -> dict:
    """Cek koneksi Cassandra dan hitung baris per token."""
    result = {"status": "unhealthy", "rows": {}}
    try:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT,
                          connect_timeout=5)
        session = cluster.connect("crypto_ks")
        for token in TOKENS:
            try:
                row = session.execute(
                    f'SELECT COUNT(*) AS cnt FROM signals WHERE "token" = \'{token}\''
                ).one()
                count = row.cnt if row else 0
                result["rows"][token] = count
                if PROMETHEUS_AVAILABLE:
                    cassandra_rows.labels(token=token).set(count)
            except Exception:
                result["rows"][token] = -1
        cluster.shutdown()
        result["status"] = "healthy"
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"[HealthCheck] Cassandra unhealthy: {e}")
    return result


def get_system_status() -> dict:
    """Mengumpulkan status keseluruhan sistem."""
    cassandra = check_cassandra_health()
    return {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "services": {
            "cassandra": cassandra,
        },
        "pipeline": "operational" if cassandra["status"] == "healthy" else "degraded"
    }


# ─── HTTP Handler ─────────────────────────────────────────────────────────────
class HealthHandler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        # Suppress default access log (kita pakai logger sendiri)
        pass

    def _send(self, code: int, content_type: str, body: bytes):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            import json
            status = get_system_status()
            code = 200 if status["pipeline"] == "operational" else 503
            self._send(code, "application/json",
                       json.dumps(status, indent=2).encode())

        elif self.path == "/metrics" and PROMETHEUS_AVAILABLE:
            # Refresh Cassandra metrics sebelum serve
            check_cassandra_health()
            self._send(200, CONTENT_TYPE_LATEST, generate_latest())

        elif self.path == "/ready":
            self._send(200, "text/plain", b"OK")

        else:
            self._send(404, "text/plain", b"Not Found")


# ─── Background metric updater ────────────────────────────────────────────────
def _metric_updater_loop(interval_seconds: int = 60):
    """Secara periodik memperbarui metrics Prometheus."""
    while True:
        try:
            check_cassandra_health()
            logger.debug("[HealthCheck] Metrics diperbarui.")
        except Exception as e:
            logger.error(f"[HealthCheck] Gagal update metrics: {e}")
        time.sleep(interval_seconds)


def start_health_server(port: int = 8000):
    """Menjalankan HTTP server health check + metrics."""
    updater = threading.Thread(
        target=_metric_updater_loop, args=(60,), daemon=True
    )
    updater.start()

    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    logger.info(f"[HealthCheck] Server berjalan di http://0.0.0.0:{port}")
    logger.info(f"[HealthCheck]   GET /health  → Status JSON")
    logger.info(f"[HealthCheck]   GET /metrics → Prometheus metrics")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("[HealthCheck] Server dihentikan.")
        server.shutdown()


if __name__ == "__main__":
    start_health_server(port=int(os.environ.get("HEALTH_PORT", 8000)))
