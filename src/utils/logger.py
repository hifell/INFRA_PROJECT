"""
src/utils/logger.py
Centralized structured logging untuk seluruh pipeline.

Fitur:
- File handler dengan rotasi otomatis (10MB per file, 5 backup)
- Console handler dengan format berwarna
- Log level dapat dikonfigurasi via env var LOG_LEVEL
- Setiap modul mendapatkan named logger-nya sendiri
"""

import os
import logging
import logging.handlers
from pathlib import Path
from datetime import datetime

# ─── Konfigurasi ──────────────────────────────────────────────────────────────
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "pipeline.log"
AUDIT_LOG_FILE = LOG_DIR / "audit.log"
ERROR_LOG_FILE = LOG_DIR / "errors.log"

# ─── Format ───────────────────────────────────────────────────────────────────
DETAILED_FORMAT = "[%(asctime)s] [%(levelname)-8s] [%(name)s] %(message)s"
SIMPLE_FORMAT   = "[%(asctime)s] [%(levelname)s] %(message)s"
DATE_FORMAT     = "%Y-%m-%d %H:%M:%S"

# ─── Warna untuk console (ANSI) ───────────────────────────────────────────────
COLORS = {
    "DEBUG":    "\033[36m",   # Cyan
    "INFO":     "\033[32m",   # Green
    "WARNING":  "\033[33m",   # Yellow
    "ERROR":    "\033[31m",   # Red
    "CRITICAL": "\033[35m",   # Magenta
    "RESET":    "\033[0m",
}


class ColoredFormatter(logging.Formatter):
    """Formatter yang menambahkan warna ANSI ke output console."""

    def format(self, record):
        color = COLORS.get(record.levelname, COLORS["RESET"])
        reset = COLORS["RESET"]
        record.levelname = f"{color}{record.levelname}{reset}"
        return super().format(record)


def get_logger(name: str, level: str = None) -> logging.Logger:
    """
    Mendapatkan logger bernama untuk modul tertentu.

    Args:
        name: Nama logger, biasanya __name__ dari modul pemanggil.
        level: Override log level (opsional).

    Returns:
        Configured Logger instance.

    Usage:
        from src.utils.logger import get_logger
        logger = get_logger(__name__)
        logger.info("Pipeline dimulai")
        logger.error("Koneksi gagal", exc_info=True)
    """
    logger = logging.getLogger(name)

    # Hindari penambahan handler duplikat
    if logger.handlers:
        return logger

    effective_level = getattr(logging, level or LOG_LEVEL, logging.INFO)
    logger.setLevel(effective_level)

    # ── Handler 1: File utama (semua level) ──
    file_handler = logging.handlers.RotatingFileHandler(
        LOG_FILE,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding="utf-8"
    )
    file_handler.setLevel(effective_level)
    file_handler.setFormatter(logging.Formatter(DETAILED_FORMAT, datefmt=DATE_FORMAT))

    # ── Handler 2: File error saja ──
    error_handler = logging.handlers.RotatingFileHandler(
        ERROR_LOG_FILE,
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=3,
        encoding="utf-8"
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(logging.Formatter(DETAILED_FORMAT, datefmt=DATE_FORMAT))

    # ── Handler 3: Console ──
    console_handler = logging.StreamHandler()
    console_handler.setLevel(effective_level)
    console_handler.setFormatter(
        ColoredFormatter(DETAILED_FORMAT, datefmt=DATE_FORMAT)
    )

    logger.addHandler(file_handler)
    logger.addHandler(error_handler)
    logger.addHandler(console_handler)
    logger.propagate = False

    return logger


def get_audit_logger() -> logging.Logger:
    """
    Logger khusus untuk audit trail.
    Setiap aksi ingestion, training, dan scan dicatat di sini.
    """
    audit_logger = logging.getLogger("AUDIT")

    if audit_logger.handlers:
        return audit_logger

    audit_logger.setLevel(logging.INFO)

    handler = logging.handlers.RotatingFileHandler(
        AUDIT_LOG_FILE,
        maxBytes=10 * 1024 * 1024,
        backupCount=10,
        encoding="utf-8"
    )
    handler.setFormatter(logging.Formatter(DETAILED_FORMAT, datefmt=DATE_FORMAT))
    audit_logger.addHandler(handler)
    audit_logger.propagate = False

    return audit_logger


def log_pipeline_event(event_type: str, details: dict):
    """
    Mencatat event pipeline ke audit log dalam format terstruktur.

    Args:
        event_type: Jenis event (INGEST, TRAIN, SCAN, ALERT, ERROR)
        details: Dict berisi metadata event

    Usage:
        log_pipeline_event("INGEST", {"token": "BTC", "rows": 150, "source": "kafka"})
    """
    audit = get_audit_logger()
    detail_str = " | ".join(f"{k}={v}" for k, v in details.items())
    audit.info(f"EVENT={event_type} | {detail_str}")
