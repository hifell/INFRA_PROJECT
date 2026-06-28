"""
src/governance/audit_trail.py
Modul Audit Trail untuk pipeline crypto.

Mencatat setiap aksi pipeline ke:
1. File audit.log (via logger)
2. Tabel PostgreSQL `pipeline_audit_log` (persistent, queryable)

Setiap event dicatat dengan:
- event_type: INGEST | TRAIN | SCAN | ALERT | ERROR | DATA_QUALITY
- actor: nama modul/service yang melakukan aksi
- token: simbol crypto (jika relevan)
- details: JSON berisi metadata tambahan
- status: SUCCESS | FAILURE | WARNING
"""

import os
import json
import psycopg2
from datetime import datetime
from typing import Optional, Any

from src.utils.logger import get_logger, log_pipeline_event

logger = get_logger(__name__)

# ─── Koneksi DB ───────────────────────────────────────────────────────────────
def _get_db_connection():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "localhost"),
        port=int(os.environ.get("DB_PORT", 5432)),
        database=os.environ.get("DB_NAME", "postgres"),
        user=os.environ.get("DB_USER", "postgres"),
        password=os.environ.get("DB_PASSWORD", ""),
    )


def ensure_audit_table():
    """
    Membuat tabel audit_log di PostgreSQL jika belum ada.
    Dipanggil saat startup pipeline.
    """
    create_sql = """
        CREATE TABLE IF NOT EXISTS pipeline_audit_log (
            id            SERIAL PRIMARY KEY,
            event_type    VARCHAR(50)  NOT NULL,
            actor         VARCHAR(100) NOT NULL,
            token         VARCHAR(20),
            status        VARCHAR(20)  NOT NULL DEFAULT 'SUCCESS',
            details       JSONB,
            rows_affected INTEGER,
            duration_ms   INTEGER,
            created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_audit_event_type ON pipeline_audit_log(event_type);
        CREATE INDEX IF NOT EXISTS idx_audit_token      ON pipeline_audit_log(token);
        CREATE INDEX IF NOT EXISTS idx_audit_created_at ON pipeline_audit_log(created_at DESC);
    """
    try:
        conn = _get_db_connection()
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()
        conn.close()
        logger.info("[AuditTrail] Tabel pipeline_audit_log siap.")
    except Exception as e:
        logger.error(f"[AuditTrail] Gagal membuat tabel audit: {e}")


def record_event(
    event_type: str,
    actor: str,
    status: str = "SUCCESS",
    token: Optional[str] = None,
    details: Optional[dict] = None,
    rows_affected: Optional[int] = None,
    duration_ms: Optional[int] = None,
):
    """
    Menyimpan satu event audit ke database dan file log.

    Args:
        event_type: Kategori event (INGEST, TRAIN, SCAN, ALERT, ERROR, DATA_QUALITY)
        actor: Nama modul/service (mis: "kafka_producer", "train_model")
        status: "SUCCESS" | "FAILURE" | "WARNING"
        token: Simbol crypto (opsional)
        details: Dict metadata tambahan (opsional)
        rows_affected: Jumlah baris yang diproses (opsional)
        duration_ms: Durasi proses dalam milidetik (opsional)

    Usage:
        from src.governance.audit_trail import record_event
        record_event("INGEST", "kafka_producer", token="BTC", rows_affected=150)
    """
    # 1. Catat ke file log
    log_pipeline_event(event_type, {
        "actor": actor,
        "token": token or "-",
        "status": status,
        "rows": rows_affected or 0,
        "duration_ms": duration_ms or 0,
    })

    # 2. Catat ke PostgreSQL
    insert_sql = """
        INSERT INTO pipeline_audit_log
            (event_type, actor, token, status, details, rows_affected, duration_ms)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    try:
        conn = _get_db_connection()
        with conn.cursor() as cur:
            cur.execute(insert_sql, (
                event_type,
                actor,
                token,
                status,
                json.dumps(details) if details else None,
                rows_affected,
                duration_ms,
            ))
        conn.commit()
        conn.close()
    except Exception as e:
        # Jangan sampai audit failure menghentikan pipeline utama
        logger.warning(f"[AuditTrail] Gagal simpan event ke DB (tidak kritikal): {e}")


class AuditContext:
    """
    Context manager untuk mencatat durasi dan status sebuah operasi pipeline.

    Usage:
        from src.governance.audit_trail import AuditContext
        with AuditContext("TRAIN", "train_model", token="BTC") as ctx:
            ctx.rows_affected = 5000
            # ... lakukan training ...
        # Event otomatis dicatat saat keluar dari blok with
    """

    def __init__(self, event_type: str, actor: str, token: str = None, details: dict = None):
        self.event_type = event_type
        self.actor = actor
        self.token = token
        self.details = details or {}
        self.rows_affected = None
        self._start = None

    def __enter__(self):
        self._start = datetime.utcnow()
        logger.info(f"[{self.actor}] Memulai {self.event_type} untuk token={self.token or 'ALL'}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration_ms = int((datetime.utcnow() - self._start).total_seconds() * 1000)
        status = "FAILURE" if exc_type else "SUCCESS"

        if exc_val:
            self.details["error"] = str(exc_val)
            logger.error(
                f"[{self.actor}] {self.event_type} GAGAL setelah {duration_ms}ms: {exc_val}",
                exc_info=True
            )

        record_event(
            event_type=self.event_type,
            actor=self.actor,
            status=status,
            token=self.token,
            details=self.details,
            rows_affected=self.rows_affected,
            duration_ms=duration_ms,
        )
        return False  # Biarkan exception terus propagate
