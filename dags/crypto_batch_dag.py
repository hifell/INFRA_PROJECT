"""
DAG: Batch Historical Load
Jadwal: Sekali sehari (jam 02:00 UTC)

Fungsi BATCH PROCESSING eksplisit:
  1. Download data historis 4 tahun (bulk) dari Yahoo Finance untuk semua token
  2. Validasi kualitas data batch (schema, harga, outlier)
  3. Migrasi data CSV ke Cassandra via batch insert
  4. Catat audit trail ke database

Perbedaan dengan stream pipeline:
  - Stream  → data real-time, setiap jam, incremental update
  - Batch   → data historis besar, sekali sehari, full refresh jika diperlukan
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "rob-sbd",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crypto_batch_historical_load",
    default_args=default_args,
    description="BATCH: Download 4-tahun data historis → DQ Check → Bulk insert ke Cassandra → Audit log",
    schedule_interval="0 2 * * *",   # Setiap hari jam 02:00 UTC
    start_date=datetime(2026, 6, 23),
    catchup=False,
    tags=["crypto", "batch", "historical", "production"],
) as dag:

    # ──────────────────────────────────────────────────────────────────────────
    # TASK 1: Download data historis batch (4 tahun, semua token)
    # Berbeda dengan stream yang hanya ambil 7 hari terakhir
    # ──────────────────────────────────────────────────────────────────────────
    batch_download_task = BashOperator(
        task_id="batch_download_historical",
        bash_command="""
            cd /opt/airflow && python -c "
import sys
sys.path.insert(0, '.')
from src.ingestion.yahoo_loader import YahooDataLoader
from src.utils.logger import get_logger
from src.governance.audit_trail import AuditContext
import os, pandas as pd
from pathlib import Path

logger = get_logger('batch.download')
loader = YahooDataLoader()
TOKENS = ['BTC', 'ETH', 'SOL', 'XRP', 'BNB']
DATA_DIR = Path('/opt/airflow/DATA')
DATA_DIR.mkdir(exist_ok=True)

logger.info('=== BATCH DOWNLOAD HISTORIS DIMULAI ===')
for token in TOKENS:
    with AuditContext('INGEST', 'batch_download', token=token) as ctx:
        logger.info(f'[{token}] Mengunduh data historis 4 tahun...')
        df = loader.fetch_historical_data(token, period='4y', interval='1h')
        if df is not None and not df.empty:
            out_path = DATA_DIR / f'{token.lower()}_historical.csv'
            df.to_csv(out_path, index=False)
            ctx.rows_affected = len(df)
            logger.info(f'[{token}] {len(df)} baris disimpan ke {out_path}')
        else:
            logger.warning(f'[{token}] Tidak ada data yang diunduh.')
logger.info('=== BATCH DOWNLOAD SELESAI ===')
"
        """,
        execution_timeout=timedelta(minutes=30),
    )

    # ──────────────────────────────────────────────────────────────────────────
    # TASK 2: Data Quality Check pada data batch
    # Validasi semua file CSV yang baru didownload
    # ──────────────────────────────────────────────────────────────────────────
    batch_dq_check_task = BashOperator(
        task_id="batch_data_quality_check",
        bash_command="""
            cd /opt/airflow && python -c "
import sys
sys.path.insert(0, '.')
import pandas as pd
from pathlib import Path
from src.governance.data_quality import check_dataframe_quality
from src.governance.audit_trail import record_event
from src.utils.logger import get_logger

logger = get_logger('batch.dq')
DATA_DIR = Path('/opt/airflow/DATA')
TOKENS = ['BTC', 'ETH', 'SOL', 'XRP', 'BNB']
failed = []

for token in TOKENS:
    csv_path = DATA_DIR / f'{token.lower()}_historical.csv'
    if not csv_path.exists():
        logger.warning(f'[{token}] File CSV tidak ditemukan, skip DQ check.')
        continue

    df = pd.read_csv(csv_path)
    df_clean, report = check_dataframe_quality(df, token)

    # Simpan kembali data yang sudah dibersihkan
    df_clean.to_csv(csv_path, index=False)

    if not report['passed']:
        failed.append(token)
        record_event('DATA_QUALITY', 'batch_dq_check', status='FAILURE', token=token, details=report)
        logger.error(f'[{token}] DQ GAGAL! Issues: {report[\"issues\"]}')
    else:
        record_event('DATA_QUALITY', 'batch_dq_check', status='SUCCESS', token=token, details=report)
        logger.info(f'[{token}] DQ LULUS: {report[\"quality_pct\"]}% data valid.')

if failed:
    logger.error(f'Token berikut GAGAL DQ check: {failed}')
    sys.exit(1)
"
        """,
    )

    # ──────────────────────────────────────────────────────────────────────────
    # TASK 3: Migrasi CSV → Cassandra (batch insert)
    # Menggunakan prepared statement untuk insert massal yang efisien
    # ──────────────────────────────────────────────────────────────────────────
    batch_migrate_task = BashOperator(
        task_id="batch_migrate_to_cassandra",
        bash_command="""
            export CASSANDRA_HOST=cassandra
            export CASSANDRA_PORT=9042
            cd /opt/airflow && python -m scripts.migrate_csv_to_cassandra
        """,
        execution_timeout=timedelta(minutes=60),
    )

    # ──────────────────────────────────────────────────────────────────────────
    # TASK 4: Audit Summary — Rekap hasil batch ke audit log
    # ──────────────────────────────────────────────────────────────────────────
    batch_audit_task = BashOperator(
        task_id="batch_audit_summary",
        bash_command="""
            cd /opt/airflow && python -c "
import sys
sys.path.insert(0, '.')
from cassandra.cluster import Cluster
from src.governance.audit_trail import record_event
from src.utils.logger import get_logger

logger = get_logger('batch.audit')
TOKENS = ['BTC', 'ETH', 'SOL', 'XRP', 'BNB']

cluster = Cluster(['cassandra'], port=9042)
session = cluster.connect('crypto_ks')

total_rows = 0
for token in TOKENS:
    try:
        row = session.execute(
            f'SELECT COUNT(*) AS cnt FROM signals WHERE \"token\" = \\\'{token}\\\''
        ).one()
        count = row.cnt if row else 0
        total_rows += count
        logger.info(f'[{token}] Total baris di Cassandra: {count:,}')
    except Exception as e:
        logger.error(f'[{token}] Gagal query count: {e}')

cluster.shutdown()

record_event(
    'BATCH_COMPLETE',
    actor='batch_historical_load',
    status='SUCCESS',
    rows_affected=total_rows,
    details={'tokens': TOKENS, 'total_rows': total_rows}
)
logger.info(f'=== BATCH PIPELINE SELESAI. Total {total_rows:,} baris di Cassandra. ===')
"
        """,
    )

    # ──────────────────────────────────────────────────────────────────────────
    # Dependency chain
    # ──────────────────────────────────────────────────────────────────────────
    batch_download_task >> batch_dq_check_task >> batch_migrate_task >> batch_audit_task
