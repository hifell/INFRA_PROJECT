"""
DAG: Pipeline Harga Crypto (Stream + ML)
Jadwal: Setiap 1 jam

Flow:
  1. Kafka Stream Check → Verifikasi Kafka Consumer daemon aktif & data mengalir ke Cassandra
  2. Data Quality       → Validasi kualitas data sebelum training
  3. Train Model        → Latih ulang XGBoost via Spark (local[*] mode, single-laptop)
  4. Scan Signals       → Inferensi XGBoost → simpan ke PostgreSQL + alert Grafana

Note: Binance WS Producer dan Kafka Consumer berjalan sebagai service daemon terpisah
      (always-on stream processing). DAG ini hanya memvalidasi dan memproses hasilnya.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "rob-sbd",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="crypto_price_pipeline",
    default_args=default_args,
    description="Stream pipeline: Kafka ingestion → Data Quality → XGBoost training → Signal scanning",
    schedule_interval="0 * * * *",
    start_date=datetime(2026, 6, 23),
    catchup=False,
    tags=["crypto", "pipeline", "production", "stream"],
) as dag:

    # ──────────────────────────────────────────────────────────────────────────
    # TASK 1: Kafka Stream Ingestion Check
    # Memverifikasi bahwa Kafka Consumer daemon sedang aktif dan data OHLCV
    # sudah berhasil mengalir masuk ke Cassandra sebelum pipeline dilanjutkan.
    # ──────────────────────────────────────────────────────────────────────────
    kafka_stream_check_task = BashOperator(
        task_id="kafka_stream_check",
        bash_command="""
            export CASSANDRA_HOST=cassandra
            export CASSANDRA_PORT=9042
            export KAFKA_BOOTSTRAP_SERVERS=kafka:29092
            cd /opt/airflow && python -m src.checks.kafka_stream_check
        """,
        execution_timeout=timedelta(minutes=5),
    )

    # ──────────────────────────────────────────────────────────────────────────
    # TASK 3: Data Quality Check
    # Memvalidasi data di Cassandra sebelum digunakan untuk training
    # ──────────────────────────────────────────────────────────────────────────
    data_quality_task = BashOperator(
        task_id="data_quality_check",
        bash_command="""
            export CASSANDRA_HOST=cassandra
            export CASSANDRA_PORT=9042
            cd /opt/airflow && python -c "
import sys
sys.path.insert(0, '.')
from cassandra.cluster import Cluster
from src.governance.data_quality import check_dataframe_quality
from src.models.train_model import load_token_from_cassandra
from src.utils.logger import get_logger
import pandas as pd

logger = get_logger('airflow.dq_check')
TOKENS = ['BTC', 'ETH', 'SOL', 'XRP', 'BNB']
cluster = Cluster(['cassandra'], port=9042)
session = cluster.connect('crypto_ks')
all_pass = True
for token in TOKENS:
    df = load_token_from_cassandra(session, token)
    if df.empty:
        logger.warning(f'[{token}] Tidak ada data di Cassandra.')
        continue
    df_clean, report = check_dataframe_quality(df, token)
    if not report['passed']:
        logger.error(f'[{token}] DQ GAGAL: {report[\"issues\"]}')
        all_pass = False
cluster.shutdown()
sys.exit(0 if all_pass else 1)
"
        """,
    )

    # ──────────────────────────────────────────────────────────────────────────
    # TASK 4: Training XGBoost via Spark (single-laptop, local mode)
    # Menghapus logika file trigger — langsung jalankan di container Airflow
    # ──────────────────────────────────────────────────────────────────────────
    train_model_task = BashOperator(
        task_id="train_model",
        bash_command="""
            export CASSANDRA_HOST=cassandra
            export CASSANDRA_PORT=9042
            export SPARK_MASTER_URL=local[*]
            cd /opt/airflow && python -m src.models.train_model
        """,
        execution_timeout=timedelta(minutes=45),
    )

    # ──────────────────────────────────────────────────────────────────────────
    # TASK 5: Scan sinyal trading
    # Inferensi model → simpan ke PostgreSQL → kirim notifikasi via Grafana
    # ──────────────────────────────────────────────────────────────────────────
    scan_signals_task = BashOperator(
        task_id="scan_signals",
        bash_command="""
            export CASSANDRA_HOST=cassandra
            export CASSANDRA_PORT=9042
            cd /opt/airflow && python -m src.signals.generator
        """,
        execution_timeout=timedelta(minutes=15),
    )

    # ──────────────────────────────────────────────────────────────────────────
    # Dependency chain
    # ──────────────────────────────────────────────────────────────────────────
    kafka_stream_check_task >> data_quality_task >> train_model_task >> scan_signals_task
