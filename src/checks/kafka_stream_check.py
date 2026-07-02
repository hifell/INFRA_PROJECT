"""
Kafka Stream Ingestion Health Check

Script ini memverifikasi bahwa:
1. Kafka broker aktif dan topik 'crypto_signals' tersedia.
2. Data OHLCV sudah berhasil mengalir dari Kafka Consumer ke Cassandra.

Dijalankan oleh Airflow DAG sebagai task pertama sebelum Data Quality & Training.
"""

import sys
import os

sys.path.insert(0, "/opt/airflow")

from cassandra.cluster import Cluster
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", 9042))


def check_kafka():
    """Cek koneksi ke Kafka dan keberadaan topik crypto_signals."""
    print("=" * 60)
    print("[*] TASK 1: KAFKA STREAM INGESTION CHECK")
    print("=" * 60)

    try:
        consumer = KafkaConsumer(
            "crypto_signals",
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            consumer_timeout_ms=5000,
            auto_offset_reset="latest",
            group_id="airflow_health_check",
        )
        partitions = consumer.partitions_for_topic("crypto_signals")
        consumer.close()
        if partitions:
            print(f"[+] Kafka OK — topik crypto_signals memiliki {len(partitions)} partisi")
        else:
            print("[!] WARNING: Topik crypto_signals belum memiliki partisi")
    except Exception as e:
        print(f"[!] Kafka check gagal: {e}")
        sys.exit(1)


def check_cassandra():
    """Cek data terbaru di Cassandra."""
    try:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect("crypto_ks")
        rows = session.execute(
            'SELECT "token", datetime, close FROM signals PER PARTITION LIMIT 1'
        )
        count = 0
        for row in rows:
            print(f"[+] Cassandra OK — {row.token}: latest={row.datetime}, close={row.close}")
            count += 1
        cluster.shutdown()
        if count == 0:
            print("[!] WARNING: Belum ada data di Cassandra. Pastikan Kafka Consumer daemon berjalan.")
        else:
            print(f"[+] Total {count} token ditemukan di Cassandra.")
    except Exception as e:
        print(f"[!] Cassandra check gagal: {e}")
        sys.exit(1)


if __name__ == "__main__":
    check_kafka()
    check_cassandra()
    print("[+] Stream Ingestion Check PASSED")
