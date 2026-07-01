"""
Kafka Consumer → Cassandra Writer.

Script ini bertugas:
1. Membaca pesan secara streaming dari topik Kafka 'crypto_signals'.
2. Menyimpan setiap record OHLCV ke tabel Cassandra 'crypto_ks.signals'.
3. Cassandra melakukan upsert otomatis berdasarkan Primary Key (token, Datetime),
   sehingga tidak perlu pengecekan duplikat manual.

Script ini dirancang untuk berjalan terus-menerus (long-running process).
"""

import os
import json
from datetime import datetime

from cassandra.cluster import Cluster
from kafka import KafkaConsumer
from prometheus_client import start_http_server, Counter
from src.monitoring.metrics import cassandra_rows_total



# Konfigurasi — single node (localhost)
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", 9042))
KAFKA_TOPIC = "crypto_signals"
CONSUMER_GROUP = "cassandra_writer_group"


def setup_cassandra():
    """
    Menghubungkan ke Cassandra dan memastikan Keyspace serta Tabel sudah ada.
    """
    print(f"[*] Menghubungkan ke Cassandra ({CASSANDRA_HOST}:{CASSANDRA_PORT})...")
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect()

    # Buat keyspace jika belum ada
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS crypto_ks
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)

    session.set_keyspace("crypto_ks")

    # Buat tabel jika belum ada
    session.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            "token" text,
            datetime timestamp,
            open double,
            high double,
            low double,
            close double,
            volume double,
            PRIMARY KEY ("token", datetime)
        )
    """)
    print("[+] Cassandra siap.")
    return cluster, session


def run_consumer():
    print("\n" + "=" * 60)
    print("[*] KAFKA CONSUMER: STREAMING DATA KE CASSANDRA")
    print("=" * 60)

    cluster, session = setup_cassandra()

    # Prepared statement untuk performa insert yang lebih tinggi
    insert_stmt = session.prepare("""
        INSERT INTO signals ("token", datetime, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)

    print(f"[*] Menghubungkan ke Kafka ({KAFKA_BOOTSTRAP_SERVERS})...")
    print(f"[*] Subscribing topik: '{KAFKA_TOPIC}', Consumer Group: '{CONSUMER_GROUP}'")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    print("[*] Consumer berjalan. Menunggu pesan dari Kafka...\n")

    count = 0
    try:
        while True:
            try:
                for message in consumer:
                    data = message.value

                    try:
                        token = data["token"]
                        dt = datetime.strptime(data["Datetime"], "%Y-%m-%d %H:%M:%S")
                        open_price = float(data["Open"])
                        high_price = float(data["High"])
                        low_price = float(data["Low"])
                        close_price = float(data["Close"])
                        volume = float(data["Volume"])

                        session.execute(insert_stmt, (
                            token, dt, open_price, high_price, low_price, close_price, volume
                        ))
                        
                        cassandra_rows_total.labels(token=token).inc()
                        count += 1

                        if count % 1000 == 0:
                            print(f"    [+] {count} pesan berhasil disimpan ke Cassandra...")

                    except KeyError as e:
                        print(f"[!] Pesan memiliki field yang hilang: {e} — Data: {data}")
                    except ValueError as e:
                        print(f"[!] Gagal konversi tipe data: {e} — Data: {data}")
                    except Exception as e:
                        print(f"[!] Error saat insert ke Cassandra: {e}")
            except Exception as stream_err:
                print(f"[!] Terjadi gangguan pada stream Kafka: {stream_err}. Mencoba kembali dalam 5 detik...")
                import time
                time.sleep(5)

    except KeyboardInterrupt:
        print(f"\n[!] Consumer dihentikan secara manual. Total {count} pesan telah disimpan.")
    finally:
        try:
            consumer.close()
        except:
            pass
        try:
            cluster.shutdown()
        except:
            pass
        print("[+] Koneksi Kafka dan Cassandra ditutup.")


if __name__ == "__main__":
    start_http_server(8000)
    run_consumer()
