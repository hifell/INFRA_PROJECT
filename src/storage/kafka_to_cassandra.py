"""
Kafka Consumer → Cassandra Writer (Streaming + 1-Hour Aggregation)

Script ini bertugas:
1. Membaca pesan 1-detik secara streaming dari topik Kafka 'crypto_signals'.
2. Mengagregasi data tersebut menjadi lilin (candle) 1-jam secara real-time.
3. Melakukan upsert (timpa data) ke Cassandra 'crypto_ks.signals' agar 
   ML dan Dasbor selalu memiliki agregat 1-jam paling terbaru.

Script ini dirancang untuk berjalan terus-menerus (long-running process).
"""

import os
import json
from datetime import datetime

from cassandra.cluster import Cluster
from kafka import KafkaConsumer


# Konfigurasi — single node (localhost)
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", 9042))
KAFKA_TOPIC = "crypto_signals"
CONSUMER_GROUP = "cassandra_writer_group"


def setup_cassandra():
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
    print("[*] KAFKA CONSUMER: STREAMING & 1-HOUR AGGREGATION")
    print("=" * 60)

    cluster, session = setup_cassandra()

    # Prepared statement untuk performa insert yang lebih tinggi
    insert_stmt = session.prepare("""
        INSERT INTO signals ("token", datetime, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)

    print(f"[*] Menghubungkan ke Kafka ({KAFKA_BOOTSTRAP_SERVERS})...")
    print(f"[*] Subscribing topik: '{KAFKA_TOPIC}', Consumer Group: '{CONSUMER_GROUP}'")
    
    # Hapus consumer_timeout_ms agar script berjalan tanpa batas waktu (True Stream)
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    print("[*] Consumer berjalan. Menunggu pesan real-time dari Kafka...\n")

    count = 0
    # State untuk melacak agregasi per token untuk jam saat ini
    aggregation_state = {}

    try:
        for message in consumer:
            data = message.value

            try:
                token = data["token"]
                dt = datetime.strptime(data["Datetime"], "%Y-%m-%d %H:%M:%S")
                # Normalisasi ke awal jam (misal 10:45:12 menjadi 10:00:00)
                hour_start = dt.replace(minute=0, second=0, microsecond=0)
                
                open_price = float(data["Open"])
                high_price = float(data["High"])
                low_price = float(data["Low"])
                close_price = float(data["Close"])
                volume = float(data["Volume"])

                # Inisialisasi state jika token baru atau jam berubah
                if token not in aggregation_state or aggregation_state[token]['hour_start'] != hour_start:
                    aggregation_state[token] = {
                        'hour_start': hour_start,
                        'open': open_price,
                        'high': high_price,
                        'low': low_price,
                        'close': close_price,
                        'volume': volume
                    }
                else:
                    # Update agregasi 1-jam dengan data detik terbaru
                    current_state = aggregation_state[token]
                    current_state['high'] = max(current_state['high'], high_price)
                    current_state['low'] = min(current_state['low'], low_price)
                    current_state['close'] = close_price  # Harga terakhir di jam tersebut
                    current_state['volume'] += volume

                # Lakukan UPSERT langsung ke Cassandra
                # Cassandra menggunakan Primary Key ("token", datetime), 
                # sehingga data pada jam yang sama akan ditimpa (di-update) dengan agregat terbaru.
                state = aggregation_state[token]
                session.execute(insert_stmt, (
                    token, state['hour_start'], state['open'], state['high'],
                    state['low'], state['close'], state['volume']
                ))
                
                count += 1
                if count % 1000 == 0:
                    print(f"    [+] {count} data 1-detik telah diserap & diagregasi ke Cassandra...")

            except KeyError as e:
                print(f"[!] Pesan memiliki field yang hilang: {e} — Data: {data}")
            except ValueError as e:
                print(f"[!] Gagal konversi tipe data: {e} — Data: {data}")
            except Exception as e:
                print(f"[!] Error saat insert ke Cassandra: {e}")

    except KeyboardInterrupt:
        print(f"\n[!] Consumer dihentikan secara manual. Total {count} pesan diproses.")
    finally:
        consumer.close()
        cluster.shutdown()
        print("[+] Koneksi Kafka dan Cassandra ditutup.")


if __name__ == "__main__":
    run_consumer()
