# 🚀 Panduan Menjalankan Proyek — ROSBD Crypto Signals (Single Laptop)

Panduan ini menjalankan **seluruh pipeline** di satu laptop menggunakan Docker Compose.

---

## Prasyarat

| Software | Versi Minimum | Cek |
|----------|--------------|-----|
| Docker | 24+ | `docker --version` |
| Docker Compose | 2.20+ | `docker compose version` |
| RAM Tersedia | 8 GB | - |
| Disk Tersedia | 20 GB | - |

---

## 1. Setup Awal (Satu Kali)

```bash
# 1. Clone/masuk ke direktori proyek
cd FUTURES_CRYPTO_SIGNALS

# 2. Salin dan isi file environment
cp .env .env.backup    # backup jika ada
# Edit .env jika perlu mengubah credentials
nano .env

# 3. Buat folder yang dibutuhkan
mkdir -p DATA MODEL logs
touch DATA/.gitkeep MODEL/.gitkeep
```

---

## 2. Jalankan Semua Service

```bash
# Jalankan semua service sekaligus (background)
docker compose up -d

# Pantau proses startup (tunggu semua "healthy")
docker compose ps
```

Tunggu hingga semua service berstatus **healthy** (±2-3 menit):

| Service | Status Target |
|---------|--------------|
| zookeeper | healthy |
| kafka | healthy |
| cassandra | healthy |
| spark-master | running |
| spark-worker | running |
| kafka-consumer | running |
| health-check | running |
| prometheus | running |
| grafana | running |
| airflow-webserver | healthy |
| airflow-scheduler | running |

---

## 3. Jalankan Batch Pipeline (Pertama Kali / Historis)

> Batch pipeline mengunduh 4 tahun data historis ke Cassandra.
> Diperlukan untuk training model pertama kali.

**Via Airflow UI:**
1. Buka http://localhost:8080
2. Login: `admin / admin`
3. Cari DAG **`crypto_batch_historical_load`**
4. Klik ▶️ **Trigger DAG** → tunggu selesai (~20-60 menit)

**Via Terminal:**
```bash
docker compose exec airflow-webserver airflow dags trigger crypto_batch_historical_load
```

---

## 4. Pipeline Stream Berjalan Otomatis

DAG **`crypto_price_pipeline`** berjalan otomatis setiap jam.
Alur: `Kafka Producer → DQ Check → Train XGBoost → Scan Sinyal`

Tidak perlu tindakan manual — Airflow Scheduler mengelolanya otomatis.

---

## 5. Jalankan Dashboard Streamlit

```bash
cd FRONTEND
streamlit run app.py --server.port 8501
```

Buka: http://localhost:8501

---

## 6. Akses Semua UI

| Service | URL | Login |
|---------|-----|-------|
| **Streamlit Dashboard** | http://localhost:8501 | - |
| **Airflow** | http://localhost:8080 | admin / admin |
| **Grafana** | http://localhost:3000 | admin / admin |
| **Spark Master** | http://localhost:8081 | - |
| **Prometheus** | http://localhost:9090 | - |
| **Health Check** | http://localhost:8000/health | - |
| **Pipeline Metrics** | http://localhost:8000/metrics | - |

---

## 7. Setup Grafana Alert ke Telegram (Notifikasi)

1. Buka Grafana → http://localhost:3000
2. Menu → **Alerting** → **Contact Points**
3. Klik **Add contact point**
4. Pilih type: **Telegram**
5. Isi:
   - **BOT API Token:** (dari `TELEGRAM_BOT_TOKEN` di `.env`)
   - **Chat ID:** (dari `TELEGRAM_CHAT_ID` di `.env`)
6. Klik **Test** untuk verifikasi
7. Buka panel yang ingin di-alert → tab **Alert** → assign ke contact point Telegram

---

## 8. Troubleshooting

### Cassandra lama startup
```bash
# Tunggu hingga healthy
docker compose logs cassandra --tail 20
```

### Kafka Consumer tidak bisa connect
```bash
docker compose restart kafka-consumer
```

### Airflow DAG tidak muncul
```bash
docker compose exec airflow-scheduler airflow dags list
docker compose restart airflow-scheduler
```

### Lihat logs pipeline
```bash
# Log realtime semua service
docker compose logs -f --tail 50

# Log spesifik
docker compose logs airflow-scheduler --tail 50
cat logs/pipeline.log
cat logs/errors.log
```

---

## 9. Hentikan Semua Service

```bash
# Stop sementara (data tersimpan)
docker compose stop

# Stop + hapus container (data volume tetap)
docker compose down

# Stop + hapus semua termasuk data (HATI-HATI!)
docker compose down -v
```
