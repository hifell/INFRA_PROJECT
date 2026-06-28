# 🔐 SECURITY.md — Aspek Keamanan Pipeline ROSBD Crypto Signals

## Overview

Dokumen ini menjelaskan model keamanan yang diterapkan pada sistem pipeline Big Data
ROSBD Crypto Signals, mencakup autentikasi, otorisasi, dan perlindungan data.

---

## 1. Secret Management (Authentication Credentials)

### Prinsip: Zero Hardcoded Secrets

Semua credential **TIDAK PERNAH** ditulis langsung di dalam kode sumber. Semua rahasia
disimpan di file `.env` yang:
- Tidak pernah di-commit ke Git (tercatat di `.gitignore`)
- Dibaca saat runtime via `python-dotenv`

### Secrets yang Dikelola

| Secret | Env Variable | Deskripsi |
|--------|-------------|-----------|
| Database password | `DB_PASSWORD` | Password PostgreSQL/Supabase |
| Supabase API Key | `SUPABASE_KEY` | Kunci akses Supabase REST API |
| Telegram Bot Token | `TELEGRAM_BOT_TOKEN` | Token autentikasi bot Telegram |
| Grafana Admin Password | `GRAFANA_PASSWORD` | Password admin Grafana |

### Cara Setup

```bash
# Salin template env
cp .env.example .env

# Edit .env dengan credential asli
nano .env
```

---

## 2. Network Security (Authorization)

### Docker Network Isolation

Semua service berjalan dalam jaringan Docker internal. Port hanya di-expose
yang diperlukan untuk akses pengguna:

| Service | Port Publik | Port Internal | Akses |
|---------|-------------|---------------|-------|
| Airflow | 8080 | 8080 | Lokal saja |
| Grafana | 3000 | 3000 | Lokal saja |
| Spark Master UI | 8081 | 8080 | Lokal saja |
| Prometheus | 9090 | 9090 | Lokal saja |
| Kafka | 9092 | 29092 | 9092: Lokal, 29092: Internal Docker |
| Cassandra | 9042 | 9042 | Lokal saja |
| Health Check | 8000 | 8000 | Lokal saja |

> **Catatan:** Untuk deployment production, akses ke port 9090, 9042, dan 9092
> harus dibatasi dengan firewall (`ufw allow from 192.168.x.x` saja).

### Kafka Security

Saat ini menggunakan PLAINTEXT (tanpa enkripsi transport) untuk kemudahan development.
Untuk production, aktifkan Kafka SASL/SCRAM:

```yaml
# docker-compose.yml (production)
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT
KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL: SCRAM-SHA-256
```

---

## 3. Data Protection

### Data at Rest

- **Cassandra:** Data OHLCV disimpan di Docker volume `cassandra_data` (tidak encrypted).
  Untuk production, aktifkan Cassandra Transparent Data Encryption (TDE).
- **PostgreSQL:** Audit log dan sinyal tersimpan di Supabase dengan enkripsi at-rest bawaan.

### Data in Transit

- Komunikasi antar container menggunakan Docker internal network (encrypted at OS level).
- Akses Supabase menggunakan HTTPS/TLS.
- Telegram API menggunakan HTTPS.

### Data Minimization

- Pipeline hanya mengumpulkan data harga (OHLCV) yang diperlukan untuk analisis.
- Tidak ada data personal pengguna yang dikumpulkan.
- Data berita disimpan hanya header (judul, deskripsi, URL) — tidak menyimpan konten penuh.

---

## 4. Airflow Security

- Login: `admin / admin` (default, **wajib diubah** sebelum deployment)
- Untuk mengubah password:
  ```bash
  docker compose exec airflow-webserver airflow users reset-password --username admin
  ```
- Airflow menggunakan PostgreSQL terpisah untuk metadata (bukan database trading).

---

## 5. Grafana Security

- Login: `admin / ${GRAFANA_PASSWORD}` (dari `.env`)
- Sign-up pengguna baru dinonaktifkan (`GF_USERS_ALLOW_SIGN_UP=false`)
- Alerting terintegrasi dengan Telegram untuk notifikasi anomali

---

## 6. Audit Trail (Accountability)

Setiap aksi pipeline dicatat di tabel `pipeline_audit_log`:
- Siapa yang menjalankan (actor/modul)
- Apa yang dilakukan (event_type)
- Kapan (created_at dengan timezone)
- Status (SUCCESS/FAILURE)
- Data apa yang terdampak (rows_affected, token)

Audit log dapat dilihat di tab **Audit Trail** pada Streamlit dashboard
atau di Grafana panel (via PostgreSQL datasource).

---

## 7. Checklist Keamanan Sebelum Deployment

- [ ] `.env` tidak di-commit ke Git
- [ ] Password Airflow admin diubah dari default
- [ ] Password Grafana admin diubah dari default (`GRAFANA_PASSWORD` di `.env`)
- [ ] Port Docker tidak di-expose ke internet publik (gunakan firewall)
- [ ] Telegram Bot Token disimpan di `.env`, bukan di kode
- [ ] Cassandra diakses hanya dari dalam jaringan Docker
