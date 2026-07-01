"""
DAG: Pipeline Berita Crypto
Jadwal: Setiap 5 menit (*/5 * * * *)

Mengorkestrasi pipeline berita pasar:
1. Fetch News — Ambil berita makro kripto terbaru dari NewsAPI
2. Save & Notify — Simpan ke database dan kirim notifikasi Telegram
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

def send_telegram_failure_alert(context):
    from airflow.models import Variable
    import requests
    
    try:
        bot_token = Variable.get("TELEGRAM_BOT_TOKEN")
        chat_id = Variable.get("TELEGRAM_CHAT_ID")
    except Exception as e:
        print(f"[!] Gagal mengambil variabel Telegram dari Airflow: {e}")
        return

    dag_id = context.get('task_instance').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    exception = context.get('exception')
    log_url = context.get('task_instance').log_url

    message = (
        f"🚨 <b>AIRFLOW TASK FAILURE ALERT</b> 🚨\n\n"
        f"<b>DAG:</b> {dag_id}\n"
        f"<b>Task:</b> {task_id}\n"
        f"<b>Execution Date:</b> {execution_date}\n"
        f"<b>Error:</b> {exception}\n"
        f"<b>Log URL:</b> <a href='{log_url}'>View Log</a>"
    )

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML"
    }

    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            print("[+] Notifikasi kegagalan tugas berhasil dikirim ke Telegram.")
        else:
            print(f"[!] Gagal mengirim Telegram alert: {response.text}")
    except Exception as e:
        print(f"[!] Exception saat mengirim Telegram alert: {e}")

# ============================================================
# Default arguments untuk semua task dalam DAG ini
# ============================================================
default_args = {
    "owner": "rob-sbd",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": send_telegram_failure_alert,
}

# ============================================================
# Definisi DAG
# ============================================================
with DAG(
    dag_id="crypto_news_pipeline",
    default_args=default_args,
    description="Pipeline berita: Fetch dari NewsAPI → Simpan ke DB → Kirim ke Telegram",
    schedule_interval="*/5 * * * *",  # Setiap 5 menit
    start_date=datetime(2026, 6, 23),
    catchup=False,
    tags=["crypto", "news", "production"],
) as dag:

    # ----------------------------------------------------------
    # TASK 1: Jalankan News Pipeline (1 siklus)
    # Mengambil 10 berita terbaru dari NewsAPI, menyimpan ke
    # tabel v_market_news di PostgreSQL (Supabase), dan mengirim
    # notifikasi ke Telegram untuk berita baru.
    # ----------------------------------------------------------
    fetch_and_send_news = BashOperator(
        task_id="fetch_and_send_news",
        bash_command="cd /opt/airflow && python -m src.ingestion.run_news_pipeline --once",
        execution_timeout=timedelta(minutes=5),
    )
