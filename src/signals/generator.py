import os
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from src.features.feature_engineering import CryptoFeatureEngineer
from src.models.predict_linear import CryptoInferenceEngine
from src.ingestion.save_to_db import TradingDatabaseConnector
from src.utils.logger import get_logger
from src.governance.audit_trail import AuditContext, record_event

# Load environment variables dari .env
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

logger = get_logger(__name__)


class TradingSignalCenter:
    def __init__(self, model_dir: str = "MODEL"):
        # Path direktori data lokal proyek ini, relatif ke root workspace
        self.data_dir = str(Path(__file__).resolve().parents[2] / "DATA")
        os.makedirs(self.data_dir, exist_ok=True)
        self.engineer = CryptoFeatureEngineer(data_dir=self.data_dir)
        self.inference = CryptoInferenceEngine(model_dir=model_dir)
        self.db = TradingDatabaseConnector()

        # Kredensial Bot Telegram — dibaca dari .env (TIDAK hardcoded)
        self.telegram_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        self.telegram_chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not self.telegram_token:
            logger.warning("[TradingSignalCenter] TELEGRAM_BOT_TOKEN tidak diset di .env. Alert Telegram dinonaktifkan.")

        # Threshold probabilitas minimal hasil prediksi model XGBoost untuk memicu sinyal
        self.confidence_thresholds = {"BTC": 0.51, "BNB": 0.51, "ETH": 0.52, "XRP": 0.52, "SOL": 0.53}
        
        # Parameter manajemen risiko kelompok (Take Profit dan Stop Loss)
        self.risk_parameters = {
            "BTC": {"TP": 0.030, "SL": 0.012},
            "BNB": {"TP": 0.030, "SL": 0.012},
            "ETH": {"TP": 0.025, "SL": 0.008},
            "SOL": {"TP": 0.050, "SL": 0.020},
            "XRP": {"TP": 0.050, "SL": 0.020}
        }

    def send_telegram_notification(self, token: str, price: float, prob: float, tp: float, sl: float, time_str: str):
        """Mengirimkan alert sinyal trading hasil prediksi model ML ke grup Telegram.
        Hanya berjalan jika TELEGRAM_BOT_TOKEN diset di .env.
        """
        if not self.telegram_token:
            logger.warning(f"[ALERT] Telegram token tidak tersedia. Sinyal {token} tidak dikirim.")
            return

        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"

        message = (
            f"🔔 *ROB-SBD TRADING SIGNAL DETECTED* 🔔\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 *Token:* {token}/USDT\n"
            f"📈 *Signal:* EXECUTE LONG\n"
            f"💵 *Entry Price:* ${price:.4f}\n"
            f"🎯 *Take Profit (TP):* ${tp:.4f} (+{self.risk_parameters[token]['TP']*100:.1f}%)\n"
            f"🛑 *Stop Loss (SL):* ${sl:.4f} (-{self.risk_parameters[token]['SL']*100:.1f}%)\n"
            f"📊 *AI Probability:* {prob*100:.2f}%\n"
            f"⏰ *Timestamp:* {time_str}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🤖 *Sent automatically by XGBoost Inference Engine*"
        )

        payload = {
            "chat_id": self.telegram_chat_id,
            "text": message,
            "parse_mode": "Markdown"
        }
        
        try:
            response = requests.post(url, json=payload)
            if response.status_code == 200:
                print(f"   [TELEGRAM SUCCESS] Notifikasi sinyal {token} berhasil terkirim ke grup.")
            else:
                print(f"   [TELEGRAM ERROR] Gagal mengirim sinyal ke grup: {response.text}")
        except Exception as e:
            print(f"   [TELEGRAM ERROR] Gangguan koneksi API Telegram pada modul sinyal: {str(e)}")

    def send_news_notification(self, title: str, source: str, url: str, sentiment: str = "NEUTRAL"):
        """Mengirimkan ringkasan berita fundamental pasar kripto ke grup Telegram menggunakan mode HTML"""
        telegram_url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        
        emoji_map = {"BULLISH": "🚀🟢", "BEARISH": "🚨🔴", "NEUTRAL": "📰🔵"}
        emoji = emoji_map.get(sentiment.upper(), "📰🔵")
        
        # Menggunakan tag HTML (<b> dan <i>) menggantikan Markdown agar tidak mudah error karena tanda baca berita
        message = (
            f"{emoji} <b>CRYPTO NEWS UPDATE</b> {emoji}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📰 <b>Judul:</b> {title}\n\n"
            f"🔍 <b>Sumber:</b> {source}\n"
            f"🔗 <b>Baca Selengkapnya:</b> <a href='{url}'>Klik Di Sini</a>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🤖 <i>Sent automatically by ROB-SBD News Ingestion System</i>"
        )
        
        payload = {
            "chat_id": self.telegram_chat_id,
            "text": message,
            "parse_mode": "HTML",  # Diubah ke HTML agar aman dari error parse character
            "disable_web_page_preview": False
        }
        
        try:
            res = requests.post(telegram_url, json=payload)
            if res.status_code == 200:
                print(f"   [TELEGRAM NEWS] Berhasil meneruskan berita ke grup Telegram.")
            else:
                print(f"   [TELEGRAM NEWS ERROR] Gagal meneruskan berita: {res.text}")
        except Exception as e:
            print(f"   [TELEGRAM NEWS ERROR] Gangguan koneksi API Telegram pada modul berita: {str(e)}")

    def scan_market_for_signals(self):
        """Membaca data dari Cassandra, mengekstrak fitur, memprediksi arah, dan memperbarui DB"""
        tokens = ["BTC", "ETH", "SOL", "XRP", "BNB"]
        
        # Setup koneksi sementara ke Cassandra untuk scan
        import os
        from cassandra.cluster import Cluster
        cass_host = os.environ.get("CASSANDRA_HOST", "127.0.0.1")
        cass_port = int(os.environ.get("CASSANDRA_PORT", 9042))
        cluster = Cluster([cass_host], port=cass_port)
        session = cluster.connect("crypto_ks")
        
        # Kirim session ke engineer
        self.engineer.cassandra_session = session

        # Check if cluster_label column exists in keyspace metadata
        has_cluster_label = False
        try:
            ks_meta = cluster.metadata.keyspaces.get("crypto_ks")
            if ks_meta:
                table_meta = ks_meta.tables.get("signals")
                if table_meta and "cluster_label" in table_meta.columns:
                    has_cluster_label = True
        except Exception:
            pass

        print("\n" + "="*50)
        print(f"[*] MENJALANKAN SCANNER SINYAL TRADING ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
        print(f"[*] Metadata 'cluster_label' terdeteksi di Cassandra: {has_cluster_label}")
        print("="*50)

        for token in tokens:
            # Mengambil 100 baris terakhir (cukup untuk fitur EMA 50 dan BB)
            if has_cluster_label:
                query = f'SELECT datetime, open, high, low, close, volume, cluster_label FROM signals WHERE "token" = \'{token}\' ORDER BY datetime DESC LIMIT 100'
            else:
                query = f'SELECT datetime, open, high, low, close, volume FROM signals WHERE "token" = \'{token}\' ORDER BY datetime DESC LIMIT 100'
                
            try:
                rows = session.execute(query)
                if has_cluster_label:
                    data = [{"Datetime": row.datetime, "Open": row.open, "High": row.high, "Low": row.low, "Close": row.close, "Volume": row.volume, "cluster_label": getattr(row, "cluster_label", None)} for row in rows]
                else:
                    data = [{"Datetime": row.datetime, "Open": row.open, "High": row.high, "Low": row.low, "Close": row.close, "Volume": row.volume} for row in rows]
                df_raw = pd.DataFrame(data)
                if not df_raw.empty:
                    df_raw["Datetime"] = pd.to_datetime(df_raw["Datetime"])
                    df_raw = df_raw.sort_values("Datetime").reset_index(drop=True)
            except Exception as e:
                print(f"[!] Gagal membaca data {token} dari Cassandra: {e}")
                continue
                
            if df_raw.empty:
                print(f"[!] Data historis untuk {token} tidak ditemukan di Cassandra.")
                continue
            df_features = self.engineer.build_features(df_raw, token, is_training=False)
            
            if df_features.empty:
                continue
                
            latest_row = df_features.iloc[-1]
            current_price = float(latest_row["Close"])
            current_time = latest_row["Datetime"]
            
            cluster_val = None
            if "cluster_label" in latest_row and not pd.isna(latest_row["cluster_label"]):
                cluster_val = int(latest_row["cluster_label"])
            
            prob = float(self.inference.predict_next_probability(df_features, token))
            threshold = self.confidence_thresholds.get(token, 0.52)
            
            print(f"[{token}] Price: ${current_price:<10} | Prob: {prob*100:.2f}% | Threshold: {threshold*100:.1f}%")
            
            risk = self.risk_parameters[token]
            
            if prob >= threshold:
                status = "LONG"
                tp_price = current_price * (1 + risk["TP"])
                sl_price = current_price * (1 - risk["SL"])
                
                print(f"   [!!!] SIGNAL DETECTED - EXECUTE LONG {token} [!!!]")
                
                # Kirim sinyal otomatis ke grup Telegram
                self.send_telegram_notification(
                    token=token,
                    price=current_price,
                    prob=prob,
                    tp=tp_price,
                    sl=sl_price,
                    time_str=str(current_time)
                )
            else:
                status = "Wait & See"
                tp_price = None
                sl_price = None
            
            try:
                self.db.insert_crypto_signal(
                    token=token,
                    price=current_price,
                    probability=round(prob * 100, 2),
                    status=status,
                    tp=tp_price,
                    sl=sl_price,
                    cluster_label=cluster_val
                )
            except Exception as e:
                print(f"[!] Gagal menyimpan entri sinyal {token} ke database PostgreSQL: {str(e)}")

def run_signal_scanner():
    center = TradingSignalCenter()
    center.scan_market_for_signals()

if __name__ == "__main__":
    run_signal_scanner()