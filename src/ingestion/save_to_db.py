# src/ingestion/save_to_db.py
import os
import psycopg2
from datetime import datetime
# Mengimpor modul analisis sentimen mandiri dari folder models yang sudah dibuat
from src.models.sentiment_model import CryptoSentimentAnalyzer
from config.settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

class TradingDatabaseConnector:
    def __init__(self):
        # Konfigurasi kredensial PostgreSQL Supabase
        self.host = DB_HOST
        self.port = DB_PORT
        self.database = DB_NAME
        self.user = DB_USER
        self.password = DB_PASSWORD

        # Inisialisasi objek analyzer dari file terpisah agar kode tetap modular
        self.analyzer = CryptoSentimentAnalyzer()

        # Pastikan kolom cluster_label ada di PostgreSQL
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("ALTER TABLE v_crypto_signals ADD COLUMN IF NOT EXISTS cluster_label INTEGER;")
            conn.commit()
            cursor.close()
            conn.close()
            print("[DB SUCCESS] Kolom 'cluster_label' terverifikasi/ditambahkan ke PostgreSQL.")
        except Exception as e:
            print(f"[DB WARNING] Gagal memeriksa/menambahkan kolom cluster_label di PostgreSQL: {e}")

    def _get_connection(self):
        import socket
        # Supabase mengembalikan alamat IPv6 yang tidak bisa dijangkau dari dalam Docker.
        # Kita paksa resolusi ke IPv4 dengan mengambil AF_INET secara eksplisit.
        try:
            ipv4 = next(
                info[4][0]
                for info in socket.getaddrinfo(self.host, self.port, socket.AF_INET, socket.SOCK_STREAM)
            )
        except StopIteration:
            ipv4 = self.host  # fallback ke hostname jika IPv4 tidak ditemukan

        return psycopg2.connect(
            host=ipv4,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            sslmode="require",
            connect_timeout=10,
        )


    def insert_crypto_signal(self, token: str, price: float, probability: float, status: str, tp: float = None, sl: float = None, cluster_label: int = None):
        """
        Memasukkan data harga berjalan dan simulasi status sinyal ke tabel v_crypto_signals
        """
        query = """
            INSERT INTO v_crypto_signals (token, price, probability, signal_status, take_profit, stop_loss, cluster_label)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(query, (token, price, probability, status, tp, sl, cluster_label))
            conn.commit()
            cursor.close()
            conn.close()
            print(f"[DB SUCCESS] Berhasil menyimpan harga terbaru {token}: ${price:.2f} (Cluster: {cluster_label})")
        except Exception as e:
            print(f"[DB ERROR] Gagal menyimpan data harga/sinyal: {str(e)}")

    def insert_market_news(self, source_name: str, title: str, description: str, url: str, published_at: str):
        """
        Memasukkan data teks berita global sekaligus label sentimennya ke tabel v_market_news
        """
        # Cek duplikasi judul berita agar database tidak penuh dengan berita yang sama
        check_query = "SELECT id FROM v_market_news WHERE title = %s;"
        
        # PERUBAHAN: Menambahkan kolom sentiment ke dalam target insert kueri SQL
        insert_query = """
            INSERT INTO v_market_news (source_name, title, description, url, published_at, sentiment)
            VALUES (%s, %s, %s, %s, %s, %s);
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Eksekusi cek duplikasi
            cursor.execute(check_query, (title,))
            if cursor.fetchone() is not None:
                cursor.close()
                conn.close()
                return # Skip jika berita sudah pernah dimasukkan sebelumnya
                
            # Konversi string ISO waktu NewsAPI ke format timestamp Python demi validitas data
            try:
                clean_date = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ")
            except:
                clean_date = datetime.now()

            # PERUBAHAN: Memanggil fungsi analisis sentimen dari objek model luar secara otomatis sebelum disimpan
            sentiment_label = self.analyzer.calculate_sentiment_label(title, description)

            # PERUBAHAN: Menyertakan sentiment_label ke dalam tuple eksekusi kueri
            cursor.execute(insert_query, (source_name, title, description, url, clean_date, sentiment_label))
            conn.commit()
            cursor.close()
            conn.close()
            print(f"[DB SUCCESS] Berhasil menyimpan berita baru [{sentiment_label}]: {title[:40]}...")
        except Exception as e:
            print(f"[DB ERROR] Gagal menyimpan data berita: {str(e)}")

    def get_latest_market_news(self, limit: int = 1):
        """
        Mengambil berita kripto paling baru dari tabel v_market_news beserta kolom sentimen aslinya
        """
        # PERUBAHAN: Menarik kolom sentiment asli dari database untuk diteruskan ke bot Telegram
        query = """
            SELECT source_name, title, description, url, sentiment 
            FROM v_market_news 
            ORDER BY id DESC LIMIT %s;
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(query, (limit,))
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if row:
                return {
                    "source": row[0],
                    "title": row[1],
                    "description": row[2],
                    "url": row[3],
                    "sentiment": row[4] if row[4] else "NEUTRAL" # Menggunakan data kolom riil database
                }
            return None
        except Exception as e:
            print(f"[DB ERROR] Gagal mengambil berita terbaru: {str(e)}")
            return None

# =========================================================================
# JALUR TESTING KONEKSI LOKAL
# =========================================================================
if __name__ == "__main__":
    print("[*] Menguji coba koneksi dan fungsionalitas insert PostgreSQL dengan Sentimen Otomatis...")
    db = TradingDatabaseConnector()
    
    # Uji coba input data harga tiruan
    db.insert_crypto_signal(token="BTC", price=62190.50, probability=55.4, status="LONG", tp=64055.0, sl=61443.0)
    
    # Uji coba input data berita tiruan (Sistem akan otomatis memberikan label lewat model eksternal)
    db.insert_market_news(
        source_name="Test Source", 
        title="Uji Coba Koneksi Sistem Database Terdistribusi ROSBD", 
        description="Deskripsi uji coba koneksi lokal.", 
        url="https://localhost", 
        published_at="2026-06-07T12:00:00Z"
    )