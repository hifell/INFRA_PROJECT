import os
import time
import datetime
import subprocess
import sys
import yfinance as yf
import pandas as pd
from src.signals.generator import TradingSignalCenter

def start_price_pipeline():
    print("\n" + "="*60)
    print("[*] BACKEND PIPELINE: FULL EXPERIMENT (KAFKA + CASSANDRA + RETRAIN + SCANNER)")
    print("="*60)
    
    scanner = TradingSignalCenter()
    tokens = ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "BNB-USD"]
    
    # Menjaga memori judul berita yang sudah dikirim ke Telegram agar tidak duplikat
    sent_news_titles = set()
    
    try:
        while True:
            print(f"\n[*] Siklus eksperimen penuh dimulai pada: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # -----------------------------------------------------------------
            # STEP 1: Ambil data harga live menggunakan Kafka Producer
            # -----------------------------------------------------------------
            print("\n[STEP 1] Menyinkronkan data live ke Kafka & Cassandra...")
            try:
                # Memanggil skrip binance_ws_producer.py secara independen
                subprocess.run(
                    [sys.executable, "-m", "src.ingestion.binance_ws_producer"],
                    check=True
                )
                print("   [+] Sinkronisasi Kafka Producer selesai.")
                # Memberi waktu sejenak agar Consumer sempat memproses & insert ke Cassandra
                print("   [*] Menunggu 5 detik agar Consumer selesai menulis ke Cassandra...")
                time.sleep(5)
            except Exception as e:
                print(f"   [!] Gagal menjalankan Kafka Producer: {str(e)}")
            
            # -----------------------------------------------------------------
            # STEP 1B: Memeriksa dan mengirimkan berita pasar kripto dari DATABASE ASLI
            # -----------------------------------------------------------------
            print("\n[STEP 1B] Memeriksa Berita Pasar Kripto Terbaru dari Database...")
            try:
                # Mengambil berita paling baru dari tabel v_market_news via DB connector
                news_data = scanner.db.get_latest_market_news(limit=1)
                
                if news_data:
                    # Jalankan pengecekan memori pencegah duplikasi otomatis
                    if news_data["title"] not in sent_news_titles:
                        scanner.send_news_notification(
                            title=news_data["title"],
                            source=news_data["source"],
                            url=news_data["url"],
                            sentiment=news_data["sentiment"]
                        )
                        # Masukkan judul ke dalam cache memori agar tidak di-spam di siklus berikutnya
                        sent_news_titles.add(news_data["title"])
                        print(f"   [+] Berita riil baru dari DB berhasil dikirim ke grup Telegram.")
                    else:
                        print(f"   [-] Berita asli di DB tidak berubah. Lewati untuk mencegah spam.")
                else:
                    print("   [-] Tabel berita di database kosong atau tidak ditemukan data.")
                        
            except Exception as e:
                print(f"   [!] Gagal memproses atau mengirim data berita asli: {str(e)}")
            
            # -----------------------------------------------------------------
            # STEP 2: JALANKAN PROSES TRAINING ULANG MODEL (EKSPERIMEN BARU)
            # -----------------------------------------------------------------
            print("\n[STEP 2] Menjalankan Re-Training Model XGBoost (Membaca ulang puluhan ribu baris)...")
            try:
                subprocess.run(
                    [sys.executable, "-m", "src.models.train_model"],
                    check=True
                )
                print("[+] Proses Re-Training ke-5 Model XGBoost selesai.")
            except Exception as e:
                print(f"[!] Gagal melakukan training ulang model: {str(e)}")
            
            # -----------------------------------------------------------------
            # STEP 3: Panggil model yang baru dilatih untuk scan sinyal dan tembak ke DB & Telegram
            # -----------------------------------------------------------------
            print("\n[STEP 3] Mengeksekusi Scanner untuk mengevaluasi sinyal baru ke Database...")
            try:
                scanner.scan_market_for_signals()
                print("[+] Sinyal hasil model baru berhasil disimpan ke database.")
            except Exception as e:
                print(f"[!] Terjadi gangguan pada saat scanner berjalan: {str(e)}")
            
            print("\n[*] Siklus penuh selesai. Menunggu Jeda 30 detik...")
            time.sleep(30)
            
    except KeyboardInterrupt:
        print("\n[!] Pipeline eksperimen dihentikan secara manual.")

if __name__ == "__main__":
    start_price_pipeline()