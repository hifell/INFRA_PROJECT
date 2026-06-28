# BAB I
# PENDAHULUAN

---

## 1.1 Latar Belakang

Pasar aset kripto (*cryptocurrency*) merupakan salah satu pasar keuangan yang paling dinamis dan volatil di dunia. Dalam kurun waktu satu dekade terakhir, kapitalisasi pasar kripto global telah berkembang secara eksponensial, mencapai lebih dari 2 triliun dolar Amerika Serikat pada tahun 2024, dengan volume perdagangan harian yang melampaui ratusan miliar dolar. Karakteristik pasar ini yang beroperasi 24 jam sehari, 7 hari seminggu, lintas batas negara, dan sangat rentan terhadap sentimen global menjadikannya tantangan tersendiri bagi para pelaku pasar dalam mengambil keputusan investasi yang tepat dan cepat.

Di sisi lain, perkembangan teknologi *Big Data* dan *Machine Learning* telah membuka peluang baru dalam analisis pasar keuangan secara kuantitatif. Teknik-teknik seperti *gradient boosting*, *deep learning*, dan *natural language processing* (NLP) kini memungkinkan pemrosesan data dalam skala besar untuk mengekstrak pola-pola tersembunyi yang tidak dapat diidentifikasi secara manual. Namun, penerapan teknologi ini dalam lingkungan produksi nyata (*production-grade*) membutuhkan lebih dari sekadar model yang akurat — ia membutuhkan sebuah **infrastruktur rekayasa data** yang mampu menangani aliran data berkecepatan tinggi, menyimpan data historis dalam jumlah masif, dan mengeksekusi pipeline analisis secara terjadwal dan otomatis.

Sistem *trading* berbasis sinyal algoritmik (*algorithmic signal trading*) telah lama menjadi domain eksklusif institusi keuangan besar yang memiliki sumber daya komputasi dan finansial yang memadai. Perdagangan aset kripto berjangka (*futures*) khususnya, memerlukan analisis multi-dimensi yang mencakup data teknikal harga, volume perdagangan, dan konteks makroekonomi global secara bersamaan dalam jendela waktu yang sangat sempit.

Kebutuhan akan sistem yang mampu mengintegrasikan keseluruhan alur tersebut — mulai dari *ingestion* data real-time, penyimpanan terdistribusi, pelatihan model *machine learning* secara berkala, hingga penyampaian sinyal kepada pengguna akhir — mendorong pengembangan sistem **ROSBD (*Real-time Orchestrated Signal & Big Data*) Crypto Signals**. Sistem ini dirancang sebagai platform terdistribusi yang memanfaatkan ekosistem teknologi *Big Data* modern, yaitu Apache Kafka untuk *streaming* data, Apache Cassandra sebagai penyimpanan deret waktu (*time-series*) terdistribusi, Apache Spark untuk komputasi terdistribusi dan pelatihan model, serta Apache Airflow sebagai orkestrator *pipeline* yang menjamin keandalan dan ketepatan jadwal eksekusi.

Model prediktif yang digunakan adalah **XGBoost** (*Extreme Gradient Boosting*) yang diintegrasikan dengan antarmuka terdistribusi PySpark (`SparkXGBClassifier`), memungkinkan pelatihan ulang model secara otomatis setiap jam menggunakan data historis terbaru. Sistem ini juga dilengkapi dengan modul analisis sentimen berita berbasis **VADER** (*Valence Aware Dictionary and sEntiment Reasoner*) yang diperkaya dengan kosakata khusus industri kripto untuk memberikan konteks fundamental pada sinyal teknikal yang dihasilkan.

---

## 1.2 Rumusan Masalah

Berdasarkan latar belakang yang telah diuraikan, rumusan masalah dalam penelitian dan pengembangan sistem ini adalah sebagai berikut:

1. **Bagaimana merancang arsitektur *end-to-end* sistem *Big Data* yang mampu menangani ingesti, penyimpanan, dan pemrosesan data harga kripto secara *real-time* dengan keandalan tinggi?**

2. **Bagaimana mengimplementasikan pipeline orkestrasi data terdistribusi menggunakan Apache Kafka, Apache Cassandra, Apache Spark, dan Apache Airflow dalam satu ekosistem yang terintegrasi untuk mendukung pelatihan model *machine learning* secara berkala?**

3. **Bagaimana membangun model prediktif berbasis XGBoost yang mampu mendeteksi potensi lonjakan volatilitas (*volatility breakout*) pada aset kripto berjangka (BTC, ETH, SOL, XRP, BNB) dalam jangka waktu 3 jam ke depan dengan mempertimbangkan ketidakseimbangan kelas (*class imbalance*) pada data?**

4. **Bagaimana mengintegrasikan analisis sentimen teks berita makroekonomi global sebagai konteks fundamental ke dalam sistem sinyal trading secara otomatis?**

5. **Bagaimana menyajikan hasil prediksi sinyal trading dan informasi pasar secara real-time dalam bentuk dashboard interaktif dan notifikasi otomatis yang dapat diakses oleh pengguna akhir?**

---

## 1.3 Batasan Masalah

Untuk menjaga fokus penelitian dan pengembangan agar tidak meluas di luar cakupan yang dapat dikelola, ditetapkan batasan-batasan masalah sebagai berikut:

1. **Cakupan Aset:** Sistem hanya mencakup lima aset kripto utama, yaitu **Bitcoin (BTC), Ethereum (ETH), Solana (SOL), XRP, dan Binance Coin (BNB)**. Aset kripto lain di luar daftar ini tidak dianalisis.

2. **Sumber Data Harga:** Data harga OHLCV (*Open, High, Low, Close, Volume*) diperoleh eksklusif dari **Yahoo Finance API** dengan interval waktu **1 jam** (*1h candlestick*). Data tick-by-tick atau data dari *exchange* langsung tidak digunakan.

3. **Sumber Data Berita:** Data berita makroekonomi diperoleh eksklusif dari **NewsAPI** dengan pembatasan kuota sesuai paket layanan yang digunakan. Cakupan berita dibatasi pada kata kunci yang relevan dengan industri kripto dan ekonomi global.

4. **Horizon Prediksi:** Model XGBoost hanya memprediksi terjadinya lonjakan volatilitas dalam **jangka waktu 3 jam ke depan** (*3-candle forward*). Prediksi jangka panjang (harian, mingguan) di luar cakupan sistem ini.

5. **Jenis Sinyal:** Sistem hanya menghasilkan sinyal **LONG** (beli/naik) dan **Wait & See** (tidak ada posisi). Sinyal **SHORT** (jual/turun) tidak diimplementasikan pada versi ini.

6. **Infrastruktur Komputasi:** Cluster Apache Spark dioperasikan dalam mode **Standalone** menggunakan maksimal **dua node laptop** yang terhubung melalui jaringan LAN lokal. Konfigurasi multi-node skala penuh di lingkungan *cloud* tidak termasuk dalam cakupan implementasi.

7. **Model Sentimen:** Analisis sentimen berita menggunakan metode **lexicon-based (VADER)** yang diperkaya dengan kosakata domain kripto. Metode berbasis *deep learning* (BERT, RoBERTa) tidak digunakan pada versi ini.

8. **Manajemen Risiko:** Parameter *Take Profit* (TP) dan *Stop Loss* (SL) bersifat **statis** berdasarkan persentase tetap per token dan tidak beradaptasi secara dinamis terhadap kondisi volatilitas pasar saat itu.

9. **Evaluasi Backtesting:** Evaluasi performa model dilakukan berdasarkan metrik klasifikasi standar pada data *holdout*. Simulasi *backtesting* yang mensimulasikan keuntungan/kerugian nyata dari eksekusi trading tidak termasuk dalam cakupan evaluasi ini.

---

## 1.4 Manfaat

Pengembangan sistem ROSBD Crypto Signals memberikan manfaat yang dapat ditinjau dari dua perspektif, yaitu manfaat teoritis dan manfaat praktis.

### 1.4.1 Manfaat Teoritis

1. **Kontribusi pada Rekayasa Pipeline Big Data:** Penelitian ini memberikan gambaran konkret tentang bagaimana komponen-komponen ekosistem *Big Data* (Kafka, Cassandra, Spark, Airflow) dapat diintegrasikan menjadi satu sistem yang kohesif untuk kasus penggunaan keuangan berbasis *real-time*.

2. **Referensi Implementasi Feature Engineering untuk Time-Series Finansial:** Penerapan indikator teknikal seperti Bollinger Bands, EMA, *Volume Ratio*, dan fitur jangkar BTC sebagai variabel prediktif memberikan referensi metodologi untuk penelitian prediksi deret waktu keuangan selanjutnya.

3. **Demonstrasi Integrasi ML Terdistribusi:** Penggunaan `SparkXGBClassifier` untuk melatih model XGBoost secara terdistribusi di atas infrastruktur Spark Standalone merupakan contoh penerapan *distributed machine learning* yang dapat diadopsi dalam konteks penelitian serupa.

4. **Pendekatan Hybrid Analisis (Teknikal + Fundamental):** Kombinasi sinyal teknikal (harga) dengan analisis sentimen berita (fundamental) dalam satu sistem otomatis memberikan kontribusi pada penelitian di bidang *quantitative finance* dan *computational finance*.

### 1.4.2 Manfaat Praktis

1. **Otomasi Pengambilan Keputusan Trading:** Sistem ini memungkinkan identifikasi peluang trading berbasis volatilitas secara otomatis, mengurangi kebutuhan pemantauan pasar manual yang melelahkan bagi trader individual maupun tim kecil.

2. **Notifikasi Real-Time yang Actionable:** Integrasi dengan Telegram Bot memastikan sinyal trading yang terdeteksi — dilengkapi dengan harga masuk, *Take Profit*, dan *Stop Loss* yang terkalkulasi — tersampaikan kepada pengguna dalam hitungan detik, di mana pun mereka berada.

3. **Infrastruktur Data yang Dapat Dikembangkan:** Arsitektur yang dibangun bersifat modular dan dapat dikembangkan (*scalable*) untuk menambahkan aset baru, metode prediksi baru, atau sumber data baru tanpa perlu merombak keseluruhan sistem.

4. **Dashboard Monitoring Terpusat:** Visualisasi real-time melalui Streamlit memberikan satu titik pantau (*single pane of glass*) untuk memantau harga pasar, status sinyal ML, dan umpan berita makroekonomi secara bersamaan.

5. **Referensi Pembelajaran Rekayasa Sistem Big Data:** Bagi komunitas akademik, khususnya mahasiswa sains data dan rekayasa perangkat lunak, sistem ini berfungsi sebagai referensi implementasi nyata (*working reference implementation*) dari konsep-konsep *Big Data Engineering* yang sering kali hanya dibahas secara teoretis di perkuliahan.

---

*Dokumen BAB I ini merupakan bagian dari dokumentasi formal proyek ROSBD Crypto Signals.*
*Terakhir diperbarui: 27 Juni 2026.*
