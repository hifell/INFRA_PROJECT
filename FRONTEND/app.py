"""
FRONTEND/app.py
Streamlit Dashboard — ROSBD Operational Trading & Monitoring

Fitur:
- Harga real-time per token dengan candlestick chart (via Altair)
- Sinyal ML (XGBoost probability + TP/SL)
- Sentimen berita global
- Status monitoring pipeline (Kafka, Cassandra, Airflow)
- Audit trail logs terbaru
- Data quality metrics
"""

import streamlit as st
import psycopg2
import pandas as pd
import altair as alt
import requests
import time
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta

# Tambahkan root project ke sys.path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

st.set_page_config(
    page_title="ROSBD Crypto Signal Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

from dotenv import load_dotenv
load_dotenv(project_root / ".env")

DB_HOST     = os.environ.get("DB_HOST", "localhost")
DB_PORT     = int(os.environ.get("DB_PORT", 5432))
DB_NAME     = os.environ.get("DB_NAME", "postgres")
DB_USER     = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "localhost")
if CASSANDRA_HOST == "cassandra":
    CASSANDRA_HOST = "localhost"
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", 9042))

# ─── CSS Kustom ───────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main { background-color: #0e1117; }
    .stMetric { background: #1c2333; border-radius: 10px; padding: 10px; border: 1px solid #2d3748; }
    .status-ok  { color: #48bb78; font-weight: bold; }
    .status-err { color: #fc8181; font-weight: bold; }
    .signal-long { background: linear-gradient(135deg, #1a4731, #22543d); border-left: 4px solid #48bb78; padding: 8px 12px; border-radius: 5px; }
    .signal-wait { background: linear-gradient(135deg, #1a202c, #2d3748); border-left: 4px solid #718096; padding: 8px 12px; border-radius: 5px; }
    h1, h2, h3 { color: #e2e8f0 !important; }
</style>
""", unsafe_allow_html=True)

# ─── Helper Functions ─────────────────────────────────────────────────────────
def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        database=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )

@st.cache_data(ttl=5)
def get_realtime_price(token: str):
    """Ambil harga real-time dari Yahoo Finance."""
    import yfinance as yf
    try:
        ticker = yf.Ticker(f"{token.strip().upper()}-USD")
        return float(ticker.fast_info['lastPrice'])
    except Exception:
        return None

@st.cache_data(ttl=60)
def get_cassandra_ohlcv(token: str, hours: int = 24):
    """Ambil data OHLCV dari Cassandra untuk chart."""
    try:
        from cassandra.cluster import Cluster
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT, connect_timeout=5)
        session = cluster.connect("crypto_ks")
        rows = session.execute(
            f'SELECT datetime, open, high, low, close, volume '
            f'FROM signals WHERE "token" = \'{token}\' '
            f'ORDER BY datetime DESC LIMIT {hours}'
        )
        data = [{"Datetime": r.datetime, "Open": r.open, "High": r.high,
                 "Low": r.low, "Close": r.close, "Volume": r.volume} for r in rows]
        cluster.shutdown()
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df["Datetime"] = pd.to_datetime(df["Datetime"])
        return df.sort_values("Datetime").reset_index(drop=True)
    except Exception as e:
        return pd.DataFrame()

def get_pipeline_status():
    """Cek status semua service pipeline."""
    status = {}
    # Cek Cassandra
    try:
        from cassandra.cluster import Cluster
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT, connect_timeout=3)
        cluster.connect()
        cluster.shutdown()
        status["cassandra"] = ("✅ Online", "status-ok")
    except Exception as e:
        status["cassandra"] = ("❌ Offline", "status-err")
    # Cek Airflow
    try:
        r = requests.get("http://localhost:8080/health", timeout=3)
        if r.status_code == 200:
            status["airflow"] = ("✅ Online", "status-ok")
        else:
            status["airflow"] = ("⚠️ Degraded", "status-err")
    except Exception:
        status["airflow"] = ("❌ Offline", "status-err")
    # Cek Grafana
    try:
        r = requests.get("http://localhost:3000/api/health", timeout=3)
        status["grafana"] = ("✅ Online", "status-ok") if r.status_code == 200 else ("⚠️ Degraded", "status-err")
    except Exception:
        status["grafana"] = ("❌ Offline", "status-err")
    # Cek PostgreSQL
    try:
        conn = get_db_connection()
        conn.close()
        status["postgresql"] = ("✅ Online", "status-ok")
    except Exception:
        status["postgresql"] = ("❌ Offline", "status-err")
    return status

# ─── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🔧 Panel Kontrol")
    st.caption(f"Last refresh: {datetime.now().strftime('%H:%M:%S')}")

    st.subheader("📡 Status Service")
    services = get_pipeline_status()
    for svc, (label, css) in services.items():
        st.markdown(f"**{svc.capitalize()}:** <span class='{css}'>{label}</span>", unsafe_allow_html=True)

    st.divider()
    st.subheader("⚙️ Pengaturan")
    selected_token = st.selectbox("Token Aktif", ["BTC", "ETH", "SOL", "XRP", "BNB"])
    chart_hours = st.slider("Rentang Chart (jam)", 6, 168, 24)
    auto_refresh = st.checkbox("Auto Refresh (10 detik)", value=True)

    st.divider()
    st.markdown("**🔗 Quick Links**")
    st.markdown("- [Airflow UI](http://localhost:8080)")
    st.markdown("- [Grafana Dashboard](http://localhost:3000)")
    st.markdown("- [Spark Master](http://localhost:8081)")
    st.markdown("- [Prometheus](http://localhost:9090)")
    st.markdown("- [Health Check](http://localhost:8000/health)")

# ─── Header ───────────────────────────────────────────────────────────────────
st.title("📈 ROSBD Crypto Signal Dashboard")
st.caption("Sistem Big Data End-to-End: Kafka → Cassandra → Spark → XGBoost → Monitoring")

# ─── Tab Layout ───────────────────────────────────────────────────────────────
tab_market, tab_chart, tab_news, tab_audit = st.tabs([
    "📊 Market & Sinyal", "📈 Chart OHLCV", "📰 Berita & Sentimen", "🔍 Audit Trail"
])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1: Market & Sinyal ML
# ══════════════════════════════════════════════════════════════════════════════
with tab_market:
    st.subheader("📊 Sinyal Trading Real-Time")

    try:
        conn = get_db_connection()
        query_signals = """
            SELECT DISTINCT ON (token) token, price, probability, signal_status,
                   take_profit, stop_loss, created_at
            FROM v_crypto_signals
            ORDER BY token, created_at DESC;
        """
        df_signals = pd.read_sql(query_signals, conn)
        conn.close()

        if not df_signals.empty:
            cols = st.columns(len(df_signals))
            for i, (_, row) in enumerate(df_signals.iterrows()):
                with cols[i]:
                    live_price = get_realtime_price(row['token'])
                    display_price = live_price if live_price is not None else float(row['price'])
                    price_src = "🔴 Live" if live_price else "⚪ DB"

                    st.metric(
                        label=f"{row['token']} ({price_src})",
                        value=f"${display_price:,.4f}",
                    )

                    prob = float(row['probability'])
                    status = row['signal_status']

                    if prob > 50 and "LONG" in str(status):
                        st.markdown(f"""<div class='signal-long'>
                            🔥 <b>{status}</b><br>
                            📊 Prob: <b>{prob:.1f}%</b><br>
                            🎯 TP: ${float(row['take_profit']):,.2f}<br>
                            🛑 SL: ${float(row['stop_loss']):,.2f}
                        </div>""", unsafe_allow_html=True)
                    else:
                        st.markdown(f"""<div class='signal-wait'>
                            ⚪ <b>Wait & See</b><br>
                            📊 Prob: {prob:.1f}%
                        </div>""", unsafe_allow_html=True)

            st.caption(f"Sync terakhir: {df_signals['created_at'].max().strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            st.info("💡 Belum ada sinyal. Jalankan pipeline terlebih dahulu.")
    except Exception as e:
        st.error(f"Database error: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2: Chart OHLCV
# ══════════════════════════════════════════════════════════════════════════════
with tab_chart:
    st.subheader(f"📈 Data OHLCV — {selected_token} (last {chart_hours} jam)")

    df_ohlcv = get_cassandra_ohlcv(selected_token, chart_hours)

    if not df_ohlcv.empty:
        # Close price line chart
        close_chart = alt.Chart(df_ohlcv).mark_line(
            color='#63b3ed',
            strokeWidth=2
        ).encode(
            x=alt.X('Datetime:T', title='Waktu', axis=alt.Axis(format='%m/%d %H:%M')),
            y=alt.Y('Close:Q', title='Harga (USD)',
                    scale=alt.Scale(zero=False)),
            tooltip=[
                alt.Tooltip('Datetime:T', title='Waktu', format='%Y-%m-%d %H:%M'),
                alt.Tooltip('Open:Q', title='Open', format=',.4f'),
                alt.Tooltip('High:Q', title='High', format=',.4f'),
                alt.Tooltip('Low:Q', title='Low', format=',.4f'),
                alt.Tooltip('Close:Q', title='Close', format=',.4f'),
            ]
        ).properties(height=300, title=f"{selected_token}/USD — Harga Penutupan")

        # Volume bar chart
        vol_chart = alt.Chart(df_ohlcv).mark_bar(
            color='#9f7aea',
            opacity=0.7
        ).encode(
            x=alt.X('Datetime:T', title='Waktu'),
            y=alt.Y('Volume:Q', title='Volume'),
            tooltip=[
                alt.Tooltip('Datetime:T', title='Waktu', format='%Y-%m-%d %H:%M'),
                alt.Tooltip('Volume:Q', title='Volume', format=',.0f'),
            ]
        ).properties(height=120, title="Volume")

        combined = alt.vconcat(close_chart, vol_chart).resolve_scale(x='shared')
        st.altair_chart(combined, use_container_width=True)

        # Ringkasan statistik
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Harga Tertinggi", f"${df_ohlcv['High'].max():,.4f}")
        col2.metric("Harga Terendah", f"${df_ohlcv['Low'].min():,.4f}")
        col3.metric("Rata-rata Close", f"${df_ohlcv['Close'].mean():,.4f}")
        col4.metric("Total Volume", f"{df_ohlcv['Volume'].sum():,.0f}")
    else:
        st.warning(f"Data OHLCV {selected_token} tidak tersedia. Pastikan Cassandra aktif dan pipeline telah dijalankan.")
        st.code(f"docker compose up -d", language="bash")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3: Berita & Sentimen
# ══════════════════════════════════════════════════════════════════════════════
with tab_news:
    st.subheader("📰 Berita Global & Sentimen Makro")

    try:
        conn = get_db_connection()
        query_news = """
            SELECT source_name, title, description, url, published_at, sentiment
            FROM v_market_news
            ORDER BY created_at DESC LIMIT 15;
        """
        df_news = pd.read_sql(query_news, conn)
        conn.close()

        if not df_news.empty:
            counts = df_news['sentiment'].value_counts()
            s1, s2, s3 = st.columns(3)
            s1.metric("🟢 Bullish", counts.get('POSITIVE', 0))
            s2.metric("🔴 Bearish", counts.get('NEGATIVE', 0))
            s3.metric("⚪ Neutral", counts.get('NEUTRAL', 0))

            st.divider()
            emoji_map = {"POSITIVE": "🟢", "NEGATIVE": "🔴", "NEUTRAL": "⚪"}
            color_map = {"POSITIVE": ":green[BULLISH]", "NEGATIVE": ":red[BEARISH]", "NEUTRAL": ":gray[NEUTRAL]"}

            for _, row in df_news.iterrows():
                sentiment = (row['sentiment'] or "NEUTRAL").upper()
                emoji = emoji_map.get(sentiment, "⚪")
                color = color_map.get(sentiment, ":gray[NEUTRAL]")
                with st.expander(f"{emoji} [{row['source_name']}] {row['title'][:60]}..."):
                    st.markdown(f"**Sentimen:** {color}")
                    st.markdown(f"**Publikasi:** `{row['published_at']}`")
                    st.write(row['description'])
                    st.markdown(f"[Baca selengkapnya ↗]({row['url']})")
        else:
            st.info("💡 Belum ada data berita.")
    except Exception as e:
        st.error(f"Gagal mengambil berita: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 4: Audit Trail
# ══════════════════════════════════════════════════════════════════════════════
with tab_audit:
    st.subheader("🔍 Audit Trail Pipeline")
    st.caption("Rekaman setiap aksi pipeline: ingestion, training, scanning, alerting")

    try:
        conn = get_db_connection()
        query_audit = """
            SELECT event_type, actor, token, status, rows_affected,
                   duration_ms, details, created_at
            FROM pipeline_audit_log
            ORDER BY created_at DESC
            LIMIT 50;
        """
        df_audit = pd.read_sql(query_audit, conn)
        conn.close()

        if not df_audit.empty:
            col_filter, col_token = st.columns(2)
            event_filter = col_filter.multiselect(
                "Filter Event Type",
                options=df_audit['event_type'].unique().tolist(),
                default=df_audit['event_type'].unique().tolist()
            )
            token_filter = col_token.multiselect(
                "Filter Token",
                options=[t for t in df_audit['token'].unique() if t],
                default=[t for t in df_audit['token'].unique() if t]
            )

            df_filtered = df_audit[df_audit['event_type'].isin(event_filter)]
            if token_filter:
                df_filtered = df_filtered[df_filtered['token'].isin(token_filter)]

            def status_color(val):
                if val == "SUCCESS":
                    return "background-color: #1a4731; color: #68d391"
                elif val == "FAILURE":
                    return "background-color: #742a2a; color: #fc8181"
                return ""

            styled = df_filtered[[
                'created_at', 'event_type', 'actor', 'token', 'status',
                'rows_affected', 'duration_ms'
            ]].style.applymap(status_color, subset=['status'])

            st.dataframe(styled, use_container_width=True, height=400)
            st.caption(f"Menampilkan {len(df_filtered)} dari {len(df_audit)} event terbaru")
        else:
            st.info("💡 Tabel audit_log belum ada. Pipeline akan membuatnya otomatis saat pertama dijalankan.")
    except Exception as e:
        st.warning(f"Audit trail belum tersedia: {e}")
        st.info("Tabel `pipeline_audit_log` akan dibuat otomatis saat pipeline berjalan pertama kali.")

# ─── Auto Refresh ─────────────────────────────────────────────────────────────
if auto_refresh:
    time.sleep(10)
    st.rerun()