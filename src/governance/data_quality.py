"""
src/governance/data_quality.py
Modul Data Quality untuk pipeline crypto.

Fungsi:
- Validasi schema OHLCV (field tidak boleh null/negatif)
- Deteksi outlier ekstrem (harga atau volume di luar batas wajar)
- Cek data freshness (apakah data tidak terlalu lama)
- Generate laporan kualitas data
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Optional
import pandas as pd
from cassandra.cluster import Cluster

from src.utils.logger import get_logger, log_pipeline_event

logger = get_logger(__name__)

# ─── Konfigurasi batas validasi ───────────────────────────────────────────────
PRICE_BOUNDS = {
    "BTC": {"min": 1_000,   "max": 500_000},
    "ETH": {"min": 50,      "max": 50_000},
    "SOL": {"min": 0.5,     "max": 5_000},
    "XRP": {"min": 0.001,   "max": 100},
    "BNB": {"min": 1,       "max": 20_000},
}
MAX_STALENESS_HOURS = 3   # Data dianggap stale jika > 3 jam dari sekarang
MIN_VOLUME = 0.0          # Volume tidak boleh negatif


class DataQualityChecker:
    """Memeriksa kualitas data OHLCV sebelum diproses oleh pipeline."""

    def __init__(self, token: str):
        self.token = token
        self.report = {
            "token": token,
            "checked_at": datetime.utcnow().isoformat(),
            "total_rows": 0,
            "null_rows": 0,
            "negative_price_rows": 0,
            "outlier_rows": 0,
            "stale_rows": 0,
            "passed": True,
            "issues": [],
        }

    def validate_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Memastikan semua kolom wajib ada dan tidak null.
        Menghapus baris yang memiliki null di kolom OHLCV.
        """
        required_cols = ["Datetime", "Open", "High", "Low", "Close", "Volume"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            msg = f"Kolom wajib tidak ditemukan: {missing}"
            logger.error(f"[{self.token}] {msg}")
            self.report["issues"].append(msg)
            self.report["passed"] = False
            return pd.DataFrame()

        self.report["total_rows"] = len(df)
        before = len(df)
        df = df.dropna(subset=required_cols)
        self.report["null_rows"] = before - len(df)

        if self.report["null_rows"] > 0:
            logger.warning(
                f"[{self.token}] Dihapus {self.report['null_rows']} baris dengan nilai null."
            )
        return df

    def validate_prices(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Memastikan Open, High, Low, Close > 0 dan dalam rentang wajar token.
        """
        # Cek harga negatif
        price_cols = ["Open", "High", "Low", "Close"]
        neg_mask = (df[price_cols] <= 0).any(axis=1)
        self.report["negative_price_rows"] = int(neg_mask.sum())
        if self.report["negative_price_rows"] > 0:
            msg = f"{self.report['negative_price_rows']} baris dengan harga <= 0 dihapus"
            logger.warning(f"[{self.token}] {msg}")
            self.report["issues"].append(msg)
        df = df[~neg_mask]

        # Cek outlier menggunakan batas per-token
        if self.token in PRICE_BOUNDS:
            bounds = PRICE_BOUNDS[self.token]
            outlier_mask = (
                (df["Close"] < bounds["min"]) |
                (df["Close"] > bounds["max"])
            )
            self.report["outlier_rows"] = int(outlier_mask.sum())
            if self.report["outlier_rows"] > 0:
                msg = f"{self.report['outlier_rows']} baris outlier harga (batas: {bounds})"
                logger.warning(f"[{self.token}] {msg}")
                self.report["issues"].append(msg)
            df = df[~outlier_mask]

        # Cek volume
        vol_neg_mask = df["Volume"] < MIN_VOLUME
        if vol_neg_mask.any():
            msg = f"{int(vol_neg_mask.sum())} baris volume negatif dihapus"
            logger.warning(f"[{self.token}] {msg}")
            self.report["issues"].append(msg)
        df = df[~vol_neg_mask]

        # Validasi OHLC consistency: High >= Low, High >= Close, Low <= Close
        inconsistent = df[
            (df["High"] < df["Low"]) |
            (df["High"] < df["Close"]) |
            (df["Low"] > df["Close"])
        ]
        if not inconsistent.empty:
            msg = f"{len(inconsistent)} baris dengan OHLC tidak konsisten dihapus"
            logger.warning(f"[{self.token}] {msg}")
            self.report["issues"].append(msg)
            df = df.drop(inconsistent.index)

        return df

    def validate_freshness(self, df: pd.DataFrame) -> bool:
        """
        Memeriksa apakah data terbaru tidak lebih lama dari MAX_STALENESS_HOURS.
        Mengembalikan True jika data fresh, False jika stale.
        """
        if df.empty:
            return False

        df["Datetime"] = pd.to_datetime(df["Datetime"]).dt.tz_localize(None)
        latest = df["Datetime"].max()
        now_utc = datetime.utcnow()
        staleness_hours = (now_utc - latest).total_seconds() / 3600

        self.report["stale_rows"] = staleness_hours
        if staleness_hours > MAX_STALENESS_HOURS:
            msg = (
                f"Data stale: {staleness_hours:.1f} jam sejak update terakhir "
                f"(batas: {MAX_STALENESS_HOURS} jam)"
            )
            logger.warning(f"[{self.token}] {msg}")
            self.report["issues"].append(msg)
            # Stale tidak di-fail (data masih bisa dipakai), hanya dicatat
        return staleness_hours <= MAX_STALENESS_HOURS

    def run_all_checks(self, df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
        """
        Menjalankan semua pengecekan kualitas data secara berurutan.

        Returns:
            Tuple (df_cleaned, report_dict)
        """
        logger.info(f"[{self.token}] Memulai pemeriksaan kualitas data ({len(df)} baris)...")

        df = self.validate_schema(df)
        if df.empty:
            self.report["passed"] = False
            return df, self.report

        df = self.validate_prices(df)
        self.validate_freshness(df)

        passed_rows = len(df)
        total_rows = self.report["total_rows"]
        quality_pct = (passed_rows / total_rows * 100) if total_rows > 0 else 0

        self.report["passed_rows"] = passed_rows
        self.report["quality_pct"] = round(quality_pct, 2)
        self.report["passed"] = quality_pct >= 90.0  # Data dianggap layak jika 90%+ valid

        status = "LULUS" if self.report["passed"] else "GAGAL"
        logger.info(
            f"[{self.token}] Data Quality Check {status}: "
            f"{passed_rows}/{total_rows} baris valid ({quality_pct:.1f}%)"
        )

        log_pipeline_event("DATA_QUALITY", {
            "token": self.token,
            "status": status,
            "total": total_rows,
            "passed": passed_rows,
            "quality_pct": quality_pct,
            "issues": len(self.report["issues"]),
        })

        return df, self.report


def check_dataframe_quality(df: pd.DataFrame, token: str) -> tuple[pd.DataFrame, dict]:
    """
    Fungsi helper untuk langsung menjalankan semua DQ checks.

    Usage:
        from src.governance.data_quality import check_dataframe_quality
        df_clean, report = check_dataframe_quality(df_raw, "BTC")
    """
    checker = DataQualityChecker(token=token)
    return checker.run_all_checks(df)
