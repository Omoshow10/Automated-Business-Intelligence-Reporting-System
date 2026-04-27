"""
python/reporting/anomaly_detector.py
--------------------------------------
Stage 3a: Statistical anomaly detection on revenue and cost columns.

Methods:
    - Z-score: flags transactions where |z| > threshold (default 2.5)
    - IQR:     flags transactions outside [Q1 - k*IQR, Q3 + k*IQR]

Platform : MS SQL Server 2019+ via pyodbc
Author   : Olayinka Somuyiwa

Fix (2026-04-26):
    - Guards against empty core_sales table (returns empty DataFrame
      gracefully instead of crashing on missing columns)
    - sort_values and concat now only operate on non-empty frames
    - _detect_zscore always returns a DataFrame with the correct columns
      even when no anomalies are found
"""

import logging
import os
import datetime
import pandas as pd
import numpy as np

from python.utils.db_connector import DBConnector
from python.utils.config_loader import load_config

logger = logging.getLogger(__name__)

# Columns that must exist in every returned anomaly DataFrame
ANOMALY_COLUMNS = [
    "transaction_id", "txn_date", "product_name", "region",
    "revenue", "cost", "revenue_zscore", "cost_zscore", "anomaly_type",
]


class AnomalyDetector:
    """Detects statistical outliers in the sales dataset."""

    def __init__(self, config: dict = None):
        self.config    = config or load_config()
        self.db        = DBConnector(self.config["database"])
        self.threshold = self.config["anomaly"]["zscore_threshold"]
        self.iqr_mult  = self.config["anomaly"]["iqr_multiplier"]
        self.columns   = self.config["anomaly"]["columns"]
        self.out_path  = self.config["data"]["processed_path"]

    # ── Public Interface ─────────────────────────────────────────────────────

    def run(self) -> pd.DataFrame:
        """
        Run anomaly detection and persist results.
        Returns DataFrame of detected anomalies (may be empty).
        """
        logger.info("=" * 60)
        logger.info("STAGE 3a: Anomaly Detection")
        logger.info("=" * 60)

        self.db.connect()
        try:
            cursor = self.db._conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM dbo.core_sales")
            row_count = cursor.fetchone()[0]
        finally:
            self.db.disconnect()

        if row_count == 0:
            logger.warning(
                "core_sales is empty — skipping anomaly detection. "
                "Ensure the transformation stage populated the table."
            )
            return pd.DataFrame(columns=ANOMALY_COLUMNS)

        self.db.connect()
        df = self._query_df(
            "SELECT transaction_id, txn_date, product_name, product_category, "
            "region, revenue, cost, discount_pct FROM dbo.core_sales"
        )
        self.db.disconnect()

        logger.info(f"Analysing {len(df):,} transactions for anomalies...")

        frames = []

        for col in self.columns:
            result = self._detect_zscore(df, col)
            if not result.empty:
                frames.append(result)
                logger.info(
                    f"  Z-score [{col}]: {len(result):,} anomalies flagged"
                )
            else:
                logger.info(f"  Z-score [{col}]: 0 anomalies flagged")

        iqr_result = self._detect_iqr(df, "discount_pct")
        if not iqr_result.empty:
            frames.append(iqr_result)
            logger.info(
                f"  IQR [discount_pct]: {len(iqr_result):,} anomalies flagged"
            )
        else:
            logger.info("  IQR [discount_pct]: 0 anomalies flagged")

        if not frames:
            logger.info("No anomalies detected.")
            return pd.DataFrame(columns=ANOMALY_COLUMNS)

        anomalies = pd.concat(frames, ignore_index=True)

        # Deduplicate — keep the row with the highest absolute z-score
        anomalies["_abs_z"] = anomalies["revenue_zscore"].abs().fillna(0)
        anomalies = (
            anomalies
            .sort_values("_abs_z", ascending=False)
            .drop_duplicates(subset="transaction_id", keep="first")
            .drop(columns=["_abs_z"])
            .reset_index(drop=True)
        )

        logger.info(
            f"Total unique anomalous transactions: {len(anomalies):,}"
        )

        self._save_anomalies(anomalies)
        self._print_summary(anomalies)

        return anomalies

    # ── Detection Methods ─────────────────────────────────────────────────────

    def _detect_zscore(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        """
        Flag rows where the Z-score of `column` exceeds self.threshold.
        Z-score computed per product_category to control for price variation.
        Always returns a DataFrame with ANOMALY_COLUMNS (may be empty).
        """
        empty = pd.DataFrame(columns=ANOMALY_COLUMNS)

        if df.empty or column not in df.columns:
            return empty

        df = df.copy()
        grp_mean = df.groupby("product_category")[column].transform("mean")
        grp_std  = df.groupby("product_category")[column].transform("std")
        grp_std  = grp_std.replace(0, np.nan)

        df["_zscore"] = ((df[column] - grp_mean) / grp_std).fillna(0)

        flagged = df[df["_zscore"].abs() > self.threshold].copy()
        if flagged.empty:
            return empty

        flagged["anomaly_type"] = flagged["_zscore"].apply(
            lambda z: f"HIGH_{column.upper()}" if z > 0
                      else f"LOW_{column.upper()}"
        )
        flagged["revenue_zscore"] = (
            flagged["_zscore"] if column == "revenue" else np.nan
        )
        flagged["cost_zscore"] = (
            flagged["_zscore"] if column == "cost" else np.nan
        )

        return flagged[[
            "transaction_id", "txn_date", "product_name", "region",
            "revenue", "cost", "revenue_zscore", "cost_zscore", "anomaly_type",
        ]].copy()

    def _detect_iqr(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        """Flag rows outside Q1 - k*IQR and Q3 + k*IQR bounds."""
        empty = pd.DataFrame(columns=ANOMALY_COLUMNS)

        if df.empty or column not in df.columns:
            return empty

        q1  = df[column].quantile(0.25)
        q3  = df[column].quantile(0.75)
        iqr = q3 - q1
        lower = q1 - self.iqr_mult * iqr
        upper = q3 + self.iqr_mult * iqr

        flagged = df[(df[column] < lower) | (df[column] > upper)].copy()
        if flagged.empty:
            return empty

        flagged["anomaly_type"]   = "DISCOUNT_OUTLIER"
        flagged["revenue_zscore"] = np.nan
        flagged["cost_zscore"]    = np.nan

        return flagged[[
            "transaction_id", "txn_date", "product_name", "region",
            "revenue", "cost", "revenue_zscore", "cost_zscore", "anomaly_type",
        ]].copy()

    # ── Persistence ───────────────────────────────────────────────────────────

    def _save_anomalies(self, anomalies: pd.DataFrame) -> None:
        """Write anomalies to dbo.rpt_anomalies and CSV."""
        if anomalies.empty:
            logger.info("No anomalies to save.")
            return

        anomalies = anomalies.copy()
        anomalies["detected_at"] = datetime.datetime.now()

        # Ensure txn_date is a date object for SQL Server DATE column
        if anomalies["txn_date"].dtype == object:
            anomalies["txn_date"] = pd.to_datetime(
                anomalies["txn_date"], errors="coerce"
            ).dt.date

        # Ensure numeric columns are Python float/None (not numpy)
        for col in ["revenue", "cost", "revenue_zscore", "cost_zscore"]:
            anomalies[col] = anomalies[col].apply(
                lambda x: None if (x is None or (isinstance(x, float) and np.isnan(x)))
                          else float(x)
            )

        insert_cols = [
            "transaction_id", "txn_date", "product_name", "region",
            "revenue", "cost", "revenue_zscore", "cost_zscore",
            "anomaly_type", "detected_at",
        ]

        self.db.connect()
        self.db._conn.cursor().execute("DELETE FROM dbo.rpt_anomalies")
        self.db._conn.commit()
        self.db.bulk_insert_df(
            anomalies[insert_cols],
            table_name="rpt_anomalies",
            schema="dbo",
        )
        self.db.disconnect()
        logger.info(
            f"Saved {len(anomalies):,} anomalies to dbo.rpt_anomalies"
        )

        os.makedirs(self.out_path, exist_ok=True)
        csv_path = os.path.join(self.out_path, "anomalies.csv")
        anomalies.to_csv(csv_path, index=False)
        logger.info(f"Saved anomalies CSV: {csv_path}")

    def _query_df(self, sql: str) -> pd.DataFrame:
        """
        Execute a SELECT using raw pyodbc cursor (avoids pandas warning).

        pyodbc returns SQL Server DECIMAL/NUMERIC columns as Python
        decimal.Decimal objects — not compatible with numpy arithmetic.
        Any column containing Decimal values is cast to float64 here
        so Z-score and IQR calculations work without TypeError.
        """
        import decimal

        cursor = self.db._conn.cursor()
        cursor.execute(sql)
        columns = [col[0] for col in cursor.description]
        rows    = cursor.fetchall()
        df      = pd.DataFrame.from_records(rows, columns=columns)

        # Cast decimal.Decimal columns → float64
        for col in df.columns:
            if not df[col].empty and df[col].apply(
                lambda x: isinstance(x, decimal.Decimal)
            ).any():
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    def _print_summary(self, anomalies: pd.DataFrame) -> None:
        """Log a summary of detected anomaly types."""
        if anomalies.empty:
            return
        summary = (
            anomalies
            .groupby("anomaly_type")
            .agg(count=("transaction_id", "count"),
                 total_revenue=("revenue", "sum"))
            .reset_index()
        )
        logger.info("Anomaly Summary:")
        for _, row in summary.iterrows():
            logger.info(
                f"  {row['anomaly_type']:<25} "
                f"{int(row['count']):>5} transactions  "
                f"${row['total_revenue']:>12,.2f} revenue"
            )


if __name__ == "__main__":
    from python.utils.logger import setup_logger
    setup_logger()
    detector = AnomalyDetector()
    detector.run()
