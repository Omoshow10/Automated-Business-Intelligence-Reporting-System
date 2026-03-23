"""
python/reporting/anomaly_detector.py
--------------------------------------
Stage 3a: Statistical anomaly detection on revenue and cost columns.

Methods:
    - Z-score: flags transactions where |z| > threshold (default 2.5)
    - IQR:     flags transactions outside [Q1 - k*IQR, Q3 + k*IQR]

Results are written to:
    - rpt_anomalies table (for Power BI Page 4)
    - data/processed/anomalies.csv (for export/audit)

Usage:
    python -m python.reporting.anomaly_detector
"""

import logging
import os
import pandas as pd
import numpy as np
from datetime import datetime

from python.utils.db_connector import DBConnector
from python.utils.config_loader import load_config

logger = logging.getLogger(__name__)


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

        Returns:
            DataFrame of detected anomalies.
        """
        logger.info("=" * 60)
        logger.info("STAGE 3a: Anomaly Detection")
        logger.info("=" * 60)

        self.db.connect()
        df = self.db.query_df("SELECT * FROM core_sales")
        self.db.disconnect()

        logger.info(f"Analyzing {len(df):,} transactions for anomalies...")

        anomalies = pd.DataFrame()

        # Z-score detection on revenue and cost
        for col in self.columns:
            z_anomalies = self._detect_zscore(df, col)
            anomalies = pd.concat([anomalies, z_anomalies], ignore_index=True)
            logger.info(f"  Z-score [{col}]: {len(z_anomalies):,} anomalies flagged")

        # IQR detection on discount_pct
        iqr_anomalies = self._detect_iqr(df, "discount_pct")
        anomalies = pd.concat([anomalies, iqr_anomalies], ignore_index=True)
        logger.info(f"  IQR [discount_pct]: {len(iqr_anomalies):,} anomalies flagged")

        # Deduplicate (same transaction flagged by multiple methods)
        anomalies = anomalies.sort_values("revenue_zscore", ascending=False)
        anomalies = anomalies.drop_duplicates(subset="transaction_id", keep="first")

        logger.info(f"Total unique anomalous transactions: {len(anomalies):,}")

        self._save_anomalies(anomalies)
        self._print_summary(anomalies)

        return anomalies

    # ── Detection Methods ─────────────────────────────────────────────────────

    def _detect_zscore(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        """
        Flag rows where the Z-score of `column` exceeds self.threshold.
        Z-score computed per product_category to control for price variation.
        """
        df = df.copy()

        # Compute Z-score within product_category groups
        df["_mean"] = df.groupby("product_category")[column].transform("mean")
        df["_std"]  = df.groupby("product_category")[column].transform("std")

        # Handle zero std (all values identical in group)
        df["_std"] = df["_std"].replace(0, np.nan)

        df["zscore"] = (df[column] - df["_mean"]) / df["_std"]
        df["zscore"] = df["zscore"].fillna(0)

        flagged = df[df["zscore"].abs() > self.threshold].copy()

        if len(flagged) == 0:
            return pd.DataFrame()

        # Determine anomaly type
        flagged["anomaly_type"] = flagged["zscore"].apply(
            lambda z: f"HIGH_{column.upper()}" if z > 0 else f"LOW_{column.upper()}"
        )
        flagged["revenue_zscore"] = flagged["zscore"] if column == "revenue" else np.nan
        flagged["cost_zscore"]    = flagged["zscore"] if column == "cost"    else np.nan

        result = flagged[[
            "transaction_id", "txn_date", "product_name", "region",
            "revenue", "cost", "revenue_zscore", "cost_zscore", "anomaly_type"
        ]].copy()

        result.columns = [
            "transaction_id", "txn_date", "product_name", "region",
            "revenue", "cost", "revenue_zscore", "cost_zscore", "anomaly_type"
        ]
        return result

    def _detect_iqr(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        """Flag rows outside Q1 - k*IQR and Q3 + k*IQR bounds."""
        q1  = df[column].quantile(0.25)
        q3  = df[column].quantile(0.75)
        iqr = q3 - q1

        lower = q1 - self.iqr_mult * iqr
        upper = q3 + self.iqr_mult * iqr

        flagged = df[(df[column] < lower) | (df[column] > upper)].copy()

        if len(flagged) == 0:
            return pd.DataFrame()

        flagged["anomaly_type"] = "DISCOUNT_OUTLIER"
        flagged["revenue_zscore"] = np.nan
        flagged["cost_zscore"]    = np.nan

        return flagged[[
            "transaction_id", "txn_date", "product_name", "region",
            "revenue", "cost", "revenue_zscore", "cost_zscore", "anomaly_type"
        ]]

    # ── Persistence ───────────────────────────────────────────────────────────

    def _save_anomalies(self, anomalies: pd.DataFrame) -> None:
        """Write anomalies to rpt_anomalies table and CSV."""
        if anomalies.empty:
            logger.info("No anomalies detected — skipping save.")
            return

        anomalies["detected_at"] = datetime.now().isoformat()

        # Save to database
        self.db.connect()
        self.db._conn.execute("DELETE FROM rpt_anomalies")
        self.db._conn.commit()
        self.db.bulk_insert_df(anomalies, "rpt_anomalies", if_exists="append")
        self.db.disconnect()
        logger.info(f"Saved {len(anomalies):,} anomalies to rpt_anomalies table")

        # Save to CSV
        os.makedirs(self.out_path, exist_ok=True)
        csv_path = os.path.join(self.out_path, "anomalies.csv")
        anomalies.to_csv(csv_path, index=False)
        logger.info(f"Saved anomalies CSV: {csv_path}")

    def _print_summary(self, anomalies: pd.DataFrame) -> None:
        """Log a summary table of detected anomaly types."""
        if anomalies.empty:
            return
        summary = anomalies.groupby("anomaly_type").agg(
            count=("transaction_id", "count"),
            total_revenue=("revenue", "sum"),
        ).reset_index()
        logger.info("\nAnomaly Summary:")
        for _, row in summary.iterrows():
            logger.info(
                f"  {row['anomaly_type']:<22} "
                f"{int(row['count']):>5} transactions  "
                f"${row['total_revenue']:>12,.2f} revenue"
            )


if __name__ == "__main__":
    from python.utils.logger import setup_logger
    setup_logger()
    detector = AnomalyDetector()
    detector.run()
