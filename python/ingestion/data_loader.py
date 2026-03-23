"""
python/ingestion/data_loader.py
--------------------------------
Stage 1 of the BI pipeline: CSV ingestion with validation.

Reads raw sales_operations.csv, validates schema and data quality,
then bulk-inserts into the stg_sales_raw table.

Usage (standalone):
    python -m python.ingestion.data_loader
"""

import os
import logging
import pandas as pd
from datetime import datetime
from typing import Tuple

from python.utils.db_connector import DBConnector
from python.utils.config_loader import load_config

logger = logging.getLogger(__name__)

# Expected schema: column → expected dtype category
REQUIRED_COLUMNS = {
    "transaction_id":   "object",
    "date":             "object",
    "product_name":     "object",
    "product_category": "object",
    "region":           "object",
    "sales_rep":        "object",
    "customer_id":      "object",
    "customer_segment": "object",
    "channel":          "object",
    "units_sold":       "numeric",
    "unit_price":       "numeric",
    "revenue":          "numeric",
    "cost":             "numeric",
    "discount_pct":     "numeric",
}

VALID_REGIONS    = {"North", "South", "East", "West", "International"}
VALID_CHANNELS   = {"Direct", "Partner", "Online"}
VALID_SEGMENTS   = {"Enterprise", "SMB", "Consumer"}
VALID_CATEGORIES = {"Electronics", "Software", "Services", "Hardware"}


class DataLoader:
    """Handles CSV ingestion, validation, and staging table load."""

    def __init__(self, config: dict = None):
        self.config = config or load_config()
        self.db = DBConnector(self.config["database"])
        self.raw_path = self.config["data"]["raw_path"]

    # ── Public Interface ─────────────────────────────────────────────────────

    def run(self) -> Tuple[int, int]:
        """
        Execute the ingestion pipeline.

        Returns:
            (rows_loaded, rows_rejected) tuple
        """
        logger.info("=" * 60)
        logger.info("STAGE 1: Data Ingestion")
        logger.info("=" * 60)

        df_raw = self._read_csv()
        df_clean, df_rejected = self._validate(df_raw)
        rows_loaded = self._load_to_staging(df_clean)

        logger.info(f"Ingestion complete — loaded: {rows_loaded:,}, rejected: {len(df_rejected):,}")
        return rows_loaded, len(df_rejected)

    # ── Private Methods ──────────────────────────────────────────────────────

    def _read_csv(self) -> pd.DataFrame:
        """Read the raw CSV file."""
        if not os.path.exists(self.raw_path):
            raise FileNotFoundError(
                f"Raw data file not found: {self.raw_path}\n"
                "Run `python generate_dataset.py` first to create the dataset."
            )

        logger.info(f"Reading CSV: {self.raw_path}")
        df = pd.read_csv(self.raw_path, dtype=str, low_memory=False)
        logger.info(f"  → {len(df):,} rows, {len(df.columns)} columns loaded")
        return df

    def _validate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Validate schema and data quality. Return (clean, rejected) DataFrames.
        """
        logger.info("Validating data quality...")
        issues = []

        # Schema check
        missing_cols = set(REQUIRED_COLUMNS.keys()) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        df = df.copy()

        # Type coercions
        numeric_cols = ["units_sold", "unit_price", "revenue", "cost", "discount_pct"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Tag rows with issues
        mask_null_id      = df["transaction_id"].isna() | (df["transaction_id"].str.strip() == "")
        mask_null_date    = df["date"].isna()
        mask_null_revenue = df["revenue"].isna()
        mask_neg_revenue  = df["revenue"] < 0
        mask_neg_cost     = df["cost"] < 0
        mask_bad_discount = (df["discount_pct"] < 0) | (df["discount_pct"] > 100)
        mask_zero_units   = df["units_sold"].fillna(0) <= 0

        reject_mask = (
            mask_null_id | mask_null_date | mask_null_revenue |
            mask_neg_revenue | mask_neg_cost
        )

        df_rejected = df[reject_mask].copy()
        df_clean    = df[~reject_mask].copy()

        # Log validation summary
        checks = [
            ("Null transaction_id",   mask_null_id.sum()),
            ("Null date",             mask_null_date.sum()),
            ("Null/invalid revenue",  mask_null_revenue.sum()),
            ("Negative revenue",      mask_neg_revenue.sum()),
            ("Negative cost",         mask_neg_cost.sum()),
            ("Invalid discount %",    mask_bad_discount.sum()),
            ("Zero/null units_sold",  mask_zero_units.sum()),
        ]
        for check, count in checks:
            level = logging.WARNING if count > 0 else logging.DEBUG
            logger.log(level, f"  {check}: {count:,} rows")

        # Auto-fix non-critical issues in clean set
        df_clean["discount_pct"] = df_clean["discount_pct"].clip(0, 100).fillna(0)
        df_clean["units_sold"]   = df_clean["units_sold"].fillna(1).clip(lower=1)
        df_clean["cost"]         = df_clean["cost"].fillna(0)

        # Standardize categoricals
        df_clean["region"]           = df_clean["region"].str.strip().str.title()
        df_clean["channel"]          = df_clean["channel"].str.strip().str.title()
        df_clean["customer_segment"] = df_clean["customer_segment"].str.strip()
        df_clean["product_category"] = df_clean["product_category"].str.strip()

        # Add load timestamp
        df_clean["loaded_at"] = datetime.now().isoformat()

        logger.info(f"  Clean rows: {len(df_clean):,} | Rejected: {len(df_rejected):,}")
        return df_clean, df_rejected

    def _load_to_staging(self, df: pd.DataFrame) -> int:
        """Bulk insert clean DataFrame into stg_sales_raw."""
        logger.info("Loading data into staging table: stg_sales_raw")

        with self.db.connection() as conn:
            # Clear staging table
            conn.execute("DELETE FROM stg_sales_raw")
            logger.debug("Cleared existing staging data")

        self.db.connect()
        rows = self.db.bulk_insert_df(
            df=df[list(REQUIRED_COLUMNS.keys()) + ["loaded_at"]],
            table_name="stg_sales_raw",
            if_exists="append",
            chunk_size=2000,
        )
        self.db.disconnect()
        return rows


if __name__ == "__main__":
    from python.utils.logger import setup_logger
    setup_logger()
    loader = DataLoader()
    loader.run()
