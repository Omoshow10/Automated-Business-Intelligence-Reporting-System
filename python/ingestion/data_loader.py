"""
python/ingestion/data_loader.py
---------------------------------
Stage 1 of the BI pipeline: CSV ingestion with validation.

Reads raw sales_operations.csv, validates schema and data quality,
then bulk-inserts into dbo.stg_sales_raw on MS SQL Server.

Platform : MS SQL Server 2019+ via pyodbc
Author   : Olayinka Somuyiwa

Fix (2026-04-26):
    _validate() now explicitly casts all columns to Python-native types
    (int, float, str) before bulk insert so pyodbc never encounters numpy
    scalars or ambiguous strings for DATE/DECIMAL SQL Server columns.
"""

import os
import logging
import datetime
import pandas as pd
import numpy as np
from typing import Tuple

from python.utils.db_connector import DBConnector
from python.utils.config_loader import load_config

logger = logging.getLogger(__name__)

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
    """Handles CSV ingestion, validation, and staging table load into MS SQL Server."""

    def __init__(self, config: dict = None):
        self.config   = config or load_config()
        self.db       = DBConnector(self.config["database"])
        self.raw_path = self.config["data"]["raw_path"]

    def run(self) -> Tuple[int, int]:
        """Execute the ingestion pipeline. Returns (rows_loaded, rows_rejected)."""
        logger.info("=" * 60)
        logger.info("STAGE 1: Data Ingestion → MS SQL Server")
        logger.info("=" * 60)

        df_raw = self._read_csv()
        df_clean, df_rejected = self._validate(df_raw)
        rows_loaded = self._load_to_staging(df_clean)

        logger.info(
            f"Ingestion complete — loaded: {rows_loaded:,}, "
            f"rejected: {len(df_rejected):,}"
        )
        return rows_loaded, len(df_rejected)

    # ── Private methods ──────────────────────────────────────────────────────

    def _read_csv(self) -> pd.DataFrame:
        """Read the raw CSV. All columns loaded as strings initially."""
        if not os.path.exists(self.raw_path):
            raise FileNotFoundError(
                f"Raw data file not found: {self.raw_path}\n"
                "Run generate_dataset.py first to create the dataset."
            )
        logger.info(f"Reading CSV: {self.raw_path}")
        df = pd.read_csv(self.raw_path, dtype=str, low_memory=False)
        logger.info(f"  → {len(df):,} rows, {len(df.columns)} columns loaded")
        return df

    def _validate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Validate schema, clean data, and coerce all columns to Python-native
        types compatible with SQL Server / pyodbc bulk insert.

        Returns (df_clean, df_rejected).
        """
        missing_cols = set(REQUIRED_COLUMNS.keys()) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        df = df.copy()

        # ── Numeric coercions ──────────────────────────────────────────────
        for col in ["units_sold", "unit_price", "revenue", "cost", "discount_pct"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # ── Reject masks ───────────────────────────────────────────────────
        reject_mask = (
            df["transaction_id"].isna()
            | (df["transaction_id"].str.strip() == "")
            | df["date"].isna()
            | df["revenue"].isna()
            | (df["revenue"] < 0)
        )

        df_rejected = df[reject_mask].copy()
        df_clean    = df[~reject_mask].copy()

        # ── Auto-fix non-critical issues ───────────────────────────────────
        df_clean["discount_pct"] = df_clean["discount_pct"].clip(0, 100).fillna(0)
        df_clean["units_sold"]   = df_clean["units_sold"].fillna(1).clip(lower=1)
        df_clean["cost"]         = df_clean["cost"].fillna(0)

        # ── Standardize categoricals ───────────────────────────────────────
        df_clean["region"]           = df_clean["region"].str.strip().str.title()
        df_clean["channel"]          = df_clean["channel"].str.strip().str.title()
        df_clean["customer_segment"] = df_clean["customer_segment"].str.strip()
        df_clean["product_category"] = df_clean["product_category"].str.strip()

        # ── Rename date → record_date (matches SQL Server column name) ─────
        df_clean = df_clean.rename(columns={"date": "record_date"})

        # ── CRITICAL: explicit dtype coercion for pyodbc compatibility ─────
        # record_date: string "YYYY-MM-DD" → datetime.date object
        # SQL Server DATE column requires a date object, not a string.
        df_clean["record_date"] = pd.to_datetime(
            df_clean["record_date"], errors="coerce"
        ).dt.date  # produces Python datetime.date objects — accepted by SQL Server DATE

        # Drop any rows where date parse failed
        df_clean = df_clean[df_clean["record_date"].notna()].copy()

        # units_sold → Python int (SQL Server INT)
        df_clean["units_sold"] = df_clean["units_sold"].astype(int)

        # Decimal columns → Python float rounded to 2dp (SQL Server DECIMAL)
        for col in ["unit_price", "revenue", "cost", "discount_pct"]:
            df_clean[col] = df_clean[col].astype(float).round(2)

        # String columns → Python str (SQL Server NVARCHAR)
        str_cols = [
            "transaction_id", "product_name", "product_category",
            "region", "sales_rep", "customer_id", "customer_segment", "channel",
        ]
        for col in str_cols:
            df_clean[col] = df_clean[col].astype(str).str.strip()

        # source_system and loaded_at
        df_clean["source_system"] = "CSV_LOAD"
        df_clean["loaded_at"]     = datetime.datetime.now()   # datetime object, not string

        logger.info(
            f"  Clean rows: {len(df_clean):,} | Rejected: {len(df_rejected):,}"
        )
        return df_clean, df_rejected

    def _load_to_staging(self, df: pd.DataFrame) -> int:
        """Clear dbo.stg_sales_raw and bulk-insert the clean DataFrame."""
        logger.info("Loading data into dbo.stg_sales_raw (MS SQL Server)")

        # Column order must match the INSERT target exactly
        insert_cols = [
            "transaction_id", "record_date", "product_name", "product_category",
            "region", "sales_rep", "customer_id", "customer_segment", "channel",
            "units_sold", "unit_price", "revenue", "cost", "discount_pct",
            "source_system", "loaded_at",
        ]

        self.db.connect()

        # Clear staging table
        self.db._conn.cursor().execute("DELETE FROM dbo.stg_sales_raw")
        self.db._conn.commit()
        logger.debug("Cleared existing staging data")

        rows = self.db.bulk_insert_df(
            df=df[insert_cols],
            table_name="stg_sales_raw",
            schema="dbo",
            chunk_size=2000,
        )
        self.db.disconnect()
        return rows


if __name__ == "__main__":
    from python.utils.logger import setup_logger
    setup_logger()
    loader = DataLoader()
    loader.run()
