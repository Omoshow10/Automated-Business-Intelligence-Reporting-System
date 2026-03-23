"""
tests/test_ingestion.py
------------------------
Unit tests for the data ingestion module.
Run: pytest tests/test_ingestion.py -v
"""

import pytest
import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from python.ingestion.data_loader import DataLoader, REQUIRED_COLUMNS, VALID_REGIONS


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_config():
    return {
        "database": {"engine": "sqlite", "path": ":memory:"},
        "data": {
            "raw_path": "data/raw/sales_operations.csv",
            "processed_path": "data/processed/",
        },
        "anomaly": {"zscore_threshold": 2.5, "iqr_multiplier": 1.5, "columns": ["revenue", "cost"]},
        "reporting": {"output_format": "xlsx", "output_path": "data/processed/", "include_charts": True},
        "sql": {
            "schema_file":    "sql/transformations/01_create_schema.sql",
            "staging_file":   "sql/transformations/02_staging_layer.sql",
            "core_file":      "sql/transformations/03_core_layer.sql",
            "reporting_file": "sql/transformations/04_reporting_layer.sql",
            "views_dir":      "sql/views/",
        },
    }


@pytest.fixture
def valid_df():
    """Minimal valid DataFrame matching required schema."""
    return pd.DataFrame({
        "transaction_id":   ["TXN-000001", "TXN-000002", "TXN-000003"],
        "date":             ["2023-01-15",  "2023-06-30",  "2024-03-10"],
        "product_name":     ["Laptop Pro 15", "CRM Suite – Annual", "Server Rack Unit"],
        "product_category": ["Electronics",   "Software",           "Hardware"],
        "region":           ["North",         "East",               "West"],
        "sales_rep":        ["Alex Johnson",  "Maria Garcia",       "David Chen"],
        "customer_id":      ["CUST-1001",     "CUST-2002",          "CUST-3003"],
        "customer_segment": ["Enterprise",    "SMB",                "Enterprise"],
        "channel":          ["Direct",        "Partner",            "Online"],
        "units_sold":       [1, 2, 3],
        "unit_price":       [1200.0, 2400.0, 5500.0],
        "revenue":          [1200.0, 4800.0, 16500.0],
        "cost":             [700.0,  600.0,  9600.0],
        "discount_pct":     [0.0,    0.0,    0.0],
    })


@pytest.fixture
def dirty_df(valid_df):
    """DataFrame with deliberate data quality issues."""
    dirty = valid_df.copy()
    dirty.loc[len(dirty)] = {
        "transaction_id": None,          # Null ID → must be rejected
        "date": "2023-05-01",
        "product_name": "Widget",
        "product_category": "Electronics",
        "region": "North",
        "sales_rep": "Test Rep",
        "customer_id": "CUST-9999",
        "customer_segment": "SMB",
        "channel": "Direct",
        "units_sold": 1,
        "unit_price": 100.0,
        "revenue": 100.0,
        "cost": 50.0,
        "discount_pct": 0.0,
    }
    dirty.loc[len(dirty)] = {
        "transaction_id": "TXN-BAD001",
        "date": "2023-05-02",
        "product_name": "Widget",
        "product_category": "Electronics",
        "region": "North",
        "sales_rep": "Test Rep",
        "customer_id": "CUST-9998",
        "customer_segment": "SMB",
        "channel": "Direct",
        "units_sold": 1,
        "unit_price": 100.0,
        "revenue": -500.0,              # Negative revenue → must be rejected
        "cost": 50.0,
        "discount_pct": 0.0,
    }
    return dirty


# ── Tests: Required Columns ───────────────────────────────────────────────────

class TestRequiredColumns:
    def test_all_required_columns_defined(self):
        """REQUIRED_COLUMNS must contain at least the core financial fields."""
        assert "transaction_id" in REQUIRED_COLUMNS
        assert "revenue"        in REQUIRED_COLUMNS
        assert "cost"           in REQUIRED_COLUMNS
        assert "date"           in REQUIRED_COLUMNS

    def test_required_columns_count(self):
        """Should have at least 14 required columns."""
        assert len(REQUIRED_COLUMNS) >= 14


# ── Tests: Validation Logic ───────────────────────────────────────────────────

class TestValidation:
    def test_valid_df_passes_validation(self, sample_config, valid_df):
        loader = DataLoader(sample_config)
        clean, rejected = loader._validate(valid_df)
        assert len(clean) == len(valid_df)
        assert len(rejected) == 0

    def test_null_transaction_id_rejected(self, sample_config, dirty_df):
        loader = DataLoader(sample_config)
        clean, rejected = loader._validate(dirty_df)
        rejected_ids = rejected["transaction_id"].tolist()
        assert any(pd.isna(r) or r is None for r in rejected_ids), \
            "Row with null transaction_id must be in rejected set"

    def test_negative_revenue_rejected(self, sample_config, dirty_df):
        loader = DataLoader(sample_config)
        clean, rejected = loader._validate(dirty_df)
        assert len(rejected) >= 1, "At least one row should be rejected"
        if len(rejected) > 0:
            assert all(clean["revenue"] >= 0), "Clean set must not contain negative revenue"

    def test_discount_clamped(self, sample_config, valid_df):
        df = valid_df.copy()
        df.loc[0, "discount_pct"] = 150.0   # Over 100%
        df.loc[1, "discount_pct"] = -10.0   # Negative
        loader = DataLoader(sample_config)
        clean, _ = loader._validate(df)
        assert clean["discount_pct"].max() <= 100.0
        assert clean["discount_pct"].min() >= 0.0

    def test_units_sold_filled(self, sample_config, valid_df):
        df = valid_df.copy()
        df.loc[0, "units_sold"] = None
        loader = DataLoader(sample_config)
        clean, _ = loader._validate(df)
        assert clean["units_sold"].min() >= 1

    def test_region_standardized(self, sample_config, valid_df):
        df = valid_df.copy()
        df.loc[0, "region"] = "north"   # lowercase
        df.loc[1, "region"] = "EAST"    # uppercase
        loader = DataLoader(sample_config)
        clean, _ = loader._validate(df)
        for region in clean["region"]:
            assert region in VALID_REGIONS, f"Region '{region}' not in valid set"

    def test_channel_standardized(self, sample_config, valid_df):
        df = valid_df.copy()
        df.loc[0, "channel"] = "direct"   # lowercase
        loader = DataLoader(sample_config)
        clean, _ = loader._validate(df)
        for ch in clean["channel"]:
            assert ch[0].isupper(), f"Channel '{ch}' should be title-cased"

    def test_missing_required_column_raises(self, sample_config, valid_df):
        df = valid_df.drop(columns=["revenue"])
        loader = DataLoader(sample_config)
        with pytest.raises(ValueError, match="Missing required columns"):
            loader._validate(df)

    def test_loaded_at_timestamp_added(self, sample_config, valid_df):
        loader = DataLoader(sample_config)
        clean, _ = loader._validate(valid_df)
        assert "loaded_at" in clean.columns
        assert clean["loaded_at"].notna().all()

    def test_clean_df_has_expected_dtypes(self, sample_config, valid_df):
        loader = DataLoader(sample_config)
        clean, _ = loader._validate(valid_df)
        assert pd.api.types.is_numeric_dtype(clean["revenue"])
        assert pd.api.types.is_numeric_dtype(clean["cost"])
        assert pd.api.types.is_numeric_dtype(clean["discount_pct"])


# ── Tests: Derived Metrics ────────────────────────────────────────────────────

class TestDerivedMetrics:
    def test_revenue_always_non_negative_in_clean(self, sample_config, valid_df):
        loader = DataLoader(sample_config)
        clean, _ = loader._validate(valid_df)
        assert (clean["revenue"] >= 0).all()

    def test_discount_pct_in_range(self, sample_config, valid_df):
        loader = DataLoader(sample_config)
        clean, _ = loader._validate(valid_df)
        assert clean["discount_pct"].between(0, 100).all()


# ── Tests: File Not Found ─────────────────────────────────────────────────────

class TestFileHandling:
    def test_missing_csv_raises_file_not_found(self, sample_config):
        config = sample_config.copy()
        config["data"] = {"raw_path": "data/raw/nonexistent_file.csv", "processed_path": "data/processed/"}
        loader = DataLoader(config)
        with pytest.raises(FileNotFoundError):
            loader._read_csv()
