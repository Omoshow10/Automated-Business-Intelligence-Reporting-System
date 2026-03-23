"""
tests/test_anomaly_detection.py
--------------------------------
Unit tests for the AnomalyDetector module.
Run: pytest tests/test_anomaly_detection.py -v
"""

import pytest
import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from python.reporting.anomaly_detector import AnomalyDetector


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_config():
    return {
        "database":  {"engine": "sqlite", "path": ":memory:"},
        "data":      {"raw_path": "data/raw/sales_operations.csv", "processed_path": "data/processed/"},
        "anomaly":   {"zscore_threshold": 2.5, "iqr_multiplier": 1.5, "columns": ["revenue", "cost"]},
        "reporting": {"output_format": "xlsx", "output_path": "data/processed/", "include_charts": True},
        "sql":       {},
    }


@pytest.fixture
def detector(sample_config):
    return AnomalyDetector(sample_config)


@pytest.fixture
def normal_df():
    """100 rows of normally distributed revenue, no intentional outliers."""
    np.random.seed(42)
    n = 100
    base_revenue = np.random.normal(loc=5000, scale=500, size=n)
    base_cost    = base_revenue * np.random.uniform(0.3, 0.6, size=n)
    return pd.DataFrame({
        "transaction_id":   [f"TXN-{i:06d}" for i in range(n)],
        "txn_date":         ["2023-06-15"] * n,
        "product_name":     ["Laptop Pro 15"] * n,
        "product_category": ["Electronics"] * n,
        "region":           ["North"] * n,
        "revenue":          np.clip(base_revenue, 100, None).round(2),
        "cost":             np.clip(base_cost,    50,  None).round(2),
        "discount_pct":     np.random.uniform(0, 20, size=n).round(2),
    })


@pytest.fixture
def df_with_outliers(normal_df):
    """Add 3 obvious high-revenue outliers."""
    df = normal_df.copy()
    # 3 transactions with revenue ~10x the mean → should be flagged
    df.loc[0, "revenue"] = 50000.0
    df.loc[1, "revenue"] = 55000.0
    df.loc[2, "revenue"] = 60000.0
    # 1 near-zero revenue → should also be flagged
    df.loc[3, "revenue"] = 5.0
    return df


@pytest.fixture
def df_with_discount_outliers(normal_df):
    """Add extreme discount values for IQR test."""
    df = normal_df.copy()
    df.loc[0, "discount_pct"] = 99.0   # Extreme high
    df.loc[1, "discount_pct"] = 0.0    # Normal
    return df


# ── Tests: Z-Score Detection ──────────────────────────────────────────────────

class TestZScoreDetection:
    def test_no_anomalies_in_normal_data(self, detector, normal_df):
        anomalies = detector._detect_zscore(normal_df, "revenue")
        # With 100 normal samples, expect very few or no anomalies
        assert len(anomalies) <= 3, \
            f"Expected few anomalies in normal data, got {len(anomalies)}"

    def test_obvious_outliers_detected(self, detector, df_with_outliers):
        anomalies = detector._detect_zscore(df_with_outliers, "revenue")
        assert len(anomalies) >= 3, \
            f"Expected at least 3 flagged outliers, got {len(anomalies)}"

    def test_high_revenue_flagged_as_high(self, detector, df_with_outliers):
        anomalies = detector._detect_zscore(df_with_outliers, "revenue")
        high_flags = anomalies[anomalies["anomaly_type"] == "HIGH_REVENUE"]
        assert len(high_flags) >= 1

    def test_low_revenue_flagged_as_low(self, detector, df_with_outliers):
        anomalies = detector._detect_zscore(df_with_outliers, "revenue")
        low_flags = anomalies[anomalies["anomaly_type"] == "LOW_REVENUE"]
        assert len(low_flags) >= 1

    def test_returns_empty_df_when_no_outliers(self, detector):
        """Identical revenue values → zero std dev → no anomalies."""
        df = pd.DataFrame({
            "transaction_id":   [f"TXN-{i:06d}" for i in range(10)],
            "txn_date":         ["2023-01-01"] * 10,
            "product_name":     ["Widget"] * 10,
            "product_category": ["Electronics"] * 10,
            "region":           ["North"] * 10,
            "revenue":          [1000.0] * 10,   # All identical
            "cost":             [500.0] * 10,
            "discount_pct":     [0.0] * 10,
        })
        anomalies = detector._detect_zscore(df, "revenue")
        assert len(anomalies) == 0

    def test_anomaly_contains_required_columns(self, detector, df_with_outliers):
        anomalies = detector._detect_zscore(df_with_outliers, "revenue")
        required = {"transaction_id", "txn_date", "product_name", "region",
                    "revenue", "cost", "revenue_zscore", "cost_zscore", "anomaly_type"}
        assert required.issubset(set(anomalies.columns))

    def test_custom_threshold_respected(self, sample_config, df_with_outliers):
        config = sample_config.copy()
        config["anomaly"] = dict(sample_config["anomaly"], zscore_threshold=1.0)
        detector_low = AnomalyDetector(config)
        anomalies_low = detector_low._detect_zscore(df_with_outliers, "revenue")

        config_high = sample_config.copy()
        config_high["anomaly"] = dict(sample_config["anomaly"], zscore_threshold=5.0)
        detector_high = AnomalyDetector(config_high)
        anomalies_high = detector_high._detect_zscore(df_with_outliers, "revenue")

        assert len(anomalies_low) >= len(anomalies_high), \
            "Lower threshold should flag more or equal anomalies"


# ── Tests: IQR Detection ──────────────────────────────────────────────────────

class TestIQRDetection:
    def test_discount_outliers_detected(self, detector, df_with_discount_outliers):
        anomalies = detector._detect_iqr(df_with_discount_outliers, "discount_pct")
        assert len(anomalies) >= 1

    def test_iqr_anomaly_type_label(self, detector, df_with_discount_outliers):
        anomalies = detector._detect_iqr(df_with_discount_outliers, "discount_pct")
        if len(anomalies) > 0:
            assert (anomalies["anomaly_type"] == "DISCOUNT_OUTLIER").all()

    def test_iqr_returns_empty_for_uniform_data(self, detector):
        df = pd.DataFrame({
            "transaction_id":   [f"TXN-{i}" for i in range(20)],
            "txn_date":         ["2023-01-01"] * 20,
            "product_name":     ["Widget"] * 20,
            "product_category": ["Electronics"] * 20,
            "region":           ["North"] * 20,
            "revenue":          [1000.0] * 20,
            "cost":             [500.0] * 20,
            "discount_pct":     [10.0] * 20,    # All identical → no IQR outliers
        })
        anomalies = detector._detect_iqr(df, "discount_pct")
        assert len(anomalies) == 0


# ── Tests: Statistical Properties ────────────────────────────────────────────

class TestStatisticalProperties:
    def test_zscore_threshold_is_configurable(self, sample_config):
        for threshold in [1.5, 2.0, 2.5, 3.0]:
            config = dict(sample_config)
            config["anomaly"] = dict(sample_config["anomaly"], zscore_threshold=threshold)
            d = AnomalyDetector(config)
            assert d.threshold == threshold

    def test_iqr_multiplier_is_configurable(self, sample_config):
        config = dict(sample_config)
        config["anomaly"] = dict(sample_config["anomaly"], iqr_multiplier=2.0)
        d = AnomalyDetector(config)
        assert d.iqr_mult == 2.0

    def test_anomaly_rate_in_synthetic_data(self, detector, df_with_outliers):
        """Anomaly rate should be low — we injected exactly 4 outliers in 100 rows."""
        all_anomalies = pd.concat([
            detector._detect_zscore(df_with_outliers, "revenue"),
            detector._detect_zscore(df_with_outliers, "cost"),
        ])
        all_anomalies = all_anomalies.drop_duplicates("transaction_id")
        rate = len(all_anomalies) / len(df_with_outliers)
        assert rate < 0.20, f"Anomaly rate {rate:.1%} seems too high for this dataset"

    def test_no_anomaly_id_duplicates_after_dedup(self, detector, df_with_outliers):
        """After deduplication, each transaction_id should appear once."""
        z_anom = detector._detect_zscore(df_with_outliers, "revenue")
        if len(z_anom) > 0:
            assert z_anom["transaction_id"].nunique() == len(z_anom)


# ── Tests: Edge Cases ────────────────────────────────────────────────────────

class TestEdgeCases:
    def test_single_row_dataframe(self, detector):
        """Single-row dataset: no std dev, should not crash."""
        df = pd.DataFrame({
            "transaction_id":   ["TXN-000001"],
            "txn_date":         ["2023-01-01"],
            "product_name":     ["Widget"],
            "product_category": ["Electronics"],
            "region":           ["North"],
            "revenue":          [1000.0],
            "cost":             [500.0],
            "discount_pct":     [10.0],
        })
        try:
            result = detector._detect_zscore(df, "revenue")
            # Either empty result or single result — must not crash
            assert isinstance(result, pd.DataFrame)
        except Exception as e:
            pytest.fail(f"Single-row DataFrame caused unexpected error: {e}")

    def test_all_zero_revenue(self, detector):
        """Zero revenue across all rows: Z-score undefined, must not crash."""
        df = pd.DataFrame({
            "transaction_id":   [f"TXN-{i}" for i in range(10)],
            "txn_date":         ["2023-01-01"] * 10,
            "product_name":     ["Widget"] * 10,
            "product_category": ["Electronics"] * 10,
            "region":           ["North"] * 10,
            "revenue":          [0.0] * 10,
            "cost":             [0.0] * 10,
            "discount_pct":     [0.0] * 10,
        })
        try:
            result = detector._detect_zscore(df, "revenue")
            assert isinstance(result, pd.DataFrame)
        except Exception as e:
            pytest.fail(f"All-zero revenue caused unexpected error: {e}")

    def test_large_dataset_performance(self, detector):
        """10,000 rows should complete Z-score detection without timeout."""
        import time
        np.random.seed(99)
        n = 10_000
        df = pd.DataFrame({
            "transaction_id":   [f"TXN-{i:07d}" for i in range(n)],
            "txn_date":         ["2023-06-01"] * n,
            "product_name":     ["Widget"] * n,
            "product_category": np.random.choice(["Electronics", "Software", "Hardware", "Services"], n),
            "region":           np.random.choice(["North", "South", "East", "West"], n),
            "revenue":          np.random.normal(5000, 800, n).clip(100),
            "cost":             np.random.normal(2000, 400, n).clip(50),
            "discount_pct":     np.random.uniform(0, 30, n),
        })
        start = time.time()
        result = detector._detect_zscore(df, "revenue")
        elapsed = time.time() - start
        assert elapsed < 5.0, f"Detection took {elapsed:.2f}s on 10k rows — too slow"
        assert isinstance(result, pd.DataFrame)
