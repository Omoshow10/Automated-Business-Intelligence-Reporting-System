"""
python/utils/config_loader.py
------------------------------
Loads and validates config/config.yaml with sensible defaults.
"""

import os
import yaml
import logging

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = {
    "database": {
        "engine": "sqlite",
        "path": "data/pipeline.db",
    },
    "pipeline": {
        "ingest": True,
        "transform": True,
        "report": True,
    },
    "data": {
        "raw_path": "data/raw/sales_operations.csv",
        "processed_path": "data/processed/",
    },
    "reporting": {
        "output_format": "xlsx",
        "output_path": "data/processed/",
        "include_charts": True,
    },
    "anomaly": {
        "zscore_threshold": 2.5,
        "iqr_multiplier": 1.5,
        "columns": ["revenue", "cost"],
    },
    "sql": {
        "schema_file":        "sql/transformations/01_create_schema.sql",
        "staging_file":       "sql/transformations/02_staging_layer.sql",
        "core_file":          "sql/transformations/03_core_layer.sql",
        "reporting_file":     "sql/transformations/04_reporting_layer.sql",
        "views_dir":          "sql/views/",
    },
    "logging": {
        "level": "INFO",
        "log_dir": "logs/",
    },
}


def deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override dict into base dict."""
    merged = base.copy()
    for key, val in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(val, dict):
            merged[key] = deep_merge(merged[key], val)
        else:
            merged[key] = val
    return merged


def load_config(config_path: str = "config/config.yaml") -> dict:
    """
    Load configuration from YAML file, merging with defaults.

    Args:
        config_path: Path to config.yaml

    Returns:
        Merged configuration dictionary
    """
    if not os.path.exists(config_path):
        logger.warning(f"Config file not found at '{config_path}'. Using defaults.")
        return DEFAULT_CONFIG.copy()

    with open(config_path, "r") as f:
        user_config = yaml.safe_load(f) or {}

    config = deep_merge(DEFAULT_CONFIG, user_config)
    logger.info(f"Configuration loaded from: {config_path}")
    return config
