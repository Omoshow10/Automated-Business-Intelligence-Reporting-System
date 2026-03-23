"""
python/transformation/sql_runner.py
------------------------------------
Stage 2: Executes the SQL transformation layers in order.

Layer execution order:
    01_create_schema.sql       → DDL setup
    02_staging_layer.sql       → Staging clean & validate
    03_core_layer.sql          → Core enrichment
    04_reporting_layer.sql     → Reporting aggregation
    sql/views/*.sql            → Create analytical views

Usage:
    python -m python.transformation.sql_runner
"""

import os
import time
import logging
from typing import List

from python.utils.db_connector import DBConnector
from python.utils.config_loader import load_config

logger = logging.getLogger(__name__)


class SQLRunner:
    """Executes SQL transformation pipeline in sequence."""

    def __init__(self, config: dict = None):
        self.config  = config or load_config()
        self.db      = DBConnector(self.config["database"])
        self.sql_cfg = self.config["sql"]

    # ── Public Interface ─────────────────────────────────────────────────────

    def run(self) -> bool:
        """
        Execute all transformation layers.

        Returns:
            True on success, raises on failure.
        """
        logger.info("=" * 60)
        logger.info("STAGE 2: SQL Transformations")
        logger.info("=" * 60)

        self.db.connect()
        try:
            files = self._get_sql_files()
            for filepath in files:
                self._execute_file(filepath)
            logger.info("✅ All SQL transformations completed successfully.")
            return True
        finally:
            self.db.disconnect()

    def run_single(self, filepath: str) -> None:
        """Execute a single SQL file."""
        self.db.connect()
        try:
            self._execute_file(filepath)
        finally:
            self.db.disconnect()

    def get_row_counts(self) -> dict:
        """Return row counts for all core tables after transformation."""
        self.db.connect()
        tables = [
            "stg_sales_raw",
            "core_sales",
            "rpt_monthly_revenue",
            "rpt_regional_summary",
            "rpt_product_summary",
            "rpt_anomalies",
        ]
        counts = {}
        for t in tables:
            try:
                counts[t] = self.db.get_row_count(t)
            except Exception:
                counts[t] = -1  # Table may not exist yet
        self.db.disconnect()
        return counts

    # ── Private Methods ──────────────────────────────────────────────────────

    def _get_sql_files(self) -> List[str]:
        """Return ordered list of SQL files to execute."""
        ordered_files = [
            self.sql_cfg["schema_file"],
            self.sql_cfg["staging_file"],
            self.sql_cfg["core_file"],
            self.sql_cfg["reporting_file"],
        ]

        # Add all view files sorted alphabetically
        views_dir = self.sql_cfg.get("views_dir", "sql/views/")
        if os.path.isdir(views_dir):
            view_files = sorted([
                os.path.join(views_dir, f)
                for f in os.listdir(views_dir)
                if f.endswith(".sql")
            ])
            ordered_files.extend(view_files)

        # Validate all files exist
        missing = [f for f in ordered_files if not os.path.exists(f)]
        if missing:
            raise FileNotFoundError(f"SQL files not found: {missing}")

        logger.info(f"SQL execution plan: {len(ordered_files)} files")
        for i, f in enumerate(ordered_files, 1):
            logger.debug(f"  {i:02d}. {f}")

        return ordered_files

    def _execute_file(self, filepath: str) -> None:
        """Execute a single SQL file and log timing."""
        filename = os.path.basename(filepath)
        logger.info(f"  ▶ Executing: {filename}")
        start = time.time()

        with open(filepath, "r") as f:
            sql_content = f.read()

        # Split by semicolons, skip blank/comment-only statements
        statements = [
            s.strip()
            for s in sql_content.split(";")
            if s.strip() and not self._is_comment_only(s.strip())
        ]

        cursor = self.db._conn.cursor()
        for stmt in statements:
            try:
                cursor.execute(stmt)
            except Exception as e:
                # Log problem statement for debugging
                preview = stmt[:200].replace("\n", " ")
                logger.error(f"SQL error in {filename}: {e}")
                logger.debug(f"  Failed statement: {preview}...")
                raise

        self.db._conn.commit()
        elapsed = time.time() - start
        logger.info(f"  ✅ {filename} completed in {elapsed:.2f}s")

    @staticmethod
    def _is_comment_only(sql: str) -> bool:
        """Return True if a SQL block contains only comments."""
        lines = [
            line.strip()
            for line in sql.splitlines()
            if line.strip() and not line.strip().startswith("--")
        ]
        return len(lines) == 0


if __name__ == "__main__":
    from python.utils.logger import setup_logger
    setup_logger()
    runner = SQLRunner()
    runner.run()

    counts = runner.get_row_counts()
    print("\nRow counts after transformation:")
    for table, count in counts.items():
        print(f"  {table:<28} {count:>8,} rows")
