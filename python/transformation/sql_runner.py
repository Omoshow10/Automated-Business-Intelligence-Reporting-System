"""
python/transformation/sql_runner.py
------------------------------------
Stage 2: Executes the SQL transformation layers in order.

Platform : MS SQL Server 2019+ via pyodbc
Author   : Olayinka Somuyiwa

Fixes (2026-04-26):
  - Split on GO batch separators, not semicolons (GO is SSMS-only)
  - 01_create_schema.sql only runs when tables do not yet exist,
    preventing DROP/recreate from wiping already-loaded staging data
  - nextset() loop drains result sets between batches
"""

import os
import re
import time
import logging
from typing import List

from python.utils.db_connector import DBConnector
from python.utils.config_loader import load_config

logger = logging.getLogger(__name__)


class SQLRunner:
    """Executes T-SQL transformation files against MS SQL Server via pyodbc."""

    def __init__(self, config: dict = None):
        self.config  = config or load_config()
        self.db      = DBConnector(self.config["database"])
        self.sql_cfg = self.config["sql"]

    # ── Public Interface ─────────────────────────────────────────────────────

    def run(self) -> bool:
        """Execute all transformation layers. Returns True on success."""
        logger.info("=" * 60)
        logger.info("STAGE 2: SQL Transformations")
        logger.info("=" * 60)

        self.db.connect()
        try:
            files = self._get_sql_files()
            for filepath in files:
                filename = os.path.basename(filepath)

                # Schema script: only run if tables don't exist yet.
                # Re-running it would DROP all tables and wipe staged data.
                if filename == "01_create_schema.sql":
                    if self._schema_exists():
                        logger.info(
                            f"  ⏭  Skipping {filename} "
                            f"— schema already exists"
                        )
                        continue
                    else:
                        logger.info(
                            f"  ▶  Schema not found — running {filename}"
                        )

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

    def force_schema_recreate(self) -> None:
        """
        Drop and recreate schema from scratch.
        Use only when you deliberately want a clean slate.
        WARNING: this wipes all data.
        """
        self.db.connect()
        try:
            schema_file = self.sql_cfg["schema_file"]
            logger.warning("Force-recreating schema — all data will be lost.")
            self._execute_file(schema_file)
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
                cursor = self.db._conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM dbo.[{t}]")
                counts[t] = cursor.fetchone()[0]
            except Exception:
                counts[t] = -1
        self.db.disconnect()
        return counts

    # ── Private Methods ──────────────────────────────────────────────────────

    def _schema_exists(self) -> bool:
        """Return True if dbo.core_sales already exists in the database."""
        try:
            cursor = self.db._conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'core_sales'"
            )
            return cursor.fetchone()[0] > 0
        except Exception:
            return False

    def _get_sql_files(self) -> List[str]:
        """Return ordered list of SQL files to execute."""
        ordered_files = [
            self.sql_cfg["schema_file"],
            self.sql_cfg["staging_file"],
            self.sql_cfg["core_file"],
            self.sql_cfg["reporting_file"],
        ]

        views_dir = self.sql_cfg.get("views_dir", "sql/views/")
        if os.path.isdir(views_dir):
            view_files = sorted([
                os.path.join(views_dir, f)
                for f in os.listdir(views_dir)
                if f.endswith(".sql")
            ])
            ordered_files.extend(view_files)

        missing = [f for f in ordered_files if not os.path.exists(f)]
        if missing:
            raise FileNotFoundError(f"SQL files not found: {missing}")

        logger.info(f"SQL execution plan: {len(ordered_files)} files")
        for i, f in enumerate(ordered_files, 1):
            logger.debug(f"  {i:02d}. {f}")

        return ordered_files

    def _split_batches(self, sql: str) -> List[str]:
        """
        Split a T-SQL script into executable batches by GO separator.

        GO is an SSMS directive — SQL Server itself does not know GO.
        pyodbc must never receive GO as a statement to execute.
        """
        go_pattern = re.compile(r'^\s*GO\s*$', re.IGNORECASE | re.MULTILINE)
        batches = go_pattern.split(sql)
        result  = []
        for batch in batches:
            batch = batch.strip()
            if batch and not self._is_comment_only(batch):
                result.append(batch)
        return result

    def _execute_file(self, filepath: str) -> None:
        """Execute a single SQL file, splitting correctly on GO."""
        filename = os.path.basename(filepath)
        logger.info(f"  ▶ Executing: {filename}")
        start = time.time()

        with open(filepath, "r", encoding="utf-8") as f:
            sql_content = f.read()

        batches = self._split_batches(sql_content)
        cursor  = self.db._conn.cursor()

        for batch in batches:
            try:
                cursor.execute(batch)
                # Drain any result sets so the connection stays clean
                try:
                    while cursor.nextset():
                        pass
                except Exception:
                    pass
            except Exception as e:
                preview = batch[:300].replace("\n", " ")
                logger.error(f"SQL error in {filename}: {e}")
                logger.debug(f"  Failed batch: {preview}")
                self.db._conn.rollback()
                raise

        self.db._conn.commit()
        elapsed = time.time() - start
        logger.info(f"  ✅ {filename} completed in {elapsed:.2f}s")

    @staticmethod
    def _is_comment_only(sql: str) -> bool:
        """Return True if a SQL block is blank or contains only comments."""
        stripped = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        lines = [
            ln.strip() for ln in stripped.splitlines()
            if ln.strip() and not ln.strip().startswith("--")
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
