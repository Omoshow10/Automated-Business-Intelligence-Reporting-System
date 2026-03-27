"""
python/utils/db_connector.py
------------------------------
MS SQL Server connection manager using pyodbc.
All pipeline modules import this for consistent connection handling.

Platform : MS SQL Server 2019+ (T-SQL)
Author   : Olayinka Somuyiwa
"""

import pyodbc
import logging
from contextlib import contextmanager
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


class DBConnector:
    """
    MS SQL Server database connection manager via pyodbc.

    Supports both Windows Authentication (Trusted_Connection)
    and SQL Server Authentication (username/password).
    """

    def __init__(self, config: dict):
        self.server      = config.get("server",   "your_server")
        self.database    = config.get("database", "bi_reporting_db")
        self.trusted     = config.get("trusted_connection", True)
        self.username    = config.get("username", "")
        self.password    = config.get("password", "")
        self.driver      = config.get("driver",   "SQL Server")
        self._conn: Optional[pyodbc.Connection] = None

    # ── Connection ──────────────────────────────────────────────────────────

    def connect(self) -> pyodbc.Connection:
        """Open and return the SQL Server connection."""
        if self.trusted:
            conn_str = (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                "Trusted_Connection=yes;"
            )
        else:
            conn_str = (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
                "Encrypt=yes;TrustServerCertificate=yes;"
            )

        self._conn = pyodbc.connect(conn_str)
        self._conn.autocommit = False
        logger.info(f"Connected to SQL Server: {self.server}/{self.database}")
        return self._conn

    def disconnect(self):
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("SQL Server connection closed.")

    @contextmanager
    def connection(self):
        """Context manager for safe connection handling."""
        conn = self.connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.disconnect()

    # ── Query Helpers ────────────────────────────────────────────────────────

    def execute_sql(self, sql: str, params: tuple = ()) -> None:
        """Execute a single T-SQL statement."""
        if not self._conn:
            self.connect()
        cursor = self._conn.cursor()
        cursor.execute(sql, params)
        self._conn.commit()

    def execute_procedure(self, proc_name: str, params: tuple = ()) -> None:
        """Execute a stored procedure by name."""
        if not self._conn:
            self.connect()
        cursor = self._conn.cursor()
        placeholders = ", ".join(["?" for _ in params])
        sql = f"EXEC {proc_name} {placeholders}" if params else f"EXEC {proc_name}"
        cursor.execute(sql, params)
        self._conn.commit()
        logger.info(f"Procedure executed: {proc_name}")

    def execute_file(self, filepath: str) -> None:
        """Read and execute a .sql file (splits on GO batches)."""
        logger.info(f"Executing SQL file: {filepath}")
        with open(filepath, "r") as f:
            sql = f.read()

        # Split on GO statements (T-SQL batch separator)
        batches = [b.strip() for b in sql.split("\nGO") if b.strip()]
        cursor  = self._conn.cursor()
        for batch in batches:
            if batch and not batch.startswith("--"):
                cursor.execute(batch)
        self._conn.commit()
        logger.info(f"✅ Completed: {filepath}")

    def query_df(self, sql: str, params: tuple = ()) -> pd.DataFrame:
        """Execute a SELECT and return results as a DataFrame."""
        if not self._conn:
            self.connect()
        return pd.read_sql(sql, self._conn, params=params)

    def bulk_insert_df(self, df: pd.DataFrame, table_name: str,
                       schema: str = "dbo", chunk_size: int = 1000) -> int:
        """
        Bulk insert a DataFrame into a SQL Server table using fast_executemany.

        Args:
            df:          DataFrame to insert
            table_name:  Target table name
            schema:      Schema name (default: dbo)
            chunk_size:  Rows per batch

        Returns:
            Number of rows inserted
        """
        if not self._conn:
            self.connect()

        cursor = self._conn.cursor()
        cursor.fast_executemany = True

        cols        = ", ".join([f"[{c}]" for c in df.columns])
        placeholders = ", ".join(["?" for _ in df.columns])
        sql = f"INSERT INTO [{schema}].[{table_name}] ({cols}) VALUES ({placeholders})"

        data = [tuple(row) for row in df.itertuples(index=False, name=None)]

        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            cursor.executemany(sql, chunk)
            self._conn.commit()

        rows = len(df)
        logger.info(f"Inserted {rows:,} rows into [{schema}].[{table_name}]")
        return rows

    def get_row_count(self, table_name: str, schema: str = "dbo") -> int:
        """Return the number of rows in a table."""
        result = self.query_df(
            f"SELECT COUNT(*) AS cnt FROM [{schema}].[{table_name}]"
        )
        return int(result["cnt"].iloc[0])

    def table_exists(self, table_name: str, schema: str = "dbo") -> bool:
        """Check if a table exists in the database."""
        result = self.query_df(
            "SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            params=(schema, table_name)
        )
        return int(result["cnt"].iloc[0]) > 0
