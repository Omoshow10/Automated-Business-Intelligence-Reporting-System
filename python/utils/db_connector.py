"""
python/utils/db_connector.py
----------------------------
Database connection manager supporting SQLite (default) and SQL Server.
All pipeline modules import this for consistent connection handling.
"""

import sqlite3
import os
import logging
from contextlib import contextmanager
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


class DBConnector:
    """
    Lightweight database connection manager.

    Supports:
        - SQLite  (default, zero-config local dev)
        - SQL Server via pyodbc (set engine: sqlserver in config.yaml)
    """

    def __init__(self, config: dict):
        self.engine = config.get("engine", "sqlite").lower()
        self.db_path = config.get("path", "data/pipeline.db")
        self.server  = config.get("server", "")
        self.database = config.get("database", "BIReportingDB")
        self.username = config.get("username", "")
        self.password = config.get("password", "")
        self._conn: Optional[sqlite3.Connection] = None

    # ── Connection ──────────────────────────────────────────────────────────

    def connect(self) -> sqlite3.Connection:
        """Open and return the database connection."""
        if self.engine == "sqlite":
            os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
            # Performance pragmas for bulk loads
            self._conn.execute("PRAGMA journal_mode = WAL;")
            self._conn.execute("PRAGMA synchronous  = NORMAL;")
            self._conn.execute("PRAGMA cache_size   = -64000;")  # 64 MB cache
            logger.info(f"Connected to SQLite: {self.db_path}")
            return self._conn

        elif self.engine in ("sqlserver", "mssql"):
            try:
                import pyodbc
            except ImportError:
                raise ImportError("Install pyodbc for SQL Server support: pip install pyodbc")
            conn_str = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
                "Encrypt=yes;TrustServerCertificate=yes;"
            )
            self._conn = pyodbc.connect(conn_str)
            logger.info(f"Connected to SQL Server: {self.server}/{self.database}")
            return self._conn

        else:
            raise ValueError(f"Unsupported engine: {self.engine}. Use 'sqlite' or 'sqlserver'.")

    def disconnect(self):
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("Database connection closed.")

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
        """Execute a single SQL statement (DDL or DML)."""
        if not self._conn:
            self.connect()
        cursor = self._conn.cursor()
        cursor.execute(sql, params)
        self._conn.commit()

    def execute_script(self, sql_script: str) -> None:
        """Execute a multi-statement SQL script."""
        if not self._conn:
            self.connect()
        if self.engine == "sqlite":
            self._conn.executescript(sql_script)
        else:
            cursor = self._conn.cursor()
            for statement in sql_script.split(";"):
                stmt = statement.strip()
                if stmt:
                    cursor.execute(stmt)
            self._conn.commit()

    def execute_file(self, filepath: str) -> None:
        """Read and execute a .sql file."""
        logger.info(f"Executing SQL file: {filepath}")
        with open(filepath, "r") as f:
            sql = f.read()
        self.execute_script(sql)
        logger.info(f"✅ Completed: {filepath}")

    def query_df(self, sql: str, params: tuple = ()) -> pd.DataFrame:
        """Execute a SELECT and return results as a DataFrame."""
        if not self._conn:
            self.connect()
        return pd.read_sql_query(sql, self._conn, params=params)

    def bulk_insert_df(self, df: pd.DataFrame, table_name: str,
                       if_exists: str = "append", chunk_size: int = 1000) -> int:
        """
        Bulk insert a DataFrame into a database table.

        Args:
            df:          DataFrame to insert
            table_name:  Target table name
            if_exists:   'append' | 'replace' | 'fail'
            chunk_size:  Rows per batch

        Returns:
            Number of rows inserted
        """
        if not self._conn:
            self.connect()
        df.to_sql(
            name=table_name,
            con=self._conn,
            if_exists=if_exists,
            index=False,
            chunksize=chunk_size,
            method="multi",
        )
        rows = len(df)
        logger.info(f"Inserted {rows:,} rows into {table_name}")
        return rows

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        if not self._conn:
            self.connect()
        if self.engine == "sqlite":
            result = self.query_df(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                params=(table_name,)
            )
        else:
            result = self.query_df(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME=?",
                params=(table_name,)
            )
        return len(result) > 0

    def get_row_count(self, table_name: str) -> int:
        """Return the number of rows in a table."""
        result = self.query_df(f"SELECT COUNT(*) AS cnt FROM {table_name}")
        return int(result["cnt"].iloc[0])
