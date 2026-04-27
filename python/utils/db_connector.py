"""
python/utils/db_connector.py
------------------------------
MS SQL Server connection manager using pyodbc.

Platform : MS SQL Server 2019+ (T-SQL)
Author   : Olayinka Somuyiwa

Fixes (2026-04-26):
  - query_df uses raw pyodbc cursor instead of pd.read_sql to avoid
    the pandas UserWarning about non-SQLAlchemy DBAPI2 connections
  - bulk_insert_df coerces every value to Python native types before
    executemany so pyodbc never raises 'Invalid character value for
    cast specification' on DATE / DECIMAL columns
"""

import pyodbc
import logging
import datetime
from contextlib import contextmanager
from typing import Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class DBConnector:
    """MS SQL Server connection manager via pyodbc."""

    def __init__(self, config: dict):
        self.server   = config.get("server",             "your_server")
        self.database = config.get("database",           "bi_reporting_db")
        self.trusted  = config.get("trusted_connection", True)
        self.username = config.get("username",           "")
        self.password = config.get("password",           "")
        self.driver   = config.get("driver",             "ODBC Driver 17 for SQL Server")
        self._conn: Optional[pyodbc.Connection] = None

    # ── Connection ───────────────────────────────────────────────────────────

    def connect(self) -> pyodbc.Connection:
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
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("SQL Server connection closed.")

    @contextmanager
    def connection(self):
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
        if not self._conn:
            self.connect()
        cursor = self._conn.cursor()
        cursor.execute(sql, params)
        self._conn.commit()

    def query_df(self, sql: str, params: tuple = ()) -> pd.DataFrame:
        """
        Execute a SELECT and return a DataFrame.

        Uses a raw pyodbc cursor rather than pd.read_sql() to avoid:
            UserWarning: pandas only supports SQLAlchemy connectable ...
        """
        if not self._conn:
            self.connect()
        cursor = self._conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        columns = [col[0] for col in cursor.description]
        rows    = cursor.fetchall()
        df      = pd.DataFrame.from_records(rows, columns=columns)

        # pyodbc returns SQL Server DECIMAL/NUMERIC as decimal.Decimal.
        # Cast any such columns to float64 for numpy compatibility.
        import decimal
        for col in df.columns:
            if not df[col].empty and df[col].apply(
                lambda x: isinstance(x, decimal.Decimal)
            ).any():
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    # ── Type coercion ────────────────────────────────────────────────────────

    @staticmethod
    def _coerce_value(v):
        """
        Convert a single value to a Python native type that pyodbc accepts
        for SQL Server columns.  Handles numpy scalars, pandas Timestamps,
        date/datetime strings, and NaN/None.
        """
        if v is None:
            return None
        if isinstance(v, float) and np.isnan(v):
            return None
        if isinstance(v, (np.integer,)):
            return int(v)
        if isinstance(v, (np.floating,)):
            return None if np.isnan(v) else float(v)
        if isinstance(v, (np.bool_,)):
            return bool(v)
        if isinstance(v, pd.Timestamp):
            return v.to_pydatetime()
        if isinstance(v, datetime.datetime):
            return v
        if isinstance(v, datetime.date):
            return v
        if isinstance(v, (np.str_, str)):
            s = str(v)
            # YYYY-MM-DD  →  datetime.date
            if len(s) == 10 and s[4] == "-" and s[7] == "-":
                try:
                    return datetime.date.fromisoformat(s)
                except ValueError:
                    pass
            # ISO datetime string  →  datetime.datetime
            if "T" in s or (len(s) >= 19 and s[10] == " "):
                try:
                    return datetime.datetime.fromisoformat(s)
                except ValueError:
                    pass
            return s
        # pandas NA
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        return v

    def _coerce_row(self, row: tuple) -> tuple:
        return tuple(self._coerce_value(v) for v in row)

    # ── Bulk Insert ──────────────────────────────────────────────────────────

    def bulk_insert_df(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str = "dbo",
        chunk_size: int = 1000,
    ) -> int:
        """
        Bulk insert a DataFrame into a SQL Server table.

        Coerces every value to a Python native type before executemany
        so pyodbc does not raise 'Invalid character value for cast
        specification' on DATE / DECIMAL / INT columns.
        """
        if not self._conn:
            self.connect()

        cursor = self._conn.cursor()
        cursor.fast_executemany = True

        cols         = ", ".join([f"[{c}]" for c in df.columns])
        placeholders = ", ".join(["?" for _ in df.columns])
        sql = (
            f"INSERT INTO [{schema}].[{table_name}] ({cols}) "
            f"VALUES ({placeholders})"
        )

        data = [
            self._coerce_row(row)
            for row in df.itertuples(index=False, name=None)
        ]

        for i in range(0, len(data), chunk_size):
            chunk = data[i: i + chunk_size]
            cursor.executemany(sql, chunk)
            self._conn.commit()

        rows = len(df)
        logger.info(f"Inserted {rows:,} rows into [{schema}].[{table_name}]")
        return rows

    def get_row_count(self, table_name: str, schema: str = "dbo") -> int:
        cursor = self._conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table_name}]")
        return cursor.fetchone()[0]

    def table_exists(self, table_name: str, schema: str = "dbo") -> bool:
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            (schema, table_name),
        )
        return cursor.fetchone()[0] > 0
