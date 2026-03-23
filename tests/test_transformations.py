"""
tests/test_transformations.py
------------------------------
Unit tests for SQL transformation layer logic.
Tests core_sales derived fields and reporting aggregations.

Run: pytest tests/test_transformations.py -v
"""

import pytest
import sqlite3
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def db():
    """In-memory SQLite database with schema and sample data."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    # Create tables
    conn.executescript("""
        CREATE TABLE stg_sales_raw (
            transaction_id TEXT, date TEXT, product_name TEXT,
            product_category TEXT, region TEXT, sales_rep TEXT,
            customer_id TEXT, customer_segment TEXT, channel TEXT,
            units_sold INTEGER, unit_price REAL, revenue REAL,
            cost REAL, discount_pct REAL, loaded_at TEXT
        );

        CREATE TABLE core_sales (
            transaction_id TEXT PRIMARY KEY,
            txn_date TEXT, txn_year INTEGER, txn_quarter INTEGER,
            txn_month INTEGER, txn_month_label TEXT,
            product_name TEXT, product_category TEXT, region TEXT,
            sales_rep TEXT, customer_id TEXT, customer_segment TEXT,
            channel TEXT, units_sold INTEGER, unit_price REAL,
            revenue REAL, cost REAL, gross_profit REAL,
            profit_margin REAL, discount_pct REAL,
            discount_amount REAL, is_profitable INTEGER,
            transformed_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE rpt_monthly_revenue (
            month_label TEXT PRIMARY KEY, txn_year INTEGER, txn_month INTEGER,
            total_revenue REAL, total_cost REAL, total_gross_profit REAL,
            avg_profit_margin REAL, transaction_count INTEGER, units_sold INTEGER
        );

        CREATE TABLE rpt_regional_summary (
            region TEXT, txn_year INTEGER,
            total_revenue REAL, total_cost REAL, total_gross_profit REAL,
            avg_profit_margin REAL, transaction_count INTEGER,
            PRIMARY KEY (region, txn_year)
        );
    """)

    # Insert staging data
    staging_rows = [
        ("TXN-000001", "2023-01-15", "Laptop Pro 15",       "Electronics", "North",         "Alex Johnson",  "CUST-1001", "Enterprise", "Direct",  1, 1200.0, 1200.0,  700.0,  0.0,  "2024-01-01"),
        ("TXN-000002", "2023-03-22", "CRM Suite – Annual",  "Software",    "East",          "Maria Garcia",  "CUST-2002", "SMB",        "Partner", 2, 2400.0, 4800.0,  600.0,  0.0,  "2024-01-01"),
        ("TXN-000003", "2023-06-30", "Server Rack Unit",    "Hardware",    "West",          "David Chen",    "CUST-3003", "Enterprise", "Online",  3, 5500.0, 16500.0, 9600.0, 0.0,  "2024-01-01"),
        ("TXN-000004", "2023-01-28", "Implementation",      "Services",    "South",         "Sarah Williams","CUST-4004", "Enterprise", "Direct",  1, 8000.0, 8000.0,  2000.0, 5.0,  "2024-01-01"),
        ("TXN-000005", "2024-02-14", "Analytics Platform",  "Software",    "North",         "Alex Johnson",  "CUST-1001", "Enterprise", "Direct",  1, 3600.0, 3600.0,  420.0,  10.0, "2024-01-01"),
        ("TXN-000006", "2024-02-28", "Wireless Headset",    "Electronics", "International", "James Martinez","CUST-5005", "Consumer",   "Online",  5, 180.0,  900.0,   300.0,  0.0,  "2024-01-01"),
        # Unprofitable transaction (cost > revenue)
        ("TXN-000007", "2024-03-10", "Training Package",    "Services",    "East",          "Emily Brown",   "CUST-6006", "SMB",        "Partner", 1, 1500.0, 800.0,   1000.0, 0.0,  "2024-01-01"),
    ]
    conn.executemany(
        "INSERT INTO stg_sales_raw VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        staging_rows
    )

    # Run core transformation SQL inline (mirrors 03_core_layer.sql logic)
    conn.execute("DELETE FROM core_sales")
    conn.execute("""
        INSERT INTO core_sales (
            transaction_id, txn_date, txn_year, txn_quarter, txn_month,
            txn_month_label, product_name, product_category, region,
            sales_rep, customer_id, customer_segment, channel,
            units_sold, unit_price, revenue, cost,
            gross_profit, profit_margin, discount_pct, discount_amount, is_profitable
        )
        SELECT
            transaction_id,
            date,
            CAST(strftime('%Y', date) AS INTEGER),
            CAST((strftime('%m', date) - 1) / 3 + 1 AS INTEGER),
            CAST(strftime('%m', date) AS INTEGER),
            strftime('%Y-%m', date),
            TRIM(product_name), TRIM(product_category), TRIM(region),
            TRIM(sales_rep), TRIM(customer_id), TRIM(customer_segment), TRIM(channel),
            units_sold, unit_price,
            ROUND(revenue, 2), ROUND(cost, 2),
            ROUND(revenue - cost, 2),
            CASE WHEN revenue = 0 THEN 0 ELSE ROUND((revenue - cost) / revenue, 6) END,
            discount_pct,
            ROUND(unit_price * units_sold * (discount_pct / 100.0), 2),
            CASE WHEN (revenue - cost) > 0 THEN 1 ELSE 0 END
        FROM stg_sales_raw
        WHERE transaction_id IS NOT NULL AND date IS NOT NULL AND revenue IS NOT NULL
    """)

    # Run reporting aggregation
    conn.execute("DELETE FROM rpt_monthly_revenue")
    conn.execute("""
        INSERT INTO rpt_monthly_revenue
        SELECT
            txn_month_label, txn_year, txn_month,
            ROUND(SUM(revenue), 2), ROUND(SUM(cost), 2), ROUND(SUM(gross_profit), 2),
            ROUND(AVG(profit_margin), 6), COUNT(*), SUM(units_sold)
        FROM core_sales
        GROUP BY txn_month_label, txn_year, txn_month
    """)

    conn.execute("DELETE FROM rpt_regional_summary")
    conn.execute("""
        INSERT INTO rpt_regional_summary
        SELECT
            region, txn_year,
            ROUND(SUM(revenue), 2), ROUND(SUM(cost), 2), ROUND(SUM(gross_profit), 2),
            ROUND(AVG(profit_margin), 6), COUNT(*)
        FROM core_sales
        GROUP BY region, txn_year
    """)

    conn.commit()
    yield conn
    conn.close()


def query(conn, sql):
    return pd.read_sql_query(sql, conn)


# ── Tests: Staging → Core ─────────────────────────────────────────────────────

class TestCoreLayerTransformation:
    def test_all_staging_rows_loaded_to_core(self, db):
        stg_count  = query(db, "SELECT COUNT(*) AS n FROM stg_sales_raw")["n"][0]
        core_count = query(db, "SELECT COUNT(*) AS n FROM core_sales")["n"][0]
        assert core_count == stg_count

    def test_gross_profit_calculated_correctly(self, db):
        df = query(db, "SELECT revenue, cost, gross_profit FROM core_sales")
        expected = (df["revenue"] - df["cost"]).round(2)
        pd.testing.assert_series_equal(df["gross_profit"], expected, check_names=False)

    def test_profit_margin_is_ratio(self, db):
        df = query(db, "SELECT profit_margin FROM core_sales WHERE revenue > 0")
        # profit_margin stored as ratio (0.0–1.0+), not percentage
        assert df["profit_margin"].between(-2.0, 2.0).all(), \
            "Profit margin should be stored as a ratio"

    def test_is_profitable_flag_correct(self, db):
        df = query(db, "SELECT gross_profit, is_profitable FROM core_sales")
        for _, row in df.iterrows():
            if row["gross_profit"] > 0:
                assert row["is_profitable"] == 1
            else:
                assert row["is_profitable"] == 0

    def test_date_decomposition_year(self, db):
        df = query(db, "SELECT txn_date, txn_year FROM core_sales")
        for _, row in df.iterrows():
            expected_year = int(row["txn_date"][:4])
            assert row["txn_year"] == expected_year

    def test_date_decomposition_quarter(self, db):
        df = query(db, "SELECT txn_month, txn_quarter FROM core_sales")
        for _, row in df.iterrows():
            expected_q = (int(row["txn_month"]) - 1) // 3 + 1
            assert row["txn_quarter"] == expected_q

    def test_month_label_format(self, db):
        df = query(db, "SELECT txn_month_label FROM core_sales")
        import re
        pattern = re.compile(r"^\d{4}-\d{2}$")
        for label in df["txn_month_label"]:
            assert pattern.match(label), f"Bad month_label format: {label}"

    def test_discount_amount_calculation(self, db):
        df = query(db, """
            SELECT unit_price, units_sold, discount_pct, discount_amount
            FROM core_sales WHERE discount_pct > 0
        """)
        for _, row in df.iterrows():
            expected = round(row["unit_price"] * row["units_sold"] * (row["discount_pct"] / 100.0), 2)
            assert abs(row["discount_amount"] - expected) < 0.01

    def test_no_null_transaction_ids_in_core(self, db):
        df = query(db, "SELECT transaction_id FROM core_sales WHERE transaction_id IS NULL")
        assert len(df) == 0

    def test_no_duplicate_transaction_ids(self, db):
        df = query(db, "SELECT transaction_id, COUNT(*) AS cnt FROM core_sales GROUP BY transaction_id HAVING cnt > 1")
        assert len(df) == 0, f"Duplicate transaction IDs found: {df}"

    def test_revenue_non_negative_in_core(self, db):
        df = query(db, "SELECT revenue FROM core_sales WHERE revenue < 0")
        assert len(df) == 0


# ── Tests: Reporting Aggregation ──────────────────────────────────────────────

class TestReportingLayer:
    def test_monthly_revenue_totals_match_core(self, db):
        core_total   = query(db, "SELECT ROUND(SUM(revenue), 2) AS t FROM core_sales")["t"][0]
        report_total = query(db, "SELECT ROUND(SUM(total_revenue), 2) AS t FROM rpt_monthly_revenue")["t"][0]
        assert abs(core_total - report_total) < 0.01, \
            f"Revenue mismatch: core={core_total}, report={report_total}"

    def test_regional_revenue_totals_match_core(self, db):
        core_total   = query(db, "SELECT ROUND(SUM(revenue), 2) AS t FROM core_sales")["t"][0]
        report_total = query(db, "SELECT ROUND(SUM(total_revenue), 2) AS t FROM rpt_regional_summary")["t"][0]
        assert abs(core_total - report_total) < 0.01

    def test_monthly_row_count(self, db):
        """Should have one row per unique year-month combination."""
        month_count = query(db, "SELECT COUNT(DISTINCT txn_month_label) AS n FROM core_sales")["n"][0]
        rpt_count   = query(db, "SELECT COUNT(*) AS n FROM rpt_monthly_revenue")["n"][0]
        assert rpt_count == month_count

    def test_regional_profit_margin_range(self, db):
        df = query(db, "SELECT avg_profit_margin FROM rpt_regional_summary")
        # Margins stored as ratios, should be in a reasonable range for our dataset
        assert df["avg_profit_margin"].between(-1.0, 1.0).all()

    def test_all_regions_present(self, db):
        regions = set(query(db, "SELECT DISTINCT region FROM rpt_regional_summary")["region"].tolist())
        expected = {"North", "South", "East", "West", "International"}
        assert regions == expected

    def test_transaction_count_sums_correctly(self, db):
        core_count   = query(db, "SELECT COUNT(*) AS n FROM core_sales")["n"][0]
        report_count = query(db, "SELECT SUM(transaction_count) AS n FROM rpt_monthly_revenue")["n"][0]
        assert core_count == report_count


# ── Tests: Business Logic Assertions ─────────────────────────────────────────

class TestBusinessLogic:
    def test_unprofitable_transaction_flagged(self, db):
        """TXN-000007 has cost > revenue — should be flagged is_profitable=0."""
        df = query(db, "SELECT is_profitable FROM core_sales WHERE transaction_id='TXN-000007'")
        assert len(df) == 1
        assert df["is_profitable"][0] == 0

    def test_high_revenue_transaction_present(self, db):
        """TXN-000003 (Server Rack × 3) should be the highest revenue transaction."""
        df = query(db, "SELECT transaction_id, revenue FROM core_sales ORDER BY revenue DESC LIMIT 1")
        assert df["transaction_id"][0] == "TXN-000003"

    def test_enterprise_revenue_share(self, db):
        """Enterprise segment should represent the majority of revenue."""
        df = query(db, """
            SELECT customer_segment, SUM(revenue) AS rev
            FROM core_sales GROUP BY customer_segment
        """)
        total = df["rev"].sum()
        enterprise_rev = df[df["customer_segment"] == "Enterprise"]["rev"].sum()
        share = enterprise_rev / total
        assert share > 0.5, f"Enterprise revenue share {share:.1%} should be > 50%"
