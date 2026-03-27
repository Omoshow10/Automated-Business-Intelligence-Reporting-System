# =============================================================================
# pipeline_runner.py
# Automated Business Intelligence Reporting System
# Purpose  : Orchestrates automated data transformation,
#            quality validation, aggregation, and report refresh.
# Platform : MS SQL Server 2019+ via pyodbc
# Author   : Olayinka Somuyiwa
# =============================================================================

import pandas as pd
import pyodbc
import logging
import schedule
import time
from datetime import date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)


def get_connection():
    return pyodbc.connect(
        "DRIVER={SQL Server};"
        "SERVER=your_server;"
        "DATABASE=bi_reporting_db;"
        "Trusted_Connection=yes;"
    )


def run_transformation(run_date=None):
    run_date = run_date or date.today()
    logging.info(f"Starting transformation for {run_date}")

    conn = get_connection()

    df = pd.read_sql(
        f"SELECT * FROM dbo.staging_operational_data WHERE record_date = '{run_date}'",
        conn
    )

    if df.empty:
        logging.warning(f"No staging data found for {run_date} — skipping")
        return

    # Data quality validation
    null_count = df.isnull().sum().sum()
    if null_count > 0:
        logging.warning(f"{null_count} null values detected — applying forward fill")
        df = df.ffill()

    # Aggregation to reporting layer
    summary = df.groupby(["entity_id", "metric_name"]).agg(
        total_value  = ("metric_value", "sum"),
        avg_value    = ("metric_value", "mean"),
        record_count = ("metric_value", "count")
    ).reset_index()

    summary["report_date"] = run_date

    # Write results to reporting schema
    cursor = conn.cursor()
    for _, row in summary.iterrows():
        cursor.execute(
            """INSERT INTO dbo.reporting_summary
               (report_date, entity_id, metric_name,
                total_value, avg_value, record_count)
               VALUES (?, ?, ?, ?, ?, ?)""",
            row.report_date, row.entity_id, row.metric_name,
            row.total_value, row.avg_value, row.record_count
        )

    conn.commit()
    conn.close()
    logging.info(f"Transformation complete — {len(summary)} records written")


# Schedule to run daily at 06:30
schedule.every().day.at("06:30").do(run_transformation)

if __name__ == "__main__":
    logging.info("Pipeline scheduler started")
    while True:
        schedule.run_pending()
        time.sleep(60)
