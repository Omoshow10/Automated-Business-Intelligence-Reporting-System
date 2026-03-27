"""
run_pipeline.py
----------------
Automated Business Intelligence Reporting System
Platform : MS SQL Server 2019+ via pyodbc
Author   : Olayinka Somuyiwa

Main orchestrator for the automated BI reporting pipeline.
Executes four stages in sequence:
    Stage 1 — Data Ingestion       (CSV → dbo.stg_sales_raw)
    Stage 2 — SQL Transformation   (Staging → Core → Reporting layers)
    Stage 3a — Anomaly Detection   (Z-score + IQR flagging)
    Stage 3b — Report Generation   (Excel workbook output)

Usage:
    python run_pipeline.py                     # Run all stages
    python run_pipeline.py --stage ingest      # Ingestion only
    python run_pipeline.py --stage transform   # SQL transforms only
    python run_pipeline.py --stage anomaly     # Anomaly detection only
    python run_pipeline.py --stage report      # Report generation only
    python run_pipeline.py --generate-data     # Generate dataset then run all
"""

import argparse
import sys
import time
import logging
from datetime import datetime

from python.utils.config_loader import load_config
from python.utils.logger import setup_logger
from python.utils.db_connector import DBConnector
from python.ingestion.data_loader import DataLoader
from python.transformation.sql_runner import SQLRunner
from python.reporting.anomaly_detector import AnomalyDetector
from python.reporting.report_generator import ReportGenerator

logger = logging.getLogger("bi_pipeline")

BANNER = """
╔══════════════════════════════════════════════════════════════╗
║   Automated Business Intelligence Reporting System           ║
║   Platform: MS SQL Server 2019+ (T-SQL) · Python · Power BI ║
║   Author  : Olayinka Somuyiwa                                ║
╚══════════════════════════════════════════════════════════════╝
"""


def parse_args():
    parser = argparse.ArgumentParser(
        description="BI Reporting Pipeline Orchestrator — MS SQL Server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--stage",
        choices=["ingest", "transform", "anomaly", "report", "all"],
        default="all",
        help="Pipeline stage to execute (default: all)",
    )
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to config YAML (default: config/config.yaml)",
    )
    parser.add_argument(
        "--generate-data",
        action="store_true",
        help="Generate synthetic dataset before running pipeline",
    )
    parser.add_argument(
        "--run-date",
        default=None,
        help="Override run date (YYYY-MM-DD). Default: today.",
    )
    return parser.parse_args()


def generate_data():
    import subprocess
    logger.info("Generating synthetic sales dataset...")
    result = subprocess.run(
        [sys.executable, "generate_dataset.py"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        logger.error(f"Dataset generation failed:\n{result.stderr}")
        sys.exit(1)
    logger.info(result.stdout.strip())


def run_stage_ingest(config: dict) -> bool:
    """Stage 1: CSV → dbo.stg_sales_raw (MS SQL Server)."""
    loader = DataLoader(config)
    rows_loaded, rows_rejected = loader.run()
    return rows_loaded > 0


def run_stage_transform(config: dict) -> bool:
    """Stage 2: Execute T-SQL transformation stored procedures."""
    runner = SQLRunner(config)
    runner.run()
    counts = runner.get_row_counts()
    logger.info("\nRow count validation:")
    for table, count in counts.items():
        status = "✅" if count > 0 else "⚠️ "
        logger.info(f"  {status} {table:<30} {count:>8,} rows")
    return True


def run_stage_anomaly(config: dict) -> bool:
    """Stage 3a: Z-score + IQR anomaly detection → dbo.rpt_anomalies."""
    detector = AnomalyDetector(config)
    anomalies = detector.run()
    logger.info(f"Anomalies detected and written to dbo.rpt_anomalies: {len(anomalies):,}")
    return True


def run_stage_report(config: dict) -> bool:
    """Stage 3b: Automated Excel report generation."""
    generator = ReportGenerator(config)
    filepath = generator.run()
    logger.info(f"Report generated: {filepath}")
    return True


def print_summary(stage_results: dict, total_elapsed: float):
    logger.info("\n" + "=" * 62)
    logger.info("PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 62)
    for stage, (success, elapsed) in stage_results.items():
        status = "✅ SUCCESS" if success else "❌ FAILED "
        logger.info(f"  {status}  {stage:<32} {elapsed:>6.2f}s")
    logger.info(f"\n  Total elapsed: {total_elapsed:.2f}s")
    logger.info("=" * 62)

    all_passed = all(s for s, _ in stage_results.values())
    if all_passed:
        logger.info("🎉 All stages completed successfully!")
        logger.info("   → Database    : bi_reporting_db (MS SQL Server)")
        logger.info("   → Excel report: data/processed/report_summary_*.xlsx")
        logger.info("   → Anomalies   : data/processed/anomalies.csv")
        logger.info("   → Pipeline log: SELECT * FROM dbo.pipeline_log ORDER BY log_id DESC")
        logger.info("   → Logs        : logs/pipeline_*.log")
    else:
        logger.error("⚠️  One or more stages failed. Check logs and dbo.pipeline_log.")


def main():
    setup_logger()
    print(BANNER)

    args   = parse_args()
    config = load_config(args.config)
    stage  = args.stage

    logger.info(f"Pipeline started  | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Stage             | {stage}")
    logger.info(
        f"Database          | MS SQL Server → "
        f"{config['database'].get('server', 'your_server')}/"
        f"{config['database'].get('database', 'bi_reporting_db')}"
    )

    if args.generate_data:
        generate_data()

    pipeline_start = time.time()
    stage_results  = {}

    stages_to_run = {
        "ingest":    (run_stage_ingest,    stage in ("all", "ingest")),
        "transform": (run_stage_transform, stage in ("all", "transform")),
        "anomaly":   (run_stage_anomaly,   stage in ("all", "anomaly")),
        "report":    (run_stage_report,    stage in ("all", "report")),
    }

    all_success = True
    for stage_name, (stage_fn, should_run) in stages_to_run.items():
        if not should_run:
            continue

        t_start = time.time()
        try:
            success = stage_fn(config)
        except Exception as e:
            logger.error(
                f"Stage '{stage_name}' failed: {e}", exc_info=True
            )
            success = False
            all_success = False

        elapsed = time.time() - t_start
        stage_results[stage_name] = (success, elapsed)

        if not success and stage == "all":
            logger.error(f"Stopping pipeline at failed stage: {stage_name}")
            break

    total_elapsed = time.time() - pipeline_start
    print_summary(stage_results, total_elapsed)
    sys.exit(0 if all_success else 1)


if __name__ == "__main__":
    main()
