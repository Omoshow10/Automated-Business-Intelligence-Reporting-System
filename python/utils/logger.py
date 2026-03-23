"""
python/utils/logger.py
-----------------------
Centralized logging configuration for the BI pipeline.
Creates rotating file logs + console output.
"""

import logging
import logging.handlers
import os
from datetime import datetime


def setup_logger(name: str = "bi_pipeline", log_dir: str = "logs") -> logging.Logger:
    """
    Configure and return the pipeline logger.

    Outputs to:
        - Console (INFO level)
        - logs/pipeline_YYYY-MM-DD.log (DEBUG level, rotating)
    """
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger  # Already configured

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(module)-20s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler — INFO and above
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # File handler — DEBUG and above, daily rotation, 7-day retention
    log_file = os.path.join(log_dir, f"pipeline_{datetime.now().strftime('%Y-%m-%d')}.log")
    fh = logging.handlers.TimedRotatingFileHandler(
        filename=log_file,
        when="midnight",
        backupCount=7,
        encoding="utf-8"
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


# Module-level convenience logger
logger = setup_logger()
