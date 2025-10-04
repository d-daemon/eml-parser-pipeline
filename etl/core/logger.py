"""
etl/core/logger.py
------------------
Centralized logging configuration.
"""

import logging
import sys
from config import settings

def setup_logger():
    """Configure root logger with stream handler + formatting."""
    logger = logging.getLogger()
    logger.setLevel(settings.LOG_LEVEL)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] - %(levelname)s - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(formatter)

    # Avoid duplicate handlers if re-run
    if not logger.handlers:
        logger.addHandler(console_handler)

    logging.captureWarnings(True)
    logger.info("Logging setup complete.")

def get_logger(name: str) -> logging.Logger:
    """Get a namespaced logger."""
    return logging.getLogger(name)
