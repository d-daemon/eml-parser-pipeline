"""
etl/load/batch_control.py
-------------------------
Simple batch tracking class for ETL pipeline.
Now persists control metadata to SQLite and CSV.
"""

import os
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from etl.core.logger import get_logger
from config import settings

logger = get_logger(__name__)

class BatchControl:
    def __init__(self, batch_name: str, ctx):
        self.batch_name = batch_name
        self.start_time = None
        self.end_time = None
        self.rows_expected = None
        self.rows_loaded = None
        self.status = None
        self.ctx = ctx

    # --------------------------------------------------------------
    # Control flow
    # --------------------------------------------------------------
    def start(self, rows_expected: int = 0):
        self.start_time = datetime.now()
        self.rows_expected = rows_expected
        self.status = "RUNNING"
        logger.info(f"Batch '{self.batch_name}' started at {self.start_time} (expected {rows_expected} rows)")

    def end(self, rows_loaded: int = 0, success: bool = True):
        self.end_time = datetime.now()
        self.rows_loaded = rows_loaded
        self.status = "SUCCESS" if success else "FAILED"
        duration = (self.end_time - self.start_time).total_seconds() if self.start_time else None

        logger.info(
            f"Batch '{self.batch_name}' completed at {self.end_time} "
            f"(duration {duration:.2f}s, rows_loaded={rows_loaded})"
        )

        # Persist record
        self.persist(duration)

    # --------------------------------------------------------------
    # Persistence layer
    # --------------------------------------------------------------
    def persist(self, duration: float):
        record = {
            "batch_name": self.batch_name,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_sec": duration,
            "rows_expected": self.rows_expected,
            "rows_loaded": self.rows_loaded,
            "status": self.status,
            "created_at": datetime.now(),
        }

        df = pd.DataFrame([record])
        output_dir = settings.LOCAL_OUTPUT_DIR
        os.makedirs(self.ctx.output_dir, exist_ok=True)

        # --- Write to CSV (append mode)
        csv_path = os.path.join(self.ctx.output_dir, "batch_control.csv")
        header = not os.path.exists(csv_path)
        df.to_csv(csv_path, mode="a", header=header, index=False)

        # --- Write to SQLite
        engine = create_engine(f"sqlite:///{os.path.join(self.ctx.output_dir, 'etl_demo.db')}")
        with engine.connect() as conn:
            df.to_sql("batch_control", conn, if_exists="append", index=False)

        logger.info(f"Batch control record saved to {csv_path} and SQLite.")
