"""
etl/load/storage.py
-------------------
Handles persistence of DataFrames.

- Local storage (CSV, SQLite)
- Extensible to cloud (BigQuery, S3, GCS) if needed
"""

import os
import pandas as pd
from sqlalchemy import create_engine
from etl.core.logger import get_logger
from config import settings

logger = get_logger(__name__)


class Storage:
    def __init__(self, output_dir: str = settings.LOCAL_OUTPUT_DIR):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.sqlite_conn = settings.SQLITE_CONN

    # ------------------------------------------------------------------
    # CSV
    # ------------------------------------------------------------------
    def write_csv(self, df: pd.DataFrame, name: str):
        """Write DataFrame to CSV in output dir."""
        if df.empty:
            logger.warning(f"No data to write for {name}. Skipping CSV export.")
            return

        file_path = os.path.join(self.output_dir, f"{name}.csv")
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        logger.info(f"Saved {len(df)} rows to CSV: {file_path}")

    # ------------------------------------------------------------------
    # SQLite
    # ------------------------------------------------------------------
    def write_sqlite(self, df: pd.DataFrame, table: str):
        """Append DataFrame to SQLite table."""
        if df.empty:
            logger.warning(f"No data to write for {table}. Skipping SQLite export.")
            return

        engine = create_engine(self.sqlite_conn)
        df.to_sql(table, engine, if_exists="append", index=False)
        logger.info(f"Appended {len(df)} rows into SQLite table: {table}")

    # ------------------------------------------------------------------
    # Optional Cloud Placeholders (for extension)
    # ------------------------------------------------------------------
    def write_bigquery(self, df: pd.DataFrame, table_id: str):
        """Placeholder for writing to BigQuery (disabled in demo)."""
        logger.info(f"(DEMO) Would write {len(df)} rows to BigQuery table {table_id}")
        # from google.cloud import bigquery
        # client = bigquery.Client(project=settings.GCP_PROJECT)
        # job = client.load_table_from_dataframe(df, table_id)
        # job.result()

    def write_s3(self, df: pd.DataFrame, bucket: str, key: str):
        """Placeholder for writing to S3 (disabled in demo)."""
        logger.info(f"(DEMO) Would write {len(df)} rows to s3://{bucket}/{key}")
