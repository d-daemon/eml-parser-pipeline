"""
config/settings.py
-------------------
Centralized configuration for ETL pipeline.
"""

import os

# --------------------------------------------------------------------
# Environment selection
# --------------------------------------------------------------------
ENV = os.getenv("ETL_ENV", "LOCAL")  # LOCAL | CLOUD

# --------------------------------------------------------------------
# Local defaults
# --------------------------------------------------------------------
LOCAL_DATA_DIR = "data/"
LOCAL_INPUT_DIR = os.path.join(LOCAL_DATA_DIR, "input")
LOCAL_OUTPUT_DIR = os.path.join(LOCAL_DATA_DIR, "output")
LOCAL_DB_PATH = os.path.join(LOCAL_DATA_DIR, "etl_demo.db")
SQLITE_CONN = f"sqlite:///{LOCAL_DB_PATH}"

# --------------------------------------------------------------------
# Optional Cloud (disabled by default, just for illustration)
# --------------------------------------------------------------------
GCP_PROJECT = os.getenv("GCP_PROJECT", "my-demo-project")
GCS_BUCKET = os.getenv("GCS_BUCKET", "my-demo-bucket")

# Example BigQuery datasets
STAGING_DATASET = os.getenv("BQ_STAGING_DATASET", "staging_dataset")
HISTORY_DATASET = os.getenv("BQ_HISTORY_DATASET", "history_dataset")

# --------------------------------------------------------------------
# ETL runtime config
# --------------------------------------------------------------------
MAX_PARALLELISM = int(os.getenv("MAX_PARALLELISM", 4))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# --------------------------------------------------------------------
# Example filters (replace/remove in your own projects)
# --------------------------------------------------------------------
DEFAULT_REGION = "GLOBAL"
DEFAULT_ENTITY = "GENERIC"
DEFAULT_LINE_OF_BUSINESS = "DEMO"
