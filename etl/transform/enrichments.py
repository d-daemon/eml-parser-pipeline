"""
etl/transform/enrichments.py
----------------------------
Data enrichment & normalization functions.
"""

import pandas as pd
import pytz
from datetime import datetime
from etl.core.logger import get_logger

logger = get_logger(__name__)

# --------------------------------------------------------------------
# Helper: get current batch date in Asia/Hong_Kong
# --------------------------------------------------------------------
def get_batch_date(tz: str = "Asia/Hong_Kong") -> str:
    """Return current date string in the given timezone (YYYY-MM-DD)."""
    return datetime.now(pytz.timezone(tz)).strftime("%Y-%m-%d")


# --------------------------------------------------------------------
# Core enrichments
# --------------------------------------------------------------------
def normalize_text(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Normalize text: strip whitespace, keep case."""
    if column in df.columns:
        df[column] = df[column].astype(str).str.strip()
        logger.info(f"Normalized whitespace in column '{column}'")
    return df


def enrich_messages(messages_df: pd.DataFrame) -> pd.DataFrame:
    """Apply enrichments to the messages DataFrame."""
    if messages_df.empty:
        return messages_df

    batch_date = get_batch_date()
    messages_df["batch_dt"] = batch_date

    messages_df = normalize_text(messages_df, "message")
    logger.info(f"Added BATCH_DT='{batch_date}' to messages DataFrame")

    return messages_df


def enrich_attachments(attachments_df: pd.DataFrame) -> pd.DataFrame:
    """Apply enrichments to the attachments DataFrame."""
    if attachments_df.empty:
        return attachments_df

    batch_date = get_batch_date()
    attachments_df["batch_dt"] = batch_date
    logger.info(f"Added BATCH_DT='{batch_date}' to attachments DataFrame")

    return attachments_df
