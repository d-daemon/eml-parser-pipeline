"""
etl/transform/data_quality.py
-----------------------------
Lightweight Data Quality (DQ) checks for parsed DataFrames.
"""

import pandas as pd
from etl.core.logger import get_logger

logger = get_logger(__name__)


def check_nulls(df: pd.DataFrame, exclude: list = None):
    """
    Check for null values in DataFrame.
    """
    if df.empty:
        logger.warning("DataFrame is empty, skipping null check.")
        return pd.DataFrame()

    exclude = exclude or []
    columns_to_check = [col for col in df.columns if col not in exclude]

    nan_counts = df[columns_to_check].isnull().sum()
    total_nan_rows = df[columns_to_check].isnull().any(axis=1).sum()

    logger.info(f"Null counts per column:\n{nan_counts}")
    logger.info(f"Total rows with nulls: {total_nan_rows}")

    return df[df[columns_to_check].isnull().any(axis=1)]


def check_duplicates(df: pd.DataFrame, subset: list = None):
    """
    Check for duplicate rows by subset of columns.
    """
    if df.empty:
        logger.warning("DataFrame is empty, skipping duplicate check.")
        return pd.DataFrame()

    subset = subset or ["email_id", "message_id"]

    duplicates = df[df.duplicated(subset=subset, keep=False)]
    logger.info(f"Found {len(duplicates)} duplicate rows based on {subset}")

    return duplicates


def run_data_quality(df: pd.DataFrame, name: str = "DataFrame"):
    """
    Run all DQ checks on a DataFrame and return rows with issues.
    """
    if df.empty:
        logger.warning(f"{name} is empty, skipping data quality checks.")
        return pd.DataFrame()

    logger.info(f"Running DQ checks on {name} with shape {df.shape}")

    nulls = check_nulls(df, exclude=["content_id"])
    dups = check_duplicates(df, subset=["email_id", "message_id"])

    issues = pd.concat([nulls, dups]).drop_duplicates()
    logger.info(f"Total rows with issues in {name}: {len(issues)}")

    return issues
