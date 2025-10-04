"""
etl/core/utils.py
-----------------
Utility helpers: retry logic, regex extractors, timezone conversions.
"""

import re
import time
import pytz
import pandas as pd
from functools import wraps
from typing import Tuple, Type, Union, Callable, Optional
from .logger import get_logger

logger = get_logger(__name__)

# --------------------------------------------------------------------
# Retry with exponential backoff
# --------------------------------------------------------------------
def retry_with_backoff(
    max_retries: int = 3,
    initial_delay: float = 1,
    max_delay: float = 60,
    exponential_base: float = 2,
    exceptions_to_retry: Union[Type[Exception], Tuple[Type[Exception]]] = Exception,
    error_check_func: Optional[Callable[[Exception], bool]] = None
):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            delay = initial_delay

            while True:
                try:
                    return func(*args, **kwargs)
                except exceptions_to_retry as e:
                    retries += 1
                    should_retry = True
                    if error_check_func is not None:
                        should_retry = error_check_func(e)

                    if not should_retry or retries > max_retries:
                        logger.error(
                            f"Function {func.__name__} failed after {retries} retries. Error: {str(e)}"
                        )
                        raise

                    wait_time = min(delay * (exponential_base ** (retries - 1)), max_delay)
                    logger.warning(
                        f"Retry {retries}/{max_retries} for {func.__name__} in {wait_time:.1f}s due to error: {str(e)}"
                    )
                    time.sleep(wait_time)
        return wrapper
    return decorator

# --------------------------------------------------------------------
# Simple regex extractors (public-safe placeholders)
# --------------------------------------------------------------------
def extract_message_id(text: str) -> Optional[str]:
    """
    Extract message ID (UUID) from text body.
    Example line:
        Message ID: 8d798677-9a33-47d1-876c-a0efe27a7222
    """
    match = re.search(
        r"Message ID:\s*([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-"
        r"[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})",
        text,
        re.IGNORECASE,
    )
    if match:
        return match.group(1)
    return None


def extract_timestamp(text: str) -> Optional[str]:
    """Extract timestamp in ISO8601 format (demo)."""
    match = re.search(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)", text)
    return match.group(1) if match else None


def extract_message_timestamp(text: str) -> Optional[str]:
    """
    Extract ISO8601 timestamp from metadata line.
    Example:
        2025-09-14T05:19:14.864688Z Bob Demo - bob@example.com says:
    """
    match = re.search(
        r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)\s+[A-Za-z ]+\s+-\s+[\w\.-]+@[\w\.-]+\s+says:",
        text
    )
    if match:
        return match.group(1)
    return None


def extract_speaker_name(text: str) -> Optional[str]:
    """
    Extract the speaker's display name from the metadata line.
    Example:
        2025-09-14T05:19:14.864688Z Bob Demo - bob@example.com says:
    """
    match = re.search(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\s+(.+?)\s+-\s+[\w\.-]+@[\w\.-]+\s+says:",
        text
    )
    if match:
        return match.group(1).strip()
    return None


def extract_speaker_contact(text: str) -> Optional[str]:
    """
    Extract the speaker's email address from metadata line.
    Example:
        2025-09-14T05:19:14.864688Z Bob Demo - bob@example.com says:
    """
    match = re.search(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\s+[A-Za-z ]+\s+-\s+([\w\.-]+@[\w\.-]+)\s+says:",
        text
    )
    if match:
        return match.group(1).strip()
    return None


def extract_message_content(text: str) -> Optional[str]:
    """
    Extract the message content after the 'says:' line.
    """
    match = re.search(r"says:\s*(.+)", text, re.DOTALL)
    if match:
        message = match.group(1).strip()
        return " ".join(message.splitlines())  # flatten multiline
    return None


# --------------------------------------------------------------------
# Timezone conversion
# --------------------------------------------------------------------
def convert_utc_to_local(df: pd.DataFrame, column: str, tz: str = "Asia/Hong_Kong"):
    """Convert UTC timestamps in a DataFrame column to given timezone."""
    target_tz = pytz.timezone(tz)
    df[column] = pd.to_datetime(df[column], errors="coerce", utc=True)
    df[column] = df[column].dt.tz_convert(target_tz).dt.strftime("%Y-%m-%d %H:%M:%S")
    return df
