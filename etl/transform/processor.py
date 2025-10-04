"""
etl/transform/processor.py
--------------------------
Parallel file processor for .eml parsing.

- Handles batch processing of multiple email files
- Uses multiprocessing for scalability
- Returns combined DataFrames for messages & attachments
"""

from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
import pandas as pd
from etl.core.logger import get_logger
from etl.extract.parser import EmailParser

logger = get_logger(__name__)


def process_single_file(file_path: str):
    """
    Process a single .eml file with its own parser instance.

    Args:
        file_path (str): Path to .eml file

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]:
            (messages_df, attachments_df)
    """
    try:
        email_id = Path(file_path).stem
        parser = EmailParser()
        messages_df, attachments_df = parser.parse_email(file_path)

        # Ensure email_id consistency
        if not messages_df.empty:
            messages_df["email_id"] = email_id
        if not attachments_df.empty:
            attachments_df["email_id"] = email_id

        return messages_df, attachments_df

    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")
        return pd.DataFrame(), pd.DataFrame()


def process_files_parallel(folder: str, max_workers: int = 4):
    """
    Process .eml files in parallel from a given folder.

    Args:
        folder (str): Directory containing .eml files
        max_workers (int): Number of parallel workers

    Returns:
        tuple[pd.DataFrame, pd.DataFrame, int]:
            (messages_df, attachments_df, file_count)
    """
    file_list = list(Path(folder).glob("*.eml"))
    file_count = len(file_list)

    if file_count == 0:
        logger.warning(f"No .eml files found in {folder}")
        return pd.DataFrame(), pd.DataFrame(), 0

    logger.info(f"Found {file_count} .eml files in {folder}")

    messages_list = []
    attachments_list = []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_single_file, str(file)) for file in file_list]
        logger.info("Processing started...")

        for future in futures:
            try:
                messages_df, attachments_df = future.result()
                if not messages_df.empty:
                    messages_list.append(messages_df)
                if not attachments_df.empty:
                    attachments_list.append(attachments_df)
            except Exception as e:
                logger.error(f"Parallel worker failed: {e}")

    result_df = pd.concat(messages_list, ignore_index=True) if messages_list else pd.DataFrame()
    attachments_df = pd.concat(attachments_list, ignore_index=True) if attachments_list else pd.DataFrame()

    logger.info(f"Finished processing {file_count} files.")
    logger.info(f"Messages shape: {result_df.shape}, Attachments shape: {attachments_df.shape}")

    return result_df, attachments_df, file_count


def merge_messages_with_attachments(messages_df: pd.DataFrame, attachments_df: pd.DataFrame):
    """
    Link attachments to their parent messages via message_id,
    and flag messages that have attachments.

    Returns:
        (messages_df, attachments_df)
    """
    if messages_df.empty:
        return messages_df, attachments_df

    if attachments_df.empty:
        # No attachments: mark all as False
        messages_df["with_attachment"] = False
        return messages_df, attachments_df

    # Link attachments to messages by email_id → message_id
    attachments_df = attachments_df.merge(
        messages_df[["email_id", "message_id"]],
        on="email_id",
        how="left"
    )

    # Flag messages that have attachments
    messages_df["with_attachment"] = messages_df["message_id"].isin(attachments_df["message_id"])

    # Drop duplicates just in case
    attachments_df = attachments_df.drop_duplicates(subset=["email_id", "attachment_name"])

    return messages_df, attachments_df

