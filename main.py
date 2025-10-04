"""
etl/main.py
-----------
Main ETL orchestrator.

Steps:
1. Extract emails from input directory
2. Transform (enrich + DQ checks)
3. Merge & link attachments
4. Load to CSV & SQLite
5. Track metadata with BatchControl
"""

import argparse
from datetime import datetime
from etl.core.logger import setup_logger, get_logger
from etl.transform.processor import process_files_parallel, merge_messages_with_attachments
from etl.transform.enrichments import enrich_messages, enrich_attachments
from etl.transform.data_quality import run_data_quality
from etl.load.storage import Storage
from etl.load.batch_control import BatchControl
from config import settings

# --------------------------------------------------------------------
# Setup logging
# --------------------------------------------------------------------
setup_logger()
logger = get_logger(__name__)


def run_pipeline(input_dir: str, output_dir: str, max_workers: int = settings.MAX_PARALLELISM):
    """
    Run the full ETL pipeline.
    """
    logger.info("Starting ETL pipeline...")
    start_time = datetime.now()

    # --------------------------------------------------------------
    # Extract
    # --------------------------------------------------------------
    messages_df, attachments_df, file_count = process_files_parallel(input_dir, max_workers=max_workers)
    logger.info(f"Extracted data from {file_count} files")

    # --------------------------------------------------------------
    # Transform
    # --------------------------------------------------------------
    if messages_df.empty:
        logger.warning("No messages parsed from input. Pipeline will exit early.")
        return

    # Merge + link attachments
    messages_df, attachments_df = merge_messages_with_attachments(messages_df, attachments_df)

    # Enrichment data with batch partition date
    messages_df = enrich_messages(messages_df)
    attachments_df = enrich_attachments(attachments_df)

    # Data Quality checks
    issues_df = run_data_quality(messages_df, name="Messages")
    if not issues_df.empty:
        logger.warning(f"Data quality issues detected: {len(issues_df)} rows")

    # --------------------------------------------------------------
    # Load
    # --------------------------------------------------------------
    storage = Storage(output_dir)

    # Messages
    msg_batch = BatchControl("messages_load")
    msg_batch.start(rows_expected=len(messages_df))
    storage.write_csv(messages_df, "messages")
    storage.write_sqlite(messages_df, "messages")
    msg_batch.end(rows_loaded=len(messages_df))

    # Attachments
    att_batch = BatchControl("attachments_load")
    att_batch.start(rows_expected=len(attachments_df))
    storage.write_csv(attachments_df, "attachments")
    storage.write_sqlite(attachments_df, "attachments")
    att_batch.end(rows_loaded=len(attachments_df))

    # --------------------------------------------------------------
    # Summary
    # --------------------------------------------------------------
    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()

    with_attachments = messages_df[messages_df["with_attachment"] == True].shape[0]
    without_attachments = messages_df[messages_df["with_attachment"] == False].shape[0]

    logger.info("------------------------------------------------------------")
    logger.info("ETL pipeline complete")
    logger.info(f"Processed {file_count} .eml files in {elapsed:.2f} seconds")
    logger.info(f"Messages total: {len(messages_df)}")
    logger.info(f" - with attachments: {with_attachments}")
    logger.info(f" - without attachments: {without_attachments}")
    logger.info(f"Attachments total: {len(attachments_df)}")
    logger.info("------------------------------------------------------------")


# --------------------------------------------------------------------
# CLI entrypoint
# --------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ETL pipeline for email parsing")
    parser.add_argument("--input", type=str, default=settings.LOCAL_INPUT_DIR,
                        help="Directory containing .eml files")
    parser.add_argument("--output", type=str, default=settings.LOCAL_OUTPUT_DIR,
                        help="Directory for output CSV/SQLite")
    parser.add_argument("--workers", type=int, default=settings.MAX_PARALLELISM,
                        help="Number of parallel workers")
    args = parser.parse_args()

    run_pipeline(input_dir=args.input, output_dir=args.output, max_workers=args.workers)
