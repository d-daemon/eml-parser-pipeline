import os
import pandas as pd
import sqlite3
from etl.load.batch_control import BatchControl
from etl.core.context import ETLContext

def test_batch_control_persistence(tmp_path):
    """Ensure BatchControl persists records to CSV and SQLite using ETLContext."""
    ctx = ETLContext.from_args(input_dir=str(tmp_path), output_dir=str(tmp_path))

    batch = BatchControl("test_load", ctx)
    batch.start(rows_expected=10)
    batch.end(rows_loaded=10)

    csv_path = os.path.join(ctx.output_dir, "batch_control.csv")
    db_path = ctx.db_path

    assert os.path.exists(csv_path)
    assert os.path.exists(db_path)

    df_csv = pd.read_csv(csv_path)
    conn = sqlite3.connect(db_path)
    df_db = pd.read_sql_query("SELECT * FROM batch_control", conn)
    conn.close()

    assert not df_csv.empty
    assert not df_db.empty
    assert "batch_name" in df_db.columns
