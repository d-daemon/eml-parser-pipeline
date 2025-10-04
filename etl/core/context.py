"""
etl/core/context.py
-------------------------
Define a single immutable context object to represent runtime-scoped ETL environment.
"""

from dataclasses import dataclass
import os

@dataclass
class ETLContext:
    """Represents runtime-scoped ETL environment."""
    input_dir: str
    output_dir: str
    db_path: str

    @classmethod
    def from_args(cls, input_dir: str, output_dir: str):
        db_path = os.path.join(output_dir, "etl_demo.db")
        os.makedirs(output_dir, exist_ok=True)
        return cls(input_dir=input_dir, output_dir=output_dir, db_path=db_path)
