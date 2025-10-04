import os
from main import run_pipeline
from examples.generate_sample_eml import generate_eml

def test_pipeline_smoke(tmp_path):
    """Run a small end-to-end ETL test."""
    input_dir = tmp_path / "emails"
    output_dir = tmp_path / "output"
    os.makedirs(input_dir, exist_ok=True)

    generate_eml(str(input_dir), count=2)
    run_pipeline(input_dir=str(input_dir), output_dir=str(output_dir))

    assert (output_dir / "messages.csv").exists()
    assert (output_dir / "attachments.csv").exists()
    assert (output_dir / "etl_demo.db").exists()
