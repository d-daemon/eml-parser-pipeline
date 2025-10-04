import os
from examples.generate_sample_eml import generate_eml

def test_generate_sample_eml(tmp_path):
    """Verify .eml files are generated."""
    output_dir = tmp_path / "emails"
    generate_eml(str(output_dir), count=2)
    files = list(output_dir.glob("*.eml"))
    assert len(files) == 2
    assert all(f.stat().st_size > 0 for f in files)
