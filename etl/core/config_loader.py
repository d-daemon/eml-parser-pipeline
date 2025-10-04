"""
etl/core/config_loader.py
-------------------------
Optional utility to load YAML/JSON configs for dynamic pipelines.
"""

import json
import yaml
from pathlib import Path

def load_config(path: str):
    """Load a config file (YAML or JSON)."""
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    if file_path.suffix.lower() in [".yaml", ".yml"]:
        with open(file_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    elif file_path.suffix.lower() == ".json":
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    else:
        raise ValueError("Unsupported config format. Use YAML or JSON.")
