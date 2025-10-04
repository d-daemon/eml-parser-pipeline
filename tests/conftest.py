# tests/conftest.py
import sys
import os

# Add project root to PYTHONPATH so "etl" and "examples" can be imported
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
