"""
Project-wide site customization to ensure service packages are importable
when running from the repository root without setting PYTHONPATH manually.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent

SERVICE_PATHS = [
    ROOT / "services" / "news-api",
    ROOT / "services" / "news-cluster",
    ROOT / "services" / "news-normalize",
    ROOT / "services" / "news-ingest",
]

for path in SERVICE_PATHS:
    if path.exists():
        sys.path.insert(0, str(path))
