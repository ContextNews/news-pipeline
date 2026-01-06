#!/usr/bin/env python3
"""Launcher for the News API without manual PYTHONPATH tweaking."""

import sys
from pathlib import Path


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    service_path = repo_root / "services" / "news-api"
    sys.path.insert(0, str(service_path))

    from news_api.main import main as run_api

    run_api()


if __name__ == "__main__":
    main()
