"""Helper functions for extract_entities CLI."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from common.cli_helpers import parse_date

DEFAULT_MODEL = "en_core_web_sm"


def parse_extract_entities_args() -> argparse.Namespace:
    """Parse CLI arguments for extract_entities."""

    parser = argparse.ArgumentParser()

    # Input options
    parser.add_argument(
        "--published-date",
        type=lambda v: parse_date(v, "published-date"),
        default=datetime.now(timezone.utc).date(),
        help="Extract entities from articles published on this date (UTC, YYYY-MM-DD)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-extract entities for articles that already have entities",
    )

    # Model options
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"spaCy model to use (default: {DEFAULT_MODEL})",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=32,
        help="Batch size for spaCy NER (default: 32)",
    )
    parser.add_argument(
        "--word-limit",
        type=int,
        default=300,
        help="Maximum number of words to extract entities from (default: 300)",
    )

    # Output options
    parser.add_argument("--load-s3", action="store_true", help="Upload results to S3")
    parser.add_argument("--load-rds", action="store_true", help="Load entities into RDS")
    parser.add_argument("--load-local", action="store_true", help="Save results to local file")

    return parser.parse_args()
