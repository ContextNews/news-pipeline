"""Helper functions for generate_stories CLI."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from common.cli_helpers import parse_date

DEFAULT_MODEL = "gpt-4o-mini"


def parse_generate_stories_args() -> argparse.Namespace:
    """Parse CLI arguments for generate_stories."""

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--cluster-period",
        type=lambda v: parse_date(v, "cluster-period"),
        default=datetime.now(timezone.utc).date(),
        help="UTC date (YYYY-MM-DD) of cluster period to process",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"OpenAI model to use (default: {DEFAULT_MODEL})",
    )
    parser.add_argument(
        "--overwrite",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Overwrite existing stories for the cluster period (default: True)",
    )
    parser.add_argument("--load-s3", action="store_true", help="Upload results to S3")
    parser.add_argument("--load-rds", action="store_true", help="Save stories to RDS")
    parser.add_argument("--load-local", action="store_true", help="Save results to local file")

    return parser.parse_args()
