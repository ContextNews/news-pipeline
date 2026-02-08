"""Helper functions for resolve_entities CLI."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from common.cli_helpers import parse_date


def parse_resolve_entities_args() -> argparse.Namespace:
    """Parse CLI arguments for resolve_entities."""

    parser = argparse.ArgumentParser()

    # Input options
    parser.add_argument(
        "--published-date",
        type=lambda v: parse_date(v, "published-date"),
        default=datetime.now(timezone.utc).date(),
        help="Resolve entities for articles published on this date (UTC, YYYY-MM-DD)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-resolve entities for articles that already have resolved entities",
    )

    # Output options
    parser.add_argument("--load-s3", action="store_true", help="Upload results to S3")
    parser.add_argument(
        "--load-rds", action="store_true", help="Load resolved entities into RDS"
    )
    parser.add_argument(
        "--load-local", action="store_true", help="Save results to local file"
    )

    return parser.parse_args()
