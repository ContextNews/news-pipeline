"""Helper functions for cluster_articles CLI."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from common.cli_helpers import parse_date

DEFAULT_MODEL = "all-MiniLM-L6-v2"


def parse_cluster_articles_args() -> argparse.Namespace:
    """Parse CLI arguments for cluster_articles."""

    parser = argparse.ArgumentParser()

    # Input options
    parser.add_argument(
        "--ingested-date",
        type=lambda v: parse_date(v, "ingested-date"),
        default=datetime.now(timezone.utc).date(),
        help="Cluster articles ingested on this date (UTC, YYYY-MM-DD)",
    )
    parser.add_argument(
        "--embedding-model",
        default=DEFAULT_MODEL,
        help=f"Embedding model to use (default: {DEFAULT_MODEL})",
    )
    parser.add_argument(
        "--overwrite",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Overwrite existing clusters for the ingested date (default: True)",
    )

    # Clustering options
    parser.add_argument(
        "--min-cluster-size",
        type=int,
        default=5,
        help="Minimum cluster size for HDBSCAN (default: 5)",
    )
    parser.add_argument(
        "--min-samples",
        type=int,
        default=None,
        help="Minimum samples for HDBSCAN (default: None)",
    )

    # Output options
    parser.add_argument("--load-s3", action="store_true", help="Upload results to S3")
    parser.add_argument("--load-rds", action="store_true", help="Save clusters to RDS")
    parser.add_argument("--load-local", action="store_true", help="Save results to local file")

    return parser.parse_args()
