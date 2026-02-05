"""Helper functions for compute_embeddings CLI."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from common.cli_helpers import parse_date

DEFAULT_MODEL = "all-MiniLM-L6-v2"


def parse_compute_embeddings_args() -> argparse.Namespace:
    """Parse CLI arguments for compute_embeddings."""

    parser = argparse.ArgumentParser()

    # Input options
    parser.add_argument(
        "--published-date",
        type=lambda v: parse_date(v, "published-date"),
        default=datetime.now(timezone.utc).date(),
        help="Embed articles published on this date (UTC, YYYY-MM-DD)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-embed even if the article already has an embedding for this model",
    )

    # Model options
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"Sentence transformer model (default: {DEFAULT_MODEL})",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=32,
        help="Batch size for encoding (default: 32)",
    )
    parser.add_argument(
        "--word-limit",
        type=int,
        default=None,
        help="Max words to embed (default: no limit)",
    )

    # Field options
    parser.add_argument("--no-title", action="store_true", help="Exclude title from embedding")
    parser.add_argument("--no-summary", action="store_true", help="Exclude summary from embedding")
    parser.add_argument("--no-text", action="store_true", help="Exclude text from embedding")

    # Output options
    parser.add_argument("--load-s3", action="store_true", help="Upload results to S3")
    parser.add_argument("--load-rds", action="store_true", help="Load embeddings into RDS")
    parser.add_argument("--load-local", action="store_true", help="Save results to local file")

    return parser.parse_args()
