"""Helper functions for generate_stories CLI."""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from typing import Any

from common.cli_helpers import parse_date


def upload_stories_to_object_store(stories: list[dict[str, Any]], now: datetime) -> None:
    """Upload generated stories to the object store."""
    from common.object_storage import build_object_key, upload_jsonl_to_object_store
    key = build_object_key(
        "generated_stories",
        now,
        f"generated_stories_{now.strftime('%Y_%m_%d_%H_%M')}.jsonl",
    )
    upload_jsonl_to_object_store(stories, os.environ["S3_BUCKET_NAME"], key)

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
    parser.add_argument("--load-object-store", action="store_true", help="Upload results to object store")
    parser.add_argument("--load-db", action="store_true", help="Save stories to DB")
    parser.add_argument("--load-local", action="store_true", help="Save results to local file")

    return parser.parse_args()
