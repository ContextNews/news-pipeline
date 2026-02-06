"""Helper functions for generate_stories CLI."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from common.cli_helpers import parse_date
from generate_stories.generate_stories import GeneratedStoryOverview

DEFAULT_MODEL = "gpt-4o-mini"


def parse_generate_stories_args() -> argparse.Namespace:
    """Parse CLI arguments for generate_stories."""

    parser = argparse.ArgumentParser()

    # Input options
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

    # Output options
    parser.add_argument("--load-s3", action="store_true", help="Upload results to S3")
    parser.add_argument("--load-rds", action="store_true", help="Save stories to RDS")
    parser.add_argument("--load-local", action="store_true", help="Save results to local file")

    return parser.parse_args()


def build_story_record(
    cluster_id: str,
    article_ids: list[str],
    story: GeneratedStoryOverview,
    story_period: datetime,
    generated_at: datetime,
) -> dict[str, Any]:
    """Build a record for a generated story."""
    return {
        "story_id": uuid4().hex,
        "cluster_id": cluster_id,
        "article_ids": article_ids,
        "title": story.title,
        "summary": story.summary,
        "key_points": story.key_points,
        "quotes": story.quotes,
        "sub_stories": story.sub_stories,
        "location": story.location,
        "location_qid": story.location_qid,
        "noise_article_ids": story.noise_article_ids,
        "story_period": story_period.isoformat(),
        "generated_at": generated_at.isoformat(),
    }
