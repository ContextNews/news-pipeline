"""Helper functions for link_stories CLI."""

from __future__ import annotations

import argparse

from common.cli_helpers import parse_date

DEFAULT_MODEL = "gpt-4o-mini"


def parse_link_stories_args() -> argparse.Namespace:
    """Parse CLI arguments for link_stories."""

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--date-a",
        required=True,
        type=lambda v: parse_date(v, "date-a"),
        help="Older UTC date to search for link candidates (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--date-b",
        required=True,
        type=lambda v: parse_date(v, "date-b"),
        help="Newer UTC date whose stories will be linked (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"OpenAI model to use for story grouping (default: {DEFAULT_MODEL})",
    )
    parser.add_argument(
        "--n-candidates",
        type=int,
        default=3,
        help="Number of similarity candidates to fetch per date-b story (default: 3)",
    )
    parser.add_argument(
        "--delete-existing",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Delete existing links between the two dates before linking (default: False)",
    )

    # Output options
    parser.add_argument("--load-rds", action="store_true", help="Persist links to RDS")

    return parser.parse_args()
