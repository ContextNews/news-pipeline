"""Helper functions for ingest_articles CLI."""

from __future__ import annotations

import argparse
import logging

from ingest_articles.fetch_articles.sources import RSS_FEEDS

logger = logging.getLogger(__name__)


def parse_sources(value: str | None) -> list[str]:
    '''Parse the --sources argument into a list of sources.'''

    # If no value is provided or if "all" is specified, return all sources
    if not value or value.strip().lower() == "all":
        return list(RSS_FEEDS.keys())

    # Parse comma-separated sources, keeping only valid ones
    valid_sources = set(RSS_FEEDS.keys())
    parsed = [s.strip() for s in value.split(",") if s.strip() and s.strip().lower() != "all"]

    # Log any invalid sources
    for source in parsed:
        if source not in valid_sources:
            logger.warning("Invalid source: %s", source)

    sources = [s for s in parsed if s in valid_sources]

    # Raise an error if no valid sources were provided
    if not sources:
        raise ValueError(f"No valid sources provided. Valid sources: {', '.join(sorted(valid_sources))}")

    return sources


def parse_ingest_articles_args() -> argparse.Namespace:
    '''Parse CLI arguments for ingest_articles.'''

    parser = argparse.ArgumentParser()
    parser.add_argument("--lookback-hours", type=int, default=12)
    parser.add_argument(
        "--sources",
        default=None,
        help="Comma-separated list of sources (default: all).",
    )
    parser.add_argument("--load-s3", action="store_true")
    parser.add_argument("--load-rds", action="store_true")
    parser.add_argument("--load-local", action="store_true")
    return parser.parse_args()
