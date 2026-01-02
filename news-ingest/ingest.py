#!/usr/bin/env python3
"""News ingestion service entrypoint.

Fetches articles from configured sources, normalizes them to a fixed schema,
and writes append-only JSONL files to S3-compatible object storage.
"""

import argparse
import hashlib
import logging
import sys
import time
from datetime import datetime, timedelta, timezone

from news_ingest.config import load_config, set_config, get_config
from news_ingest.resolve import resolve_article
from news_ingest.sources import get_source_module
from news_ingest.state.db import ensure_table, get_last_fetched_at, update_last_fetched_at
from news_ingest.storage.s3 import upload_articles

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def generate_article_id(source: str, url: str) -> str:
    """Generate a stable, deterministic article ID from source and URL."""
    content = f"{source}:{url}"
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]


def normalize_article(raw: dict, source: str, fetched_at: datetime) -> dict:
    """Normalize a raw article to the canonical schema.

    Resolves full article content if resolve is enabled in config.
    """
    config = get_config()

    # Resolve full article content
    if config.resolve.enabled:
        result = resolve_article(raw["url"])
        content = result.text
        resolution = {
            "success": result.success,
            "method": result.method,
            "error": result.error,
        }
    else:
        # Skip resolution when disabled (e.g., tests)
        content = None
        resolution = {
            "success": False,
            "method": None,
            "error": "Resolution disabled",
        }

    return {
        "article_id": generate_article_id(source, raw["url"]),
        "source": source,
        "headline": raw["headline"],
        "body": raw["body"],
        "content": content,
        "url": raw["url"],
        "published_at": raw["published_at"],
        "fetched_at": fetched_at.isoformat(),
        "resolution": resolution,
    }


def ingest_source(source_id: str, run_timestamp: datetime) -> int:
    """Ingest articles from a single source.

    Args:
        source_id: The source identifier
        run_timestamp: Timestamp for this ingestion run

    Returns:
        Number of articles ingested
    """
    config = get_config()
    logger.info(f"Starting ingestion for source: {source_id}")
    start_time = time.monotonic()

    # Get last fetched timestamp or default lookback
    since = get_last_fetched_at(source_id)
    if since is None:
        since = run_timestamp - timedelta(hours=config.lookback_hours)
        logger.info(f"No previous state for {source_id}, using lookback: {since.isoformat()}")
    else:
        logger.info(f"Fetching articles since: {since.isoformat()}")

    # Fetch articles from source
    source_module = get_source_module(source_id)
    articles = []
    fetch_errors = 0

    for raw_article in source_module.fetch_articles(since):
        try:
            normalized = normalize_article(raw_article, source_id, run_timestamp)
            articles.append(normalized)
        except Exception as e:
            logger.warning(f"Failed to normalize article: {e}")
            fetch_errors += 1
            continue

    logger.info(f"Fetched {len(articles)} articles from {source_id} ({fetch_errors} errors)")

    if not articles:
        logger.info(f"No new articles for {source_id}, skipping upload")
        update_last_fetched_at(source_id, run_timestamp)
        return 0

    # Upload to storage
    output_path = upload_articles(articles, source_id, run_timestamp)
    logger.info(f"Uploaded to: {output_path}")

    # Update state only after successful upload
    update_last_fetched_at(source_id, run_timestamp)

    elapsed = time.monotonic() - start_time
    logger.info(f"Completed {source_id}: {len(articles)} articles in {elapsed:.2f}s")

    return len(articles)


def main():
    parser = argparse.ArgumentParser(
        description="Ingest news articles from configured sources"
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Config file to use: 'prod' (S3 + Postgres) or 'test' (local). Defaults to 'prod'",
    )
    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)
    set_config(config)

    run_timestamp = datetime.now(timezone.utc)
    logger.info(f"Starting ingestion run at {run_timestamp.isoformat()}")
    logger.info(f"Sources: {config.sources}")
    logger.info(f"Storage backend: {config.storage.backend}")
    logger.info(f"State backend: {config.state.backend}")
    logger.info(f"Output format: {config.output.format}")

    # Ensure database table exists (no-op for memory backend)
    ensure_table()

    source_ids = config.sources
    if not source_ids:
        logger.warning("No sources configured")
        return

    total_articles = 0
    failed_sources = []

    for source_id in source_ids:
        try:
            count = ingest_source(source_id, run_timestamp)
            total_articles += count
        except Exception as e:
            logger.error(f"Failed to ingest {source_id}: {e}")
            failed_sources.append(source_id)
            continue

    logger.info(f"Ingestion complete: {total_articles} total articles")

    if failed_sources:
        logger.error(f"Failed sources: {failed_sources}")
        sys.exit(1)


if __name__ == "__main__":
    main()
