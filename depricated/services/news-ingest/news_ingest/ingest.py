"""Core ingestion logic for news-ingest service.

Fetches articles from configured sources, normalizes them to a fixed schema,
and writes append-only JSONL files to S3-compatible object storage.
"""

import hashlib
import logging
import time
from datetime import datetime, timedelta, timezone

from news_ingest.config import Config
from news_ingest.resolve import resolve_article
from news_ingest.sources import get_source_module
from news_ingest.state.db import ensure_table, get_last_fetched_at, update_last_fetched_at
from news_ingest.storage.s3 import upload_articles

logger = logging.getLogger(__name__)


def generate_article_id(source: str, url: str) -> str:
    """Generate a stable, deterministic article ID from source and URL."""
    content = f"{source}:{url}"
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]


def normalize_article(raw: dict, source: str, fetched_at: datetime, config: Config) -> dict:
    """Normalize a raw article to the canonical schema.

    Resolves full article content if resolve is enabled in config.
    """
    # Resolve full article content
    if config.resolve_enabled:
        result = resolve_article(raw["url"], config)
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


def fetch_source(source_id: str, run_timestamp: datetime, config: Config) -> list[dict]:
    """Fetch and normalize articles from a single source.

    Args:
        source_id: The source identifier
        run_timestamp: Timestamp for this ingestion run
        config: Configuration object

    Returns:
        List of normalized article dictionaries
    """
    logger.info(f"Starting fetch for source: {source_id}")
    start_time = time.monotonic()

    # Get last fetched timestamp or default lookback
    since = get_last_fetched_at(source_id, config)
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
            normalized = normalize_article(raw_article, source_id, run_timestamp, config)
            articles.append(normalized)
        except Exception as e:
            logger.warning(f"Failed to normalize article: {e}")
            fetch_errors += 1
            continue

    elapsed = time.monotonic() - start_time
    logger.info(f"Fetched {len(articles)} articles from {source_id} ({fetch_errors} errors) in {elapsed:.2f}s")

    return articles


def ingest(config: Config) -> int:
    """Run the ingestion pipeline with the given configuration.

    Args:
        config: Configuration object

    Returns:
        Exit code (0 for success, 1 if any source failed)
    """
    run_timestamp = datetime.now(timezone.utc)
    logger.info(f"Starting ingestion run at {run_timestamp.isoformat()}")
    logger.info(f"Sources: {config.sources}")
    logger.info(f"Storage backend: {config.storage_backend}")
    logger.info(f"State backend: {config.state_backend}")
    logger.info(f"Output format: {config.output_format}")

    # Ensure database table exists (no-op for memory backend)
    ensure_table(config)

    source_ids = config.sources
    if not source_ids:
        logger.warning("No sources configured")
        return 0

    all_articles = []
    successful_sources = []
    failed_sources = []

    # Fetch articles from all sources
    for source_id in source_ids:
        try:
            articles = fetch_source(source_id, run_timestamp, config)
            all_articles.extend(articles)
            successful_sources.append(source_id)
        except Exception as e:
            logger.error(f"Failed to fetch {source_id}: {e}")
            failed_sources.append(source_id)
            continue

    logger.info(f"Fetched {len(all_articles)} total articles from {len(successful_sources)} sources")

    # Upload all articles to a single file
    if all_articles:
        output_path = upload_articles(all_articles, run_timestamp, config)
        logger.info(f"Uploaded to: {output_path}")

        # Update state for all successful sources after upload
        for source_id in successful_sources:
            update_last_fetched_at(source_id, run_timestamp, config)
    else:
        logger.info("No articles fetched, skipping upload")
        # Still update state for sources that returned no articles
        for source_id in successful_sources:
            update_last_fetched_at(source_id, run_timestamp, config)

    logger.info(f"Ingestion complete: {len(all_articles)} total articles")

    if failed_sources:
        logger.error(f"Failed sources: {failed_sources}")
        return 1

    return 0
