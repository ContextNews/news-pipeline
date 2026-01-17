"""Ingest and clean articles from RSS sources."""

import logging

from ingest_articles.fetch_articles.fetch_articles import fetch_articles
from ingest_articles.clean_articles.clean import clean
from ingest_articles.models import CleanedArticle

logger = logging.getLogger(__name__)


def ingest_articles(
    sources: list[str],
    lookback_hours: int,
) -> list[CleanedArticle]:
    """Fetch RSS articles and return cleaned results."""
    logger.info("Ingesting articles from %d sources", len(sources))

    # Ingest raw articles
    raw_articles = fetch_articles(sources, lookback_hours)
    if not raw_articles:
        logger.warning("0 Articles ingested")
        return []

    # Clean articles
    cleaned = clean(raw_articles)
    if not cleaned:
        logger.warning("0 Articles cleaned")
        return []

    logger.info("%d Articles ingested and cleaned", len(cleaned))
    return cleaned
