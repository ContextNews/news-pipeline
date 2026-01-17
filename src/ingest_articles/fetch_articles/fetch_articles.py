"""Core ingest logic."""

import logging
from datetime import datetime, timezone, timedelta

from ingest_articles.fetch_articles.fetch_rss_articles import fetch_rss_articles
from ingest_articles.fetch_articles.fetch_article_text import (
    fetch_article_text as fetch_text,
)
from ingest_articles.models import ResolvedArticle
from news_pipeline.utils.hashing import generate_article_id


logger = logging.getLogger(__name__)


def fetch_articles(
    sources: list[str],
    lookback_hours: int,
) -> list[ResolvedArticle]:
    """Fetch and process articles from sources."""
    articles = []
    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=lookback_hours)
    ingested_at = now

    for source in sources:
        logger.info("Fetching articles from %s", source)

        try:
            rss_articles = list(fetch_rss_articles(source, since))
            logger.info("Found %d articles from %s", len(rss_articles), source)
        except Exception as e:
            logger.error("Failed to fetch RSS from %s: %s", source, e)
            continue

        for rss_article in rss_articles:
            article_id = generate_article_id(source, rss_article.url)

            text = fetch_text(rss_article.url)

            articles.append(
                ResolvedArticle(
                    id=article_id,
                    source=source,
                    title=rss_article.title,
                    summary=rss_article.summary,
                    url=rss_article.url,
                    published_at=rss_article.published_at,
                    ingested_at=ingested_at,
                    text=text,
                )
            )

    logger.info("Total articles collected: %d", len(articles))
    return articles
