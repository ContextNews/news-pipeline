"""Core ingest logic."""

import logging
from datetime import datetime, timezone, timedelta

from news_pipeline.stage1_ingest.fetch_rss_articles import fetch_rss_articles
from news_pipeline.stage1_ingest.fetch_article_text import fetch_article_text as fetch_text
from news_pipeline.stage1_ingest.models import RawArticle
from news_pipeline.utils.hashing import generate_article_id

logger = logging.getLogger(__name__)


def ingest(sources: list[str], lookback_hours: int = 24, fetch_article_text: bool = True) -> list[RawArticle]:
    """Fetch and process articles from sources."""
    articles = []
    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=lookback_hours)
    ingested_at = now

    for source in sources:
        logger.info(f"Fetching articles from {source}")

        try:
            rss_articles = list(fetch_rss_articles(source, since))
            logger.info(f"Found {len(rss_articles)} articles from {source}")
        except Exception as e:
            logger.error(f"Failed to fetch RSS from {source}: {e}")
            continue

        for rss_article in rss_articles:
            article_id = generate_article_id(source, rss_article.url)

            if fetch_article_text:
                text = fetch_text(rss_article.url)
            else:
                text = None

            articles.append(RawArticle(
                id=article_id,
                source=source,
                title=rss_article.title,
                summary=rss_article.summary,
                url=rss_article.url,
                published_at=rss_article.published_at,
                ingested_at=ingested_at,
                text=text,
            ))

    logger.info(f"Total articles collected: {len(articles)}")
    return articles
