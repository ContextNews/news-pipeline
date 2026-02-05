"""Core clean logic."""

import logging
import re
from typing import Any, Optional

from ingest_articles.models import CleanedArticle
from common.datetime import parse_datetime
from common.utils import get_value

logger = logging.getLogger(__name__)


def clean_text(text: Optional[str]) -> Optional[str]:
    """Clean text by stripping HTML, fixing escapes, and collapsing whitespace."""
    if not text:
        return None
    # Strip HTML tags (keep text content)
    text = re.sub(r"<[^>]+>", " ", text)
    # Remove escaped quotes
    text = text.replace('\\"', '"')
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text if text else None


def clean(raw_articles: list[Any]) -> list[CleanedArticle]:
    """Clean raw articles: title, summary, and text."""
    if not raw_articles:
        logger.warning("No articles to clean")
        return []

    logger.info("Cleaning %d articles", len(raw_articles))

    results = []
    for raw in raw_articles:
        article_id = get_value(raw, "id")
        url = get_value(raw, "url")

        # Skip articles missing required fields
        if not article_id or not url:
            logger.warning("Skipping article with missing id or url: id=%s, url=%s", article_id, url)
            continue

        results.append(
            CleanedArticle(
                id=article_id,
                source=get_value(raw, "source"),
                title=clean_text(get_value(raw, "title")) or "",
                summary=clean_text(get_value(raw, "summary")) or "",
                url=url,
                published_at=parse_datetime(get_value(raw, "published_at")),
                ingested_at=parse_datetime(get_value(raw, "ingested_at")),
                text=clean_text(get_value(raw, "text")),
            )
        )

    logger.info("Cleaned %d articles", len(results))
    return results
