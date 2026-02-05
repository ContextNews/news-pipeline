"""Core clean logic."""

import logging
import re
from typing import Any, Optional

from ingest_articles.models import CleanedArticle
from common.datetime import parse_datetime

logger = logging.getLogger(__name__)


def _get_value(raw: Any, key: str) -> Any:
    if isinstance(raw, dict):
        return raw.get(key)
    return getattr(raw, key, None)


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
        article_id = _get_value(raw, "id")
        url = _get_value(raw, "url")

        # Skip articles missing required fields
        if not article_id or not url:
            logger.warning("Skipping article with missing id or url: id=%s, url=%s", article_id, url)
            continue

        results.append(
            CleanedArticle(
                id=article_id,
                source=_get_value(raw, "source"),
                title=clean_text(_get_value(raw, "title")) or "",
                summary=clean_text(_get_value(raw, "summary")) or "",
                url=url,
                published_at=parse_datetime(_get_value(raw, "published_at")),
                ingested_at=parse_datetime(_get_value(raw, "ingested_at")),
                text=clean_text(_get_value(raw, "text")),
            )
        )

    logger.info("Cleaned %d articles", len(results))
    return results
