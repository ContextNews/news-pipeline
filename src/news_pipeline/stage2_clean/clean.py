"""Core clean logic."""

import logging
import re
from typing import Optional

from news_pipeline.stage2_clean.models import CleanedArticle
from news_pipeline.utils.datetime import parse_datetime

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


def clean(raw_articles: list[dict]) -> list[CleanedArticle]:
    """Clean raw articles: title, summary, and text."""
    if not raw_articles:
        logger.warning("No articles to clean")
        return []

    logger.info(f"Cleaning {len(raw_articles)} articles")

    results = []
    for raw in raw_articles:
        results.append(CleanedArticle(
            id=raw["id"],
            source=raw["source"],
            title=clean_text(raw.get("title")) or "",
            summary=clean_text(raw.get("summary")) or "",
            url=raw["url"],
            published_at=parse_datetime(raw.get("published_at")),
            ingested_at=parse_datetime(raw.get("ingested_at")),
            text=clean_text(raw.get("text")),
        ))

    logger.info(f"Cleaned {len(results)} articles")
    return results
