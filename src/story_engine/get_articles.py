"""Fetch articles from the database for story clustering."""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import select

from dotenv import load_dotenv

from rds_postgres.connection import get_session
from rds_postgres.models import Article

logger = logging.getLogger(__name__)


def _article_to_dict(article: Article) -> dict[str, Any]:
    embedding = article.embedding
    if embedding is not None and not isinstance(embedding, list):
        try:
            embedding = list(embedding)
        except TypeError:
            pass

    return {
        "id": article.id,
        "source": article.source,
        "title": article.title,
        "summary": article.summary,
        "url": article.url,
        "published_at": article.published_at,
        "ingested_at": article.ingested_at,
        "text": article.text,
        "embedded_text": article.embedded_text,
        "embedding": embedding,
        "embedding_model": article.embedding_model,
    }


def get_articles(limit: int | None = None, require_embedding: bool = True) -> list[dict[str, Any]]:
    """Return articles from the database, optionally filtering to those with embeddings."""
    load_dotenv()



    logger.info("Loading articles (limit=%s, require_embedding=%s)", limit, require_embedding)
    with get_session() as session:
        stmt = select(Article)
        if require_embedding:
            stmt = stmt.where(Article.embedding.is_not(None))
        if limit:
            stmt = stmt.limit(limit)
        results = session.execute(stmt).scalars().all()

    articles = [_article_to_dict(article) for article in results]
    logger.info("Loaded %d articles", len(articles))
    return articles
