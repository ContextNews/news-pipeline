"""Fetch articles for story clustering."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import select

logger = logging.getLogger(__name__)


def _article_to_dict(article: Any) -> dict[str, Any]:
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


def _get_articles_from_rds(
    limit: int | None = None,
    require_embedding: bool = True,
) -> list[dict[str, Any]]:
    """Return articles from RDS, optionally filtering to those with embeddings."""
    load_dotenv()

    from rds_postgres.connection import get_session
    from rds_postgres.models import Article

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


def _get_articles_from_local(path: str | Path) -> list[dict[str, Any]]:
    """Return articles from a local parquet file."""
    import pyarrow.parquet as pq

    path = Path(path)
    logger.info("Loading articles from parquet: %s", path)
    table = pq.read_table(path)
    articles = table.to_pylist()
    logger.info("Loaded %d articles", len(articles))
    return articles


def get_articles(
    limit: int | None = None,
    require_embedding: bool = True,
    path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Return articles from local parquet or RDS."""
    if path:
        return _get_articles_from_local(path)
    return _get_articles_from_rds(limit=limit, require_embedding=require_embedding)
