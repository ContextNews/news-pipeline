"""Load extracted articles into RDS PostgreSQL."""

import logging
from typing import Any

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from rds_postgres.models import Article, Entity, ArticleEntity

logger = logging.getLogger(__name__)


def load_articles(articles: list[dict[str, Any]], session: Session) -> tuple[int, int, int]:
    """
    Load extracted articles into the database.

    Args:
        articles: List of extracted article dicts from S3
        session: SQLAlchemy session

    Returns:
        Tuple of (articles_loaded, entities_loaded, article_entities_loaded)
    """
    articles_loaded = 0
    entities_loaded = 0
    article_entities_loaded = 0

    for article_data in articles:
        article_id = article_data["id"]
        entities_data = article_data.get("entities", [])

        # Insert article (upsert on conflict)
        article_stmt = insert(Article).values(
            id=article_id,
            source=article_data["source"],
            title=article_data["title"],
            summary=article_data["summary"],
            url=article_data["url"],
            published_at=article_data["published_at"],
            ingested_at=article_data["ingested_at"],
            text=article_data.get("text"),
            embedded_text=article_data.get("embedded_text"),
            embedding=article_data.get("embedding"),
            embedding_model=article_data.get("embedding_model"),
        ).on_conflict_do_update(
            index_elements=["id"],
            set_={
                "source": article_data["source"],
                "title": article_data["title"],
                "summary": article_data["summary"],
                "url": article_data["url"],
                "published_at": article_data["published_at"],
                "ingested_at": article_data["ingested_at"],
                "text": article_data.get("text"),
                "embedded_text": article_data.get("embedded_text"),
                "embedding": article_data.get("embedding"),
                "embedding_model": article_data.get("embedding_model"),
            }
        )
        session.execute(article_stmt)
        articles_loaded += 1

        # Insert entities and article_entity relationships
        for entity_data in entities_data:
            entity_type = entity_data["type"]
            entity_name = entity_data["name"]

            # Insert entity (upsert on conflict)
            entity_stmt = insert(Entity).values(
                type=entity_type,
                name=entity_name,
            ).on_conflict_do_nothing(
                index_elements=["type", "name"]
            )
            result = session.execute(entity_stmt)
            if result.rowcount > 0:
                entities_loaded += 1

            # Insert article_entity relationship (upsert on conflict)
            article_entity_stmt = insert(ArticleEntity).values(
                article_id=article_id,
                entity_type=entity_type,
                entity_name=entity_name,
            ).on_conflict_do_nothing(
                index_elements=["article_id", "entity_type", "entity_name"]
            )
            result = session.execute(article_entity_stmt)
            if result.rowcount > 0:
                article_entities_loaded += 1

    session.commit()

    return articles_loaded, entities_loaded, article_entities_loaded
