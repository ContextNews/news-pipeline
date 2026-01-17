"""Remove all rows from article and entity tables."""

from __future__ import annotations

import logging

from dotenv import load_dotenv
from sqlalchemy import delete

load_dotenv()

from rds_postgres.connection import get_session
from rds_postgres.models import Article, Entity, ArticleEntity

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def clear_db() -> None:
    with get_session() as session:
        article_entities_deleted = session.execute(delete(ArticleEntity)).rowcount or 0
        articles_deleted = session.execute(delete(Article)).rowcount or 0
        entities_deleted = session.execute(delete(Entity)).rowcount or 0
        session.commit()

    logger.info(
        "Deleted %d article_entities, %d articles, %d entities",
        article_entities_deleted,
        articles_deleted,
        entities_deleted,
    )


def main() -> None:
    clear_db()


if __name__ == "__main__":
    main()
