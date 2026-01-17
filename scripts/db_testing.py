"""Print counts of articles and entities in the RDS database."""

from __future__ import annotations

import logging

from dotenv import load_dotenv
from sqlalchemy import func, select

load_dotenv()

from rds_postgres.connection import get_session
from rds_postgres.models import Article, Entity

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_counts() -> tuple[int, int]:
    """Return (article_count, entity_count)."""
    with get_session() as session:
        article_count = session.execute(select(func.count()).select_from(Article)).scalar_one()
        entity_count = session.execute(select(func.count()).select_from(Entity)).scalar_one()
    return article_count, entity_count


def main() -> None:
    article_count, entity_count = get_counts()
    print(f"Articles: {article_count}")
    print(f"Entities: {entity_count}")


if __name__ == "__main__":
    main()
