"""Clear all rows from cluster tables."""

from __future__ import annotations

import logging

from dotenv import load_dotenv


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger(__name__)

    load_dotenv()

    from sqlalchemy import text

    from rds_postgres.connection import get_session

    with get_session() as session:
        rel_result = session.execute(text("DELETE FROM article_cluster_articles"))
        cluster_result = session.execute(text("DELETE FROM article_clusters"))
        session.commit()

    logger.info(
        "Deleted %d rows from article_cluster_articles and %d rows from article_clusters",
        rel_result.rowcount or 0,
        cluster_result.rowcount or 0,
    )


if __name__ == "__main__":
    main()
