"""Clear all rows from article_embeddings."""

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
        result = session.execute(text("DELETE FROM article_embeddings"))
        session.commit()

    logger.info("Deleted %d rows from article_embeddings", result.rowcount or 0)


if __name__ == "__main__":
    main()
