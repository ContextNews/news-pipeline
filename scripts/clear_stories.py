"""Clear all rows from story tables."""

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
        rel_result = session.execute(text("DELETE FROM article_stories"))
        story_result = session.execute(text("DELETE FROM stories"))
        session.commit()

    logger.info(
        "Deleted %d rows from article_stories and %d rows from stories",
        rel_result.rowcount or 0,
        story_result.rowcount or 0,
    )


if __name__ == "__main__":
    main()
