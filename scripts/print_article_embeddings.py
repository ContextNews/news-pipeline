"""Print rows from the article_embeddings table."""

from __future__ import annotations

import argparse
import json
import logging

from dotenv import load_dotenv


def main() -> None:
    parser = argparse.ArgumentParser(description="Print rows from article_embeddings.")
    parser.add_argument("--limit", type=int, default=50, help="Max rows to print")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger(__name__)

    load_dotenv()

    from sqlalchemy import text

    from rds_postgres.connection import get_session

    stmt = text(
        """
        SELECT
            article_id,
            embedded_text,
            embedding,
            embedding_model
        FROM article_embeddings
        ORDER BY article_id
        LIMIT :limit
        """
    )

    with get_session() as session:
        rows = session.execute(stmt, {"limit": args.limit}).mappings().all()

    logger.info("Fetched %d rows from article_embeddings", len(rows))
    for row in rows:
        print(json.dumps(dict(row), default=str, ensure_ascii=False))


if __name__ == "__main__":
    main()
