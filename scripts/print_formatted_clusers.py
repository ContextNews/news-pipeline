"""Print formatted clusters and their articles from the database."""

from __future__ import annotations

import logging

from dotenv import load_dotenv


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger(__name__)

    load_dotenv()

    from sqlalchemy import text

    from rds_postgres.connection import get_session

    stmt = text(
        """
        SELECT
            ac.article_cluster_id,
            aca.article_id,
            a.title,
            a.source,
            a.published_at
        FROM article_clusters ac
        JOIN article_cluster_articles aca
            ON aca.article_cluster_id = ac.article_cluster_id
        JOIN articles a
            ON a.id = aca.article_id
        ORDER BY ac.article_cluster_id, a.published_at DESC, a.id
        """
    )

    with get_session() as session:
        rows = session.execute(stmt).mappings().all()

    if not rows:
        logger.info("No clusters found")
        return

    logger.info("Fetched %d cluster rows", len(rows))
    current_cluster = None
    for row in rows:
        cluster_id = row["article_cluster_id"]
        if cluster_id != current_cluster:
            if current_cluster is not None:
                print()
            print(f"Cluster: {cluster_id}")
            current_cluster = cluster_id

        published_at = row["published_at"]
        if published_at is not None:
            published_at = str(published_at)
        else:
            published_at = "None"

        print(
            f"- {row['article_id']} | {row['source']} | {published_at} | {row['title']}"
        )


if __name__ == "__main__":
    main()
