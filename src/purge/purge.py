"""Core pruning logic for old pipeline data."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

logger = logging.getLogger(__name__)

# Tables to delete rows from, filtered by article.published_at via subquery.
# Order matters: children before parents where FKs exist.
_ARTICLE_DEPENDENT_TABLES = [
    "article_embeddings",
    "article_entity_mentions",
    "article_entities_resolved",
    "article_topics",
    "article_cluster_articles",
]


def purge(session, retention_days: int, dry_run: bool) -> dict[str, int]:
    """
    Prune intermediate pipeline data for articles older than retention_days.

    Keeps article rows intact (referenced by story_articles) but nulls out
    the text column and removes all intermediate table rows.

    Returns a dict of {table: row_count} for each operation performed.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    cutoff_naive = cutoff.replace(tzinfo=None)
    counts: dict[str, int] = {}

    logger.info(
        "%s data older than %d days (cutoff: %s)",
        "Counting" if dry_run else "Pruning",
        retention_days,
        cutoff.date().isoformat(),
    )

    # Null out articles.text
    if dry_run:
        result = session.execute(
            text("SELECT COUNT(*) FROM articles WHERE published_at < :cutoff AND text IS NOT NULL"),
            {"cutoff": cutoff_naive},
        )
        counts["articles.text"] = result.scalar()
    else:
        result = session.execute(
            text("UPDATE articles SET text = NULL WHERE published_at < :cutoff AND text IS NOT NULL"),
            {"cutoff": cutoff_naive},
        )
        counts["articles.text"] = result.rowcount
        session.commit()

    # Delete rows from intermediate tables
    old_article_ids_subquery = "SELECT id FROM articles WHERE published_at < :cutoff"
    for table in _ARTICLE_DEPENDENT_TABLES:
        if dry_run:
            result = session.execute(
                text(f"SELECT COUNT(*) FROM {table} WHERE article_id IN ({old_article_ids_subquery})"),
                {"cutoff": cutoff_naive},
            )
            counts[table] = result.scalar()
        else:
            result = session.execute(
                text(f"DELETE FROM {table} WHERE article_id IN ({old_article_ids_subquery})"),
                {"cutoff": cutoff_naive},
            )
            counts[table] = result.rowcount
            session.commit()

    # Sweep orphaned clusters (no remaining article members)
    if dry_run:
        result = session.execute(
            text("""
                SELECT COUNT(*) FROM article_clusters
                WHERE id NOT IN (SELECT DISTINCT article_cluster_id FROM article_cluster_articles)
            """)
        )
        counts["article_clusters"] = result.scalar()
    else:
        result = session.execute(
            text("""
                DELETE FROM article_clusters
                WHERE id NOT IN (SELECT DISTINCT article_cluster_id FROM article_cluster_articles)
            """)
        )
        counts["article_clusters"] = result.rowcount
        session.commit()

    return counts


def vacuum_tables(engine) -> None:
    """Run VACUUM ANALYZE on the pruned tables to reclaim storage."""
    tables = ["articles"] + _ARTICLE_DEPENDENT_TABLES + ["article_clusters"]
    table_list = ", ".join(tables)
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        logger.info("Running VACUUM ANALYZE on pruned tables")
        conn.execute(text(f"VACUUM ANALYZE {table_list}"))
    logger.info("VACUUM ANALYZE complete")
