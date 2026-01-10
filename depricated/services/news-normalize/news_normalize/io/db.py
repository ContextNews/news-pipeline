"""Database operations for storing normalized articles."""

import os
import logging
from contextlib import contextmanager
from datetime import timezone

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

from news_normalize.schema import NormalizedArticle

load_dotenv()
logger = logging.getLogger(__name__)


def get_connection():
    """Create a new database connection from DATABASE_URL."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is required")
    return psycopg2.connect(database_url)


@contextmanager
def get_cursor():
    """Context manager for database cursor with automatic commit/rollback."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _article_to_row(article: NormalizedArticle) -> tuple:
    """Convert NormalizedArticle to database row tuple."""
    published_at = article.published_at
    if published_at and published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=timezone.utc)

    fetched_at = article.fetched_at
    if fetched_at and fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=timezone.utc)

    return (
        article.article_id,
        article.source,
        article.headline,
        article.url,
        published_at,
        fetched_at,
    )


def write_articles_to_db(articles: list[NormalizedArticle], batch_size: int = 100) -> int:
    """Write normalized articles to database with upsert logic.

    Uses ON CONFLICT to handle duplicate article_ids gracefully.

    Args:
        articles: List of normalized articles to insert
        batch_size: Number of articles per batch insert

    Returns:
        Number of articles inserted/updated
    """
    if not articles:
        return 0

    logger.info(f"Writing {len(articles)} articles to database")
    total = 0

    with get_cursor() as cur:
        for i in range(0, len(articles), batch_size):
            batch = articles[i:i + batch_size]
            rows = [_article_to_row(a) for a in batch]

            execute_values(
                cur,
                """
                INSERT INTO articles (id, source, headline, url, published_at, fetched_at)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    source = EXCLUDED.source,
                    headline = EXCLUDED.headline,
                    url = EXCLUDED.url,
                    published_at = EXCLUDED.published_at,
                    fetched_at = EXCLUDED.fetched_at,
                    updated_at = NOW()
                """,
                rows,
            )
            total += len(batch)

    logger.info(f"Successfully wrote {total} articles to database")
    return total
