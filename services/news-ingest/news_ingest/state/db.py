"""State management for tracking ingestion progress."""

import os
from datetime import datetime, timezone
from contextlib import contextmanager

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

from news_ingest.config import Config

load_dotenv()

# In-memory state storage (used when backend="memory")
_memory_state: dict[str, datetime] = {}


def get_connection():
    """Create a new database connection from DATABASE_URL."""
    return psycopg2.connect(os.environ["DATABASE_URL"])


@contextmanager
def get_cursor():
    """Context manager for database cursor with automatic commit/rollback."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def ensure_table(config: Config) -> None:
    """Create the source_state table if it doesn't exist (postgres only)."""
    if config.state_backend != "postgres":
        return

    with get_cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS source_state (
                source_id TEXT PRIMARY KEY,
                last_fetched_at TIMESTAMP WITH TIME ZONE NOT NULL
            )
        """)


def get_last_fetched_at(source_id: str, config: Config) -> datetime | None:
    """Get the last fetched timestamp for a source.

    Returns None if the source has never been fetched.
    """
    if config.state_backend == "memory":
        return _memory_state.get(source_id)

    with get_cursor() as cur:
        cur.execute(
            "SELECT last_fetched_at FROM source_state WHERE source_id = %s",
            (source_id,)
        )
        row = cur.fetchone()
        if row is None:
            return None
        return row["last_fetched_at"]


def update_last_fetched_at(source_id: str, timestamp: datetime, config: Config) -> None:
    """Update the last fetched timestamp for a source.

    Uses upsert to handle both new and existing sources.
    """
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    if config.state_backend == "memory":
        _memory_state[source_id] = timestamp
        return

    with get_cursor() as cur:
        cur.execute("""
            INSERT INTO source_state (source_id, last_fetched_at)
            VALUES (%s, %s)
            ON CONFLICT (source_id)
            DO UPDATE SET last_fetched_at = EXCLUDED.last_fetched_at
        """, (source_id, timestamp))
