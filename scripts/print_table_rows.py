"""Print row counts for all tables in the database."""

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
        tables = session.execute(
            text(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name
                """
            )
        ).scalars().all()

        if not tables:
            logger.info("No tables found")
            return

        for table in tables:
            count = session.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one()
            print(f"{table}: {count}")


if __name__ == "__main__":
    main()
