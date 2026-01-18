"""Print rows from a specified database table."""

from __future__ import annotations

import argparse
import logging

from dotenv import load_dotenv


def _format_value(value: object, max_len: int = 100) -> str:
    if isinstance(value, str) and len(value) > max_len:
        return f"{value[:max_len]}..."
    return str(value) if value is not None else "None"


def main() -> None:
    parser = argparse.ArgumentParser(description="Print rows from a table.")
    parser.add_argument("--table", required=True, help="Table name to print")
    parser.add_argument("--limit", type=int, default=50, help="Max rows to print")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger(__name__)

    load_dotenv()

    from sqlalchemy import text

    from rds_postgres.connection import get_session

    table_name = args.table.strip()
    if not table_name.replace("_", "").isalnum():
        raise SystemExit("Invalid table name. Use only letters, numbers, and underscores.")

    stmt = text(f"SELECT * FROM {table_name} LIMIT :limit")

    with get_session() as session:
        rows = session.execute(stmt, {"limit": args.limit}).mappings().all()

    logger.info("Fetched %d rows from %s", len(rows), table_name)
    for row in rows:
        formatted = {key: _format_value(value) for key, value in dict(row).items()}
        for key in sorted(formatted.keys()):
            print(f"{key}: {formatted[key]}")
        print("-" * 40)


if __name__ == "__main__":
    main()
