"""CLI for pruning old intermediate pipeline data from RDS."""

from __future__ import annotations

import argparse
import logging

from dotenv import load_dotenv
from context_db.connection import get_session

from purge.purge import purge, vacuum_tables
from common.cli_helpers import setup_logging

load_dotenv()

setup_logging()
logger = logging.getLogger(__name__)


def main() -> None:
    args = _parse_args()

    if args.dry_run:
        logger.info("DRY RUN — no data will be deleted")

    counts = {}
    with get_session() as session:
        counts = purge(session, args.retention_days, dry_run=args.dry_run)

    action = "Would delete/null" if args.dry_run else "Deleted/nulled"
    for table, count in counts.items():
        logger.info("%s %d rows in %s", action, count, table)

    if not args.dry_run and args.vacuum:
        from context_db.connection import engine
        vacuum_tables(engine)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prune intermediate pipeline data older than a retention window."
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=30,
        help="Delete intermediate data for articles older than this many days (default: 30)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print row counts that would be affected without making any changes",
    )
    parser.add_argument(
        "--no-vacuum",
        dest="vacuum",
        action="store_false",
        default=True,
        help="Skip VACUUM ANALYZE after pruning",
    )
    return parser.parse_args()
