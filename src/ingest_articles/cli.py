"""CLI for ingesting and cleaning articles."""

from __future__ import annotations

import logging

from dotenv import load_dotenv
from rds_postgres.connection import get_session

from ingest_articles.ingest_articles import ingest_articles
from ingest_articles.helpers import parse_sources, parse_ingest_articles_args
from common.aws import upload_jsonl_records_to_s3, upload_articles
from common.cli_helpers import setup_logging
from common.local_io import save_jsonl_records_local

load_dotenv()

setup_logging()
logger = logging.getLogger(__name__)


def main() -> None:
    '''
    Main function to ingest articles and optionally load them to S3, RDS, or local storage.

    CLI Arguments:
        --lookback-hours: Number of hours to look back for articles (default: 12)
        --sources: Comma-separated list of RSS sources to fetch (default: all)
        --load-s3: Upload ingested articles to S3
        --load-rds: Upload ingested articles to RDS
        --load-local: Save ingested articles to local JSONL file
    '''

    # Parse CLI arguments
    args = parse_ingest_articles_args()
    sources = parse_sources(args.sources)

    # Ingest and clean articles
    ingested_articles = ingest_articles(
        sources=sources,
        lookback_hours=args.lookback_hours,
    )

    if not ingested_articles:
        logger.warning("No articles ingested. Exiting.")
        return

    if args.load_s3:
        upload_jsonl_records_to_s3(ingested_articles, "ingested_articles")

    if args.load_local:
        save_jsonl_records_local(ingested_articles, "ingested_articles")

    if args.load_rds:
        with get_session() as session:
            upload_articles(ingested_articles, session)


if __name__ == "__main__":
    main()
