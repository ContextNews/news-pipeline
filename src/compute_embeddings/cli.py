"""CLI for computing embeddings."""

from __future__ import annotations

import logging

from dotenv import load_dotenv
from rds_postgres.connection import get_session

from compute_embeddings.compute_embeddings import compute_embeddings
from compute_embeddings.helpers import parse_compute_embeddings_args
from common.aws import load_ingested_articles, upload_embeddings, upload_jsonl_records_to_s3
from common.cli_helpers import setup_logging
from common.local_io import save_jsonl_records_local

load_dotenv()

setup_logging()
logger = logging.getLogger(__name__)


def main() -> None:
    args = parse_compute_embeddings_args()
    articles = load_ingested_articles(args.published_date, args.model, args.overwrite)

    if not articles:
        logger.warning("No articles to process")
        return

    # Compute embeddings
    embedded_articles = compute_embeddings(
        articles=articles,
        model=args.model,
        batch_size=args.batch_size,
        embed_title=not args.no_title,
        embed_summary=not args.no_summary,
        embed_text=not args.no_text,
        word_limit=args.word_limit,
    )

    if not embedded_articles:
        logger.warning("No articles embedded")
        return

    if args.load_s3:
        upload_jsonl_records_to_s3(embedded_articles, "embedded_articles")

    if args.load_local:
        save_jsonl_records_local(embedded_articles, "embedded_articles")

    if args.load_rds:
        with get_session() as session:
            upload_embeddings(embedded_articles, session)


if __name__ == "__main__":
    main()
