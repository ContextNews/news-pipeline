"""CLI for clustering articles."""

from __future__ import annotations

import logging

from dotenv import load_dotenv
from rds_postgres.connection import get_session

from cluster_articles.cluster_articles import cluster_articles
from cluster_articles.helpers import parse_cluster_articles_args
from common.aws import load_articles_with_embeddings, upload_clusters, upload_jsonl_records_to_s3
from common.cli_helpers import setup_logging
from common.local_io import save_jsonl_records_local

load_dotenv()

setup_logging()
logger = logging.getLogger(__name__)


def main() -> None:
    args = parse_cluster_articles_args()
    articles = load_articles_with_embeddings(args.ingested_date, args.embedding_model)

    if not articles:
        logger.warning("No articles to cluster")
        return

    clustered = cluster_articles(
        articles,
        min_cluster_size=args.min_cluster_size,
        min_samples=args.min_samples,
    )

    if not clustered:
        logger.warning("No clusters produced")
        return

    if args.load_s3:
        upload_jsonl_records_to_s3(clustered, "clustered_articles")

    if args.load_local:
        save_jsonl_records_local(clustered, "clustered_articles")

    if args.load_rds:
        with get_session() as session:
            upload_clusters(clustered, session, args.ingested_date, args.overwrite)


if __name__ == "__main__":
    main()
