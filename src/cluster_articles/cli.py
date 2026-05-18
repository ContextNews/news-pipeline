"""CLI for clustering articles."""

from __future__ import annotations

import logging

from dotenv import load_dotenv
from context_db.connection import get_session

from cluster_articles.cluster_articles import cluster_articles
from cluster_articles.helpers import parse_cluster_articles_args
from common.db_io import load_articles_with_embeddings, upload_clusters
from common.object_storage import upload_jsonl_records_to_object_store
from common.cli_helpers import setup_logging
from common.local_io import save_jsonl_records_local

load_dotenv()

setup_logging()
logger = logging.getLogger(__name__)


def main() -> None:

    # Parse CLI arguments
    args = parse_cluster_articles_args()
    articles = load_articles_with_embeddings(args.ingested_date, args.embedding_model)

    if not articles:
        logger.warning("No articles to cluster")
        return

    # Cluster articles
    clustered = cluster_articles(
        articles,
        min_cluster_size=args.min_cluster_size,
        min_samples=args.min_samples,
    )

    if not clustered:
        logger.warning("No clusters produced")
        return

    if args.load_object_store:
        upload_jsonl_records_to_object_store(clustered, "clustered_articles")

    if args.load_local:
        save_jsonl_records_local(clustered, "clustered_articles")

    if args.load_db:
        with get_session() as session:
            upload_clusters(clustered, session, args.ingested_date, args.overwrite)


if __name__ == "__main__":
    main()
