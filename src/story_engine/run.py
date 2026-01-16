"""CLI entry point for story clustering."""

import argparse
import logging
from dotenv import load_dotenv

load_dotenv()

from story_engine.cluster import cluster_articles, get_articles, _print_clusters


def main() -> None:
    parser = argparse.ArgumentParser(description="Cluster articles into stories.")
    parser.add_argument("--min-cluster-size", type=int, default=2)
    parser.add_argument("--min-samples", type=int, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--path",
        type=str,
        default="tests/data/articles_2026_01_12_14_19.parquet",
        help="Optional local parquet file path with articles.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    articles = get_articles(limit=args.limit, require_embedding=True, path=args.path)
    clusters = cluster_articles(
        articles,
        min_cluster_size=args.min_cluster_size,
        min_samples=args.min_samples,
    )
    _print_clusters(clusters)


if __name__ == "__main__":
    main()
