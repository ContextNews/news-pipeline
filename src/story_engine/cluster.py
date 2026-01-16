"""Cluster articles into stories with HDBSCAN."""

from __future__ import annotations

import argparse
from collections import defaultdict
import logging
from typing import Any

import hdbscan
import numpy as np

from story_engine.get_articles import get_articles

logger = logging.getLogger(__name__)


def _prepare_embeddings(articles: list[dict[str, Any]]) -> tuple[np.ndarray, list[dict[str, Any]]]:
    embeddings = []
    kept_articles = []

    for article in articles:
        embedding = article.get("embedding")
        if not embedding:
            continue
        embeddings.append(embedding)
        kept_articles.append(article)

    if not embeddings:
        return np.empty((0, 0), dtype="float32"), []

    return np.asarray(embeddings, dtype="float32"), kept_articles


def cluster_articles(
    articles: list[dict[str, Any]],
    min_cluster_size: int = 5,
    min_samples: int | None = None,
) -> dict[int, list[dict[str, Any]]]:
    """Cluster articles and return label -> article list."""
    vectors, kept_articles = _prepare_embeddings(articles)
    if vectors.size == 0:
        logger.warning("No embeddings available for clustering")
        return {}

    logger.info(
        "Clustering %d articles (min_cluster_size=%d, min_samples=%s)",
        len(kept_articles),
        min_cluster_size,
        min_samples,
    )
    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=min_samples,
    )
    labels = clusterer.fit_predict(vectors)

    clusters: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for label, article in zip(labels, kept_articles, strict=True):
        clusters[int(label)].append(article)

    logger.info("Built %d clusters (%d noise)", len(clusters), len(clusters.get(-1, [])))
    return clusters


def _print_clusters(clusters: dict[int, list[dict[str, Any]]]) -> None:
    if not clusters:
        print("No clusters found (missing embeddings or too few articles).")
        return

    for label in sorted(clusters, key=lambda value: (value == -1, value)):
        articles = clusters[label]
        header = "Noise" if label == -1 else f"Cluster {label}"
        print(f"{header} ({len(articles)} articles)")
        for article in articles:
            title = article.get("title") or "(untitled)"
            print(f"- {article.get('id')} - {article.get('source')} : {title}")
        print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Cluster articles into stories.")
    parser.add_argument("--min-cluster-size", type=int, default=5)
    parser.add_argument("--min-samples", type=int, default=None)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    articles = get_articles(limit=args.limit, require_embedding=True)
    clusters = cluster_articles(
        articles,
        min_cluster_size=args.min_cluster_size,
        min_samples=args.min_samples,
    )
    _print_clusters(clusters)


if __name__ == "__main__":
    main()
