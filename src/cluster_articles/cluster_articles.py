"""Cluster articles into groups using HDBSCAN."""

from __future__ import annotations

import json
import logging
from typing import Any

import hdbscan
import numpy as np

logger = logging.getLogger(__name__)


def _coerce_embedding(value: Any) -> list[float] | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return None
        if isinstance(parsed, list):
            return [float(v) for v in parsed]
        return None
    if hasattr(value, "tolist"):
        try:
            return [float(v) for v in value.tolist()]
        except TypeError:
            return None
    if isinstance(value, (list, tuple)):
        return [float(v) for v in value]
    return None


def _prepare_embeddings(articles: list[dict[str, Any]]) -> tuple[np.ndarray, list[dict[str, Any]]]:
    vectors = []
    kept = []
    for article in articles:
        embedding = _coerce_embedding(article.get("embedding"))
        if not embedding:
            continue
        vectors.append(embedding)
        kept.append(article)

    if not vectors:
        return np.empty((0, 0), dtype="float32"), []

    return np.asarray(vectors, dtype="float32"), kept


def cluster_articles(
    articles: list[dict[str, Any]],
    min_cluster_size: int = 5,
    min_samples: int | None = None,
) -> list[dict[str, Any]]:
    """Assign cluster labels to articles and return updated records."""
    vectors, kept_articles = _prepare_embeddings(articles)
    if vectors.size == 0:
        logger.warning("No embeddings available for clustering")
        return []

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

    results = []
    for label, article in zip(labels, kept_articles, strict=True):
        record = dict(article)
        record["cluster_id"] = int(label)
        results.append(record)

    noise = sum(1 for record in results if record["cluster_id"] == -1)
    logger.info("Built %d clusters (%d noise)", len(set(labels)), noise)
    return results
