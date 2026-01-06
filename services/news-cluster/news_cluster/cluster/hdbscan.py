"""HDBSCAN clustering for news articles."""

from typing import Any

import hdbscan
import numpy as np

from news_cluster.config_loader import ClusterConfig


def normalize_embeddings(embeddings: np.ndarray) -> np.ndarray:
    """L2-normalize embeddings for cosine distance."""
    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    # Avoid division by zero
    norms = np.where(norms == 0, 1, norms)
    return embeddings / norms


def cluster_articles(
    articles: list[dict[str, Any]], config: ClusterConfig
) -> tuple[np.ndarray, np.ndarray]:
    """Cluster articles using HDBSCAN.

    Args:
        articles: List of article dicts with 'embedding_combined' field
        config: Cluster configuration

    Returns:
        Tuple of (cluster_labels, embeddings_matrix)
        - cluster_labels: Array of cluster assignments (-1 for noise)
        - embeddings_matrix: L2-normalized embedding matrix
    """
    # Extract embeddings
    embeddings = []
    for article in articles:
        emb = article.get("embedding_combined")
        if emb is None:
            raise ValueError(
                f"Article {article.get('article_id', 'unknown')} missing embedding_combined"
            )
        embeddings.append(emb)

    embeddings_matrix = np.array(embeddings, dtype=np.float32)

    # L2-normalize embeddings
    # On unit vectors, euclidean distance is equivalent to cosine distance:
    # ||a - b||² = 2(1 - cos(a,b))
    embeddings_matrix = normalize_embeddings(embeddings_matrix)

    # Run HDBSCAN with euclidean metric (equivalent to cosine on L2-normalized vectors)
    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=config.min_cluster_size,
        min_samples=config.min_samples,
        metric="euclidean",
        cluster_selection_method="eom",  # Excess of Mass
    )

    cluster_labels = clusterer.fit_predict(embeddings_matrix)

    return cluster_labels, embeddings_matrix


def get_cluster_stats(labels: np.ndarray) -> dict[str, int]:
    """Get statistics about clustering results."""
    unique_labels = set(labels)
    n_clusters = len(unique_labels) - (1 if -1 in unique_labels else 0)
    n_noise = int(np.sum(labels == -1))
    n_clustered = len(labels) - n_noise

    return {
        "n_clusters": n_clusters,
        "n_clustered": n_clustered,
        "n_noise": n_noise,
        "total": len(labels),
    }
