"""Main clustering pipeline."""

import logging

from news_cluster.cluster.hdbscan import cluster_articles, get_cluster_stats
from news_cluster.cluster.stories import build_stories
from news_cluster.config_loader import ClusterConfig
from news_cluster.io.read_parquet import load_all_articles
from news_cluster.io.write_output import write_output

logger = logging.getLogger(__name__)


def run_pipeline(config: ClusterConfig) -> dict:
    """Run the clustering pipeline.

    Args:
        config: Cluster configuration

    Returns:
        Dict with pipeline statistics
    """
    # Load articles
    logger.info("Loading articles for date range: %s", config.date_range)
    articles = load_all_articles(config)
    logger.info("Loaded %d articles", len(articles))

    if not articles:
        logger.warning("No articles found, skipping clustering")
        return {
            "articles_loaded": 0,
            "stories_created": 0,
            "articles_clustered": 0,
            "articles_noise": 0,
        }

    # Filter articles with embeddings
    articles_with_embeddings = [
        a for a in articles if a.get("embedding_combined") is not None
    ]
    logger.info(
        "%d/%d articles have embeddings",
        len(articles_with_embeddings),
        len(articles),
    )

    if len(articles_with_embeddings) < config.min_cluster_size:
        logger.warning(
            "Not enough articles with embeddings (%d < %d), skipping clustering",
            len(articles_with_embeddings),
            config.min_cluster_size,
        )
        return {
            "articles_loaded": len(articles),
            "articles_with_embeddings": len(articles_with_embeddings),
            "stories_created": 0,
            "articles_clustered": 0,
            "articles_noise": 0,
        }

    # Run clustering
    logger.info(
        "Running HDBSCAN (min_cluster_size=%d, min_samples=%d)",
        config.min_cluster_size,
        config.min_samples,
    )
    cluster_labels, embeddings = cluster_articles(articles_with_embeddings, config)

    stats = get_cluster_stats(cluster_labels)
    logger.info(
        "Clustering complete: %d clusters, %d clustered, %d noise",
        stats["n_clusters"],
        stats["n_clustered"],
        stats["n_noise"],
    )

    # Build stories
    stories, article_maps, story_articles = build_stories(
        articles_with_embeddings, cluster_labels, embeddings
    )
    logger.info("Built %d stories", len(stories))

    # Write output
    stories_path, maps_path, story_articles_path = write_output(
        config, stories, article_maps, story_articles
    )
    logger.info("Wrote stories to: %s", stories_path)
    logger.info("Wrote article-story map to: %s", maps_path)
    logger.info("Wrote story articles to: %s", story_articles_path)

    return {
        "articles_loaded": len(articles),
        "articles_with_embeddings": len(articles_with_embeddings),
        "stories_created": len(stories),
        "articles_clustered": stats["n_clustered"],
        "articles_noise": stats["n_noise"],
        "stories_path": stories_path,
        "maps_path": maps_path,
        "story_articles_path": story_articles_path,
    }
