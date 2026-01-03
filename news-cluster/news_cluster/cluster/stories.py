"""Story aggregation from clustered articles."""

import hashlib
from collections import Counter
from datetime import datetime
from typing import Any

import numpy as np

from news_cluster.schema import (
    ArticleStoryMap,
    ArticleSummary,
    Entity,
    HierarchicalLocation,
    Location,
    Story,
    StoryArticles,
    SubLocation,
)


def generate_story_id(cluster_label: int, article_ids: list[str]) -> str:
    """Generate a deterministic story ID from cluster label and article IDs."""
    # Sort article IDs for determinism
    sorted_ids = sorted(article_ids)
    content = f"{cluster_label}:{','.join(sorted_ids)}"
    hash_bytes = hashlib.sha256(content.encode()).hexdigest()[:8]
    return f"story_{hash_bytes}"


def select_story_title(
    articles: list[dict[str, Any]],
    embeddings: np.ndarray,
    article_indices: list[int],
) -> str:
    """Select the best title for a story.

    Strategy:
    1. Compute cluster centroid
    2. Find article closest to centroid
    3. Use its headline as the story title
    """
    if not article_indices:
        return "Untitled Story"

    # Get embeddings for this cluster
    cluster_embeddings = embeddings[article_indices]

    # Compute centroid
    centroid = np.mean(cluster_embeddings, axis=0)
    centroid = centroid / np.linalg.norm(centroid)  # Normalize

    # Find article closest to centroid (highest cosine similarity)
    similarities = cluster_embeddings @ centroid
    best_idx = int(np.argmax(similarities))
    best_article_idx = article_indices[best_idx]

    # Get headline from that article
    article = articles[best_article_idx]
    headline = article.get("headline") or article.get("title") or "Untitled Story"

    return headline


def aggregate_entities(articles: list[dict[str, Any]], indices: list[int]) -> list[Entity]:
    """Aggregate entities across articles in a cluster."""
    entity_counts: Counter[tuple[str, str]] = Counter()

    for idx in indices:
        article = articles[idx]
        entities = article.get("entities", [])
        for ent in entities:
            if isinstance(ent, dict):
                key = (ent.get("text", ""), ent.get("label", ""))
                entity_counts[key] += 1

    # Convert to Entity objects, sorted by count
    top_entities = []
    for (text, ent_type), count in entity_counts.most_common(10):
        if text:
            top_entities.append(Entity(text=text, type=ent_type, count=count))

    return top_entities


def aggregate_locations_hierarchical(
    articles: list[dict[str, Any]],
    indices: list[int],
    min_confidence: float = 0.75,
    max_locations: int = 10,
) -> list[HierarchicalLocation]:
    """Aggregate locations into hierarchical country-based structure.

    Cities and regions contribute to their parent country's confidence.
    Only returns locations with confidence >= min_confidence.

    Args:
        articles: List of article dicts
        indices: Indices of articles in this cluster
        min_confidence: Minimum confidence threshold (default 0.75)
        max_locations: Maximum number of countries to return

    Returns:
        List of HierarchicalLocation objects
    """
    # Track data per country
    # country_code -> {country_name, country_mentions, regions, cities, articles_with_mentions, headline_mentions}
    country_data: dict[str, dict[str, Any]] = {}
    total_articles = len(indices)

    for idx in indices:
        article = articles[idx]
        locations = article.get("locations", [])
        headline = (article.get("headline") or "").lower()

        # Track which countries this article mentions (for coverage calculation)
        countries_in_article: set[str] = set()

        for loc in locations:
            if not isinstance(loc, dict):
                continue

            name = loc.get("name", "")
            country_code = loc.get("country_code")
            loc_type = loc.get("type", "unknown")
            parent_region = loc.get("parent_region")

            if not name or not country_code:
                continue  # Skip locations without country association

            # Initialize country bucket if needed
            if country_code not in country_data:
                country_data[country_code] = {
                    "country_name": None,
                    "country_mentions": 0,
                    "regions": {},  # region_name -> mention_count
                    "cities": {},   # city_name -> mention_count
                    "articles_with_mentions": 0,
                    "headline_mentions": 0,
                }

            data = country_data[country_code]
            countries_in_article.add(country_code)

            # Check if this location is mentioned in headline
            in_headline = name.lower() in headline

            if loc_type == "country":
                data["country_name"] = name
                data["country_mentions"] += 1
                if in_headline:
                    data["headline_mentions"] += 1
            elif loc_type == "region":
                if name not in data["regions"]:
                    data["regions"][name] = 0
                data["regions"][name] += 1
                if in_headline:
                    data["headline_mentions"] += 1
            elif loc_type == "city":
                if name not in data["cities"]:
                    data["cities"][name] = 0
                data["cities"][name] += 1
                if in_headline:
                    data["headline_mentions"] += 1

        # Update article coverage for each country mentioned in this article
        for cc in countries_in_article:
            country_data[cc]["articles_with_mentions"] += 1

    # Calculate confidence and build HierarchicalLocation objects
    results: list[HierarchicalLocation] = []

    for country_code, data in country_data.items():
        # Get country name (from direct mentions or look up)
        country_name = data["country_name"]
        if not country_name:
            # Try to get name from pycountry
            try:
                import pycountry
                country = pycountry.countries.get(alpha_2=country_code)
                if country:
                    country_name = getattr(country, "common_name", country.name)
                else:
                    country_name = country_code
            except Exception:
                country_name = country_code

        # Calculate total mentions (country + regions + cities)
        region_mentions = sum(data["regions"].values())
        city_mentions = sum(data["cities"].values())
        total_mentions = data["country_mentions"] + region_mentions + city_mentions

        if total_mentions == 0:
            continue

        # Calculate confidence score
        # - Mention score: normalized by assumed max of 20 mentions (60% weight)
        # - Coverage score: proportion of articles mentioning this country (20% weight)
        # - Headline score: proportion of articles with headline mention (20% weight)
        mention_score = min(1.0, total_mentions / 20) * 0.6
        coverage_score = (data["articles_with_mentions"] / total_articles) * 0.2 if total_articles > 0 else 0
        headline_score = (data["headline_mentions"] / total_articles) * 0.2 if total_articles > 0 else 0

        confidence = mention_score + coverage_score + headline_score

        # Apply minimum confidence filter
        if confidence < min_confidence:
            continue

        # Build sub-location lists
        regions = [
            SubLocation(name=name, type="region", mention_count=count)
            for name, count in sorted(data["regions"].items(), key=lambda x: -x[1])[:5]
        ]
        cities = [
            SubLocation(name=name, type="city", mention_count=count)
            for name, count in sorted(data["cities"].items(), key=lambda x: -x[1])[:5]
        ]

        results.append(HierarchicalLocation(
            name=country_name,
            country_code=country_code,
            confidence=confidence,
            mention_count=total_mentions,
            regions=regions,
            cities=cities,
        ))

    # Sort by confidence and return top N
    results.sort(key=lambda x: x.confidence, reverse=True)
    return results[:max_locations]


# Keep old function for backward compatibility
def aggregate_locations(articles: list[dict[str, Any]], indices: list[int]) -> list[Location]:
    """Aggregate locations across articles in a cluster (legacy flat version)."""
    # Key: (name, country_code), Value: list of confidence scores
    location_scores: dict[tuple[str, str | None], list[float]] = {}

    for idx in indices:
        article = articles[idx]
        locations = article.get("locations", [])
        for loc in locations:
            if isinstance(loc, dict):
                name = loc.get("name", "")
                confidence = loc.get("confidence", 0.5)
                country_code = loc.get("country_code")
                if name:
                    key = (name, country_code)
                    if key not in location_scores:
                        location_scores[key] = []
                    location_scores[key].append(confidence)

    # Average confidence per location
    aggregated = []
    for (name, country_code), scores in location_scores.items():
        avg_confidence = sum(scores) / len(scores)
        aggregated.append(Location(name=name, confidence=avg_confidence, country_code=country_code))

    # Sort by confidence
    aggregated.sort(key=lambda x: x.confidence, reverse=True)
    return aggregated[:5]


def get_sources(articles: list[dict[str, Any]], indices: list[int]) -> list[str]:
    """Get unique sources for articles in a cluster."""
    sources = set()
    for idx in indices:
        article = articles[idx]
        source = article.get("source")
        if source:
            sources.add(source)
    return sorted(sources)


def get_time_range(
    articles: list[dict[str, Any]], indices: list[int]
) -> tuple[datetime, datetime]:
    """Get the publication time range for articles in a cluster."""
    timestamps = []
    for idx in indices:
        article = articles[idx]
        pub_at = article.get("published_at")
        if pub_at:
            if isinstance(pub_at, str):
                # Parse ISO format
                pub_at = datetime.fromisoformat(pub_at.replace("Z", "+00:00"))
            timestamps.append(pub_at)

    if not timestamps:
        now = datetime.now()
        return now, now

    return min(timestamps), max(timestamps)


def build_stories(
    articles: list[dict[str, Any]],
    cluster_labels: np.ndarray,
    embeddings: np.ndarray,
) -> tuple[list[Story], list[ArticleStoryMap], list[StoryArticles]]:
    """Build Story and ArticleStoryMap objects from clustering results.

    Args:
        articles: List of article dicts
        cluster_labels: Array of cluster assignments
        embeddings: L2-normalized embedding matrix

    Returns:
        Tuple of (stories, article_story_maps, story_articles)
    """
    # Group articles by cluster
    cluster_to_indices: dict[int, list[int]] = {}
    for idx, label in enumerate(cluster_labels):
        label_int = int(label)
        if label_int not in cluster_to_indices:
            cluster_to_indices[label_int] = []
        cluster_to_indices[label_int].append(idx)

    stories = []
    article_maps = []
    story_articles_list = []

    for cluster_label, indices in cluster_to_indices.items():
        # Get article IDs for this cluster
        article_ids = [articles[idx].get("article_id", f"article_{idx}") for idx in indices]

        if cluster_label == -1:
            # Noise articles - no story
            for idx in indices:
                article_id = articles[idx].get("article_id", f"article_{idx}")
                article_maps.append(
                    ArticleStoryMap(
                        article_id=article_id,
                        story_id=None,
                        cluster_label=-1,
                    )
                )
        else:
            # Create story for this cluster
            story_id = generate_story_id(cluster_label, article_ids)

            # Select title (headline closest to centroid)
            title = select_story_title(articles, embeddings, indices)

            # Aggregate metadata
            top_entities = aggregate_entities(articles, indices)
            locations = aggregate_locations_hierarchical(articles, indices)
            sources = get_sources(articles, indices)
            start_pub, end_pub = get_time_range(articles, indices)

            # Compute story embedding (centroid)
            cluster_embeddings = embeddings[indices]
            story_embedding = np.mean(cluster_embeddings, axis=0).tolist()

            story = Story(
                story_id=story_id,
                title=title,
                article_count=len(indices),
                sources=sources,
                top_entities=top_entities,
                locations=locations,
                story_embedding=story_embedding,
                start_published_at=start_pub,
                end_published_at=end_pub,
            )
            stories.append(story)

            # Build article summaries for this story
            article_summaries = []
            for idx in indices:
                article = articles[idx]
                article_id = article.get("article_id", f"article_{idx}")
                headline = article.get("headline") or article.get("title") or ""
                source = article.get("source") or ""
                article_summaries.append(ArticleSummary(
                    article_id=article_id,
                    headline=headline,
                    source=source,
                ))
                article_maps.append(
                    ArticleStoryMap(
                        article_id=article_id,
                        story_id=story_id,
                        cluster_label=cluster_label,
                    )
                )

            # Create story articles view
            story_articles_list.append(StoryArticles(
                story_id=story_id,
                title=title,
                locations=locations,
                articles=article_summaries,
            ))

    return stories, article_maps, story_articles_list
