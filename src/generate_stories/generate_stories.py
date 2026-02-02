from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from cronkite import Cronkite, CronkiteConfig

from generate_stories.resolve_story_location import resolve_story_location


@dataclass
class GeneratedStoryOverview:
    title: str
    summary: str
    key_points: list[str]
    article_ids: list[str]
    noise_article_ids: list[str]
    quotes: list[dict[str, Any]]
    sub_stories: list[dict[str, Any]]
    location: dict[str, Any] | None
    location_qid: str | None = None


def _normalize_articles_for_cronkite(
    cluster: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    normalized = []
    for article in cluster:
        published_at = article.get("published_at")
        if isinstance(published_at, (datetime, date)):
            published_at = published_at.isoformat()
        normalized.append({
            **article,
            "published_at": published_at,
        })
    return normalized


def generate_story_overview(
    cluster: list[dict[str, Any]],
    model: str = "gpt-4o-mini",
) -> GeneratedStoryOverview:
    """Generate a story overview from a cluster of articles using Cronkite."""
    config = CronkiteConfig(
        group_articles=False,
        extract_quotes=False,
        generate_substories=False,
        resolve_location=False,
    )
    cronkite = Cronkite(model=model, config=config)
    normalized_cluster = _normalize_articles_for_cronkite(cluster)
    data = cronkite.generate_story(normalized_cluster)
    summary = data.get("summary", "")

    return GeneratedStoryOverview(
        title=data.get("title", ""),
        summary=summary,
        key_points=list(data.get("key_points") or []),
        article_ids=list(data.get("article_ids") or []),
        noise_article_ids=list(data.get("noise_article_ids") or []),
        quotes=list(data.get("quotes") or []),
        sub_stories=list(data.get("sub_stories") or []),
        location=data.get("location"),
    )


def generate_stories(
    article_clusters: list[list[dict[str, Any]]],
    model: str = "gpt-4o-mini",
) -> list[GeneratedStoryOverview]:
    """Generate story overviews for multiple article clusters."""
    return [generate_story(cluster, model=model) for cluster in article_clusters]


def generate_story(
    cluster: list[dict[str, Any]],
    model: str = "gpt-4o-mini",
    article_locations: dict[str, list[str]] | None = None,
) -> GeneratedStoryOverview:
    """Generate a story overview from a single cluster of related articles."""
    story_overview = generate_story_overview(
        cluster,
        model=model,
    )

    # Resolve location from article locations
    location_qid = None
    if article_locations:
        article_ids = story_overview.article_ids or [a["id"] for a in cluster]
        location_qid = resolve_story_location(article_ids, article_locations)

    story_overview.location_qid = location_qid
    return story_overview

def filter_artice_cluster(
    cluster: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    filtered_cluster = []
    return filtered_cluster
