from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

from cronkite import Cronkite, CronkiteConfig

from generate_stories.classify_stories import classify_stories
from generate_stories.resolve_story_entities import resolve_story_location, resolve_story_persons
from generate_stories.topic_indicators import get_indicators_for_topics

logger = logging.getLogger(__name__)


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
    person_qids: list[str] | None = None


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
    article_persons: dict[str, list[str]] | None = None,
) -> GeneratedStoryOverview:
    """Generate a story overview from a single cluster of related articles."""
    story_overview = generate_story_overview(
        cluster,
        model=model,
    )

    article_ids = story_overview.article_ids or [a["id"] for a in cluster]

    location_qid = None
    if article_locations:
        location_qid = resolve_story_location(article_ids, article_locations)

    person_qids = []
    if article_persons:
        person_qids = resolve_story_persons(article_ids, article_persons)

    story_overview.location_qid = location_qid
    story_overview.person_qids = person_qids
    return story_overview


def build_story_record(
    cluster_id: str,
    article_ids: list[str],
    story: GeneratedStoryOverview,
    story_period: Any,
    generated_at: datetime,
) -> dict[str, Any]:
    """Build a persistable record dict from a GeneratedStoryOverview."""
    return {
        "story_id": uuid4().hex,
        "cluster_id": cluster_id,
        "article_ids": article_ids,
        "title": story.title,
        "summary": story.summary,
        "key_points": story.key_points,
        "quotes": story.quotes,
        "sub_stories": story.sub_stories,
        "location": story.location,
        "location_qid": story.location_qid,
        "person_qids": story.person_qids or [],
        "noise_article_ids": story.noise_article_ids,
        "story_period": story_period.isoformat() if hasattr(story_period, "isoformat") else story_period,
        "generated_at": generated_at.isoformat(),
    }


def process_clusters(
    clusters: list[dict[str, Any]],
    article_locations: dict[str, list[str]],
    article_persons: dict[str, list[str]],
    article_topics: dict[str, list[str]],
    model: str = "gpt-4o-mini",
    generated_at: datetime | None = None,
) -> list[dict[str, Any]]:
    """Generate story records for all clusters, classify by topic, and attach indicators.

    Args:
        clusters: List of cluster dicts with 'cluster_id', 'articles', 'cluster_period'.
        article_locations: Mapping of article_id to list of location QIDs.
        article_persons: Mapping of article_id to list of person QIDs.
        article_topics: Mapping of article_id to list of topic labels.
            Pass empty dict to skip classification.
        model: OpenAI model to use for story generation.
        generated_at: Timestamp to record on each story. Defaults to now (UTC).

    Returns:
        List of story record dicts ready for persistence.
    """
    if generated_at is None:
        generated_at = datetime.now(timezone.utc)

    story_records = []
    for i, cluster in enumerate(clusters, 1):
        cluster_id = cluster["cluster_id"]
        articles = cluster["articles"]

        logger.info(
            "--- Cluster %d/%d [%s] — %d articles ---",
            i, len(clusters), cluster_id, len(articles),
        )

        try:
            story = generate_story(
                articles,
                model=model,
                article_locations=article_locations,
                article_persons=article_persons,
            )
            article_ids = story.article_ids or [a["id"] for a in articles]
            record = build_story_record(
                cluster_id, article_ids, story, cluster["cluster_period"], generated_at
            )
            story_records.append(record)

            logger.info("  Story: %s", story.title)
            logger.info(
                "  %d kept, %d noise | location=%s | persons=%s",
                len(article_ids), len(story.noise_article_ids),
                story.location_qid, story.person_qids,
            )
        except Exception as e:
            logger.error("Failed to generate story for cluster %s: %s", cluster_id, e)
            continue

    if not story_records:
        return story_records

    # Classify by topic from article topics
    if article_topics:
        classified = classify_stories(story_records, article_topics)
        topics_by_id = {cs.story_id: cs.topics for cs in classified}
        for record in story_records:
            record["topics"] = topics_by_id.get(record["story_id"], [])
    else:
        for record in story_records:
            record["topics"] = []

    # Attach World Bank time series indicators based on topics
    for record in story_records:
        record["ts_indicators"] = get_indicators_for_topics(record["topics"])

    return story_records
