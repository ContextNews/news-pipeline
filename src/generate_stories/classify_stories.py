"""Core logic for classifying stories by topic from article topics."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

MAX_TOPICS = 2
MIN_ARTICLE_FRACTION = 0.25


@dataclass
class ClassifiedStory:
    """A story with its assigned topics."""

    story_id: str
    topics: list[str]


def classify_stories(
    stories: list[dict[str, Any]],
    article_topics: dict[str, list[str]],
) -> list[ClassifiedStory]:
    """
    Classify stories by aggregating topics from their constituent articles.

    A topic is included if it appears in more than 25% of the story's articles.
    At most 2 topics are returned, ordered by frequency descending.

    Args:
        stories: List of story dicts with 'story_id' and 'article_ids'.
        article_topics: Mapping of article_id to list of topic labels.

    Returns:
        List of ClassifiedStory with story_id and assigned topics.
    """
    if not stories:
        return []

    results = []
    for story in stories:
        story_id = story["story_id"]
        article_ids = story.get("article_ids", [])

        if not article_ids:
            results.append(ClassifiedStory(story_id=story_id, topics=[]))
            continue

        topic_counts: dict[str, int] = {}
        for article_id in article_ids:
            for topic in article_topics.get(article_id, []):
                topic_counts[topic] = topic_counts.get(topic, 0) + 1

        threshold = len(article_ids) * MIN_ARTICLE_FRACTION
        qualified = {topic: count for topic, count in topic_counts.items() if count > threshold}
        top_topics = sorted(qualified, key=lambda t: qualified[t], reverse=True)[:MAX_TOPICS]

        results.append(ClassifiedStory(story_id=story_id, topics=top_topics))

    return results
