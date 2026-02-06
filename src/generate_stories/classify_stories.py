"""Core logic for classifying stories by topic using Cronkite."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from cronkite import Cronkite


@dataclass
class ClassifiedStory:
    """A story with its assigned topics."""

    story_id: str
    topics: list[str]


def classify_stories(
    stories: list[dict[str, Any]],
    model: str = "gpt-4o-mini",
) -> list[ClassifiedStory]:
    """
    Classify stories by topic using Cronkite.

    Args:
        stories: List of story dicts with 'id', 'title', 'summary'.
        model: OpenAI model to use for classification.

    Returns:
        List of ClassifiedStory with story_id and assigned topics.
    """
    if not stories:
        return []

    cronkite = Cronkite(model=model)

    cronkite_stories = [
        {
            "id": story["id"],
            "title": story["title"],
            "summary": story["summary"],
        }
        for story in stories
    ]

    classified = cronkite.classify_stories(cronkite_stories)

    results = []
    for story_data in classified:
        story_id = story_data.get("id")
        topics = story_data.get("topics", [])
        if story_id and topics:
            results.append(ClassifiedStory(story_id=story_id, topics=topics))

    return results
