from dataclasses import dataclass
import json
import os
from typing import Any

from openai import OpenAI

from generate_stories.instructions import GENERATE_OVERVIEW_INSTRUCTIONS


@dataclass
class GeneratedStoryOverview:
    title: str
    summary: str
    key_points: list[str]


def _format_cluster_for_prompt(cluster: list[dict[str, Any]]) -> str:
    """Format a cluster of articles into a text block for the LLM prompt."""
    lines = []
    for i, article in enumerate(cluster, 1):
        title = article.get("title", "")
        source = article.get("source", "")
        summary = article.get("summary", "")
        lines.append(f"Article {i}:")
        lines.append(f"  Source: {source}")
        lines.append(f"  Title: {title}")
        if summary:
            lines.append(f"  Summary: {summary}")
        lines.append("")
    return "\n".join(lines)


def generate_story_overview(
    cluster: list[dict[str, Any]],
    instructions: str,
    model: str = "gpt-4o-mini",
) -> GeneratedStoryOverview:
    """Generate a story overview from a cluster of articles using an LLM."""
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

    articles_text = _format_cluster_for_prompt(cluster)

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": instructions},
            {"role": "user", "content": articles_text},
        ],
        response_format={"type": "json_object"},
    )

    content = response.choices[0].message.content
    data = json.loads(content)

    return GeneratedStoryOverview(
        title=data["title"],
        summary=data["summary"],
        key_points=data["key_points"],
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
) -> GeneratedStoryOverview:
    """Generate a story overview from a single cluster of related articles."""
    story_overview = generate_story_overview(
        cluster,
        instructions=GENERATE_OVERVIEW_INSTRUCTIONS,
        model=model,
    )
    return story_overview
