"""Link stories across days using similarity search and LLM grouping."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from cronkite import Cronkite

from generate_stories.get_similar_stories import get_similar_stories

logger = logging.getLogger(__name__)


def link_stories(
    today_stories: list[dict[str, Any]],
    previous_date: date,
    model: str = "gpt-4o-mini",
    n_candidates: int = 3,
) -> list[tuple[str, str]]:
    """Link today's stories to related stories from a previous date.

    For each story in today_stories, finds candidate matches from previous_date
    using embedding/topic/entity similarity, then uses Cronkite's LLM to confirm
    which stories cover the same event.

    Args:
        today_stories: List of story dicts with story_id, title, summary, key_points.
        previous_date: Date to search for candidate matches.
        model: OpenAI model to use for LLM grouping.
        n_candidates: Number of candidate matches to retrieve per story.

    Returns:
        List of (story_id_1, story_id_2) tuples where story_id_1 is from
        previous_date and story_id_2 is from today.
    """
    if not today_stories:
        return []

    # Collect unique candidate story IDs from previous date
    candidate_ids: set[str] = set()
    for story in today_stories:
        candidates = get_similar_stories(
            story["story_id"], previous_date, n=n_candidates
        )
        for c in candidates:
            candidate_ids.add(c["story_id"])

    if not candidate_ids:
        logger.info("No candidate stories found for %s", previous_date)
        return []

    # Load full data for yesterday's candidates
    yesterday_stories = _load_stories_for_llm(list(candidate_ids))

    if not yesterday_stories:
        return []

    # Use Cronkite LLM to confirm matches
    cronkite = Cronkite(model=model)
    links = cronkite.group_stories(
        group_a=yesterday_stories, group_b=today_stories
    )

    # Map index pairs back to story ID pairs
    result = []
    for link in links:
        a_idx = link["group_a_index"]
        b_idx = link["group_b_index"]
        if a_idx < len(yesterday_stories) and b_idx < len(today_stories):
            story_id_1 = yesterday_stories[a_idx]["story_id"]
            story_id_2 = today_stories[b_idx]["story_id"]
            result.append((story_id_1, story_id_2))

    logger.info(
        "Linked %d story pairs between %s and today", len(result), previous_date
    )
    return result


def _load_stories_for_llm(story_ids: list[str]) -> list[dict[str, Any]]:
    """Load title, summary, key_points from stories table for the given IDs.

    Returns list of dicts in stable order matching story_ids.
    """
    from rds_postgres.connection import get_session
    from rds_postgres.models import Story

    with get_session() as session:
        stories_by_id = {}
        rows = session.query(Story).filter(Story.id.in_(story_ids)).all()
        for row in rows:
            stories_by_id[row.id] = {
                "story_id": row.id,
                "title": row.title,
                "summary": row.summary,
                "key_points": row.key_points,
            }

    # Return in stable order matching input story_ids
    return [stories_by_id[sid] for sid in story_ids if sid in stories_by_id]


def save_story_links(
    links: list[tuple[str, str]], session: Any
) -> None:
    """Insert story-story relationship rows into story_stories table.

    Args:
        links: List of (story_id_1, story_id_2) tuples where story_id_1 is
            the older story and story_id_2 is the newer story.
        session: SQLAlchemy session.
    """
    if not links:
        return

    from sqlalchemy import text

    rows = [
        {"story_id_1": sid1, "story_id_2": sid2}
        for sid1, sid2 in links
    ]

    session.execute(
        text(
            """
            INSERT INTO story_stories (story_id_1, story_id_2)
            VALUES (:story_id_1, :story_id_2)
            ON CONFLICT DO NOTHING
            """
        ),
        rows,
    )
    logger.info("Saved %d story links to RDS", len(rows))
