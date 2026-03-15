"""Link stories across days using similarity search and LLM grouping."""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

from cronkite import Cronkite

from link_stories.get_similar_stories import get_similar_stories

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
        logger.info("No today_stories provided, skipping linking")
        return []

    logger.info(
        "Linking %d stories from today against previous date %s",
        len(today_stories), previous_date,
    )

    candidate_ids: set[str] = set()
    for story in today_stories:
        candidates = get_similar_stories(
            story["story_id"], previous_date, n=n_candidates
        )
        for candidate in candidates:
            candidate_ids.add(candidate["story_id"])

    if not candidate_ids:
        logger.info("No candidate stories found for %s", previous_date)
        return []

    # Load full data for candidate stories on the older date.
    previous_stories = _load_stories_for_llm(list(candidate_ids))
    if not previous_stories:
        logger.info("No previous stories loaded from DB for candidate IDs")
        return []

    cronkite = Cronkite(model=model)
    links = cronkite.group_stories(
        group_a=previous_stories,
        group_b=today_stories,
    )

    result: list[tuple[str, str]] = []
    for link in links:
        a_idx = link["group_a_index"]
        b_idx = link["group_b_index"]
        if a_idx < len(previous_stories) and b_idx < len(today_stories):
            story_id_1 = previous_stories[a_idx]["story_id"]
            story_id_2 = today_stories[b_idx]["story_id"]
            result.append((story_id_1, story_id_2))
        else:
            logger.warning(
                "LLM returned out-of-bounds indices: group_a_index=%d, group_b_index=%d",
                a_idx,
                b_idx,
            )

    logger.info("Linked %d story pairs", len(result))
    return result


def load_stories_for_date(target_date: date) -> list[dict[str, Any]]:
    """Load stories for a date as dicts suitable for linking."""
    from sqlalchemy import Date, cast
    from context_db.connection import get_session
    from context_db.models import Story

    with get_session() as session:
        rows = (
            session.query(Story)
            .filter(cast(Story.story_period, Date) == target_date)
            .all()
        )

        return [
            {
                "story_id": row.id,
                "title": row.title,
                "summary": row.summary,
                "key_points": row.key_points,
            }
            for row in rows
        ]


def _load_stories_for_llm(story_ids: list[str]) -> list[dict[str, Any]]:
    """Load title, summary, key_points from stories table for the given IDs."""
    from context_db.connection import get_session
    from context_db.models import Story

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

    # Preserve stable order matching the input candidate IDs.
    return [stories_by_id[story_id] for story_id in story_ids if story_id in stories_by_id]


def save_story_links(
    links: list[tuple[str, str]], session: Any
) -> None:
    """Insert directed story relationship rows into story_edges table.

    story_id_1 is the older story (from_story_id), story_id_2 is the newer story (to_story_id).
    """
    if not links:
        return

    from datetime import timezone
    from sqlalchemy import text

    now = datetime.now(timezone.utc)
    rows = [
        {"from_story_id": story_id_1, "to_story_id": story_id_2, "created_at": now}
        for story_id_1, story_id_2 in links
    ]

    session.execute(
        text(
            """
            INSERT INTO story_edges (from_story_id, to_story_id, relation_type, score, created_at)
            VALUES (:from_story_id, :to_story_id, 'followup', NULL, :created_at)
            ON CONFLICT DO NOTHING
            """
        ),
        rows,
    )
    logger.info("Saved %d story edges to RDS", len(rows))


def delete_story_links(date_a: date, date_b: date, session: Any) -> int:
    """Delete existing edges between stories from two specific dates."""
    from sqlalchemy import text

    result = session.execute(
        text(
            """
            DELETE FROM story_edges se
            USING stories s1, stories s2
            WHERE s1.id = se.from_story_id
              AND s2.id = se.to_story_id
              AND (
                (CAST(s1.story_period AS DATE) = :date_a AND CAST(s2.story_period AS DATE) = :date_b)
                OR
                (CAST(s1.story_period AS DATE) = :date_b AND CAST(s2.story_period AS DATE) = :date_a)
              )
            """
        ),
        {"date_a": date_a, "date_b": date_b},
    )
    deleted = result.rowcount or 0
    logger.info("Deleted %d existing story edges between %s and %s", deleted, date_a, date_b)
    return deleted
