"""Resolve story location from article locations."""

from __future__ import annotations

import logging
from collections import Counter

logger = logging.getLogger(__name__)


def resolve_story_location(
    article_ids: list[str],
    article_locations: dict[str, list[str]],
) -> str | None:
    """
    Determine the most common location among a story's articles.

    Args:
        article_ids: List of article IDs belonging to the story
        article_locations: Mapping of article_id -> list of wikidata_qids

    Returns:
        wikidata_qid of the most common location, or None if no locations found.
        In case of ties, returns alphabetically first wikidata_qid.
    """
    if not article_ids:
        return None

    # Count location occurrences across the story's articles
    location_counts: Counter[str] = Counter()
    for article_id in article_ids:
        locations = article_locations.get(article_id, [])
        for qid in locations:
            location_counts[qid] += 1

    if not location_counts:
        logger.debug("No locations found for %d articles", len(article_ids))
        return None

    # Find max count, then pick alphabetically first among ties
    max_count = max(location_counts.values())
    top_locations = [qid for qid, count in location_counts.items() if count == max_count]
    top_locations.sort()

    result = top_locations[0]
    logger.debug(
        "Resolved story location to %s (in %d articles)",
        result,
        max_count,
    )
    return result


def resolve_story_persons(
    article_ids: list[str],
    article_persons: dict[str, list[str]],
) -> list[str]:
    """
    Collect all distinct person QIDs from a story's articles.

    Args:
        article_ids: List of article IDs belonging to the story
        article_persons: Mapping of article_id -> list of wikidata_qids

    Returns:
        Sorted list of unique wikidata_qids, or empty list if none found.
    """
    if not article_ids:
        return []

    qids: set[str] = set()
    for article_id in article_ids:
        persons = article_persons.get(article_id, [])
        qids.update(persons)

    if not qids:
        logger.debug("No persons found for %d articles", len(article_ids))
        return []

    result = sorted(qids)
    logger.debug("Resolved %d persons for story", len(result))
    return result
