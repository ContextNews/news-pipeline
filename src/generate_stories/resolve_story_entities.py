"""Resolve story location from article locations."""

from __future__ import annotations

import logging
from collections import Counter

logger = logging.getLogger(__name__)


# Specificity ranks for kb_locations.location_type. Lower = more specific = preferred
# when picking a story's anchor location. States are filtered upstream
# (article_locations contains only entity_type='location' QIDs) and surface
# instead via §3d auto-attach into story_entities.
LOCATION_TYPE_SPECIFICITY: dict[str, int] = {
    "city": 0,
    "town": 0,
    "state": 1,           # sub-national state/province
    "state_province": 1,
    "region": 1,
    "country": 2,
    "continent": 3,
}


def resolve_story_location(
    article_ids: list[str],
    article_locations: dict[str, list[str]],
    location_types: dict[str, str] | None = None,
) -> str | None:
    """
    Pick the story's anchor location: most specific wins, ties broken by mention count.

    Ranking key per candidate QID: (specificity, -mention_count, qid).
    Specificity ordering: city/town < region/state < country < continent. Unknown
    types fall to the end so a typed city always beats an untyped neighbour.

    Args:
        article_ids: Articles belonging to the story.
        article_locations: article_id -> list of location QIDs (entity_type='location' only).
        location_types: qid -> kb_locations.location_type. Optional; when omitted,
            all candidates are treated as equally specific and selection falls back
            to mention count + alphabetical order (preserving prior behaviour).

    Returns:
        The chosen QID, or None if the story has no locations.
    """
    if not article_ids:
        return None

    location_counts: Counter[str] = Counter()
    for article_id in article_ids:
        for qid in article_locations.get(article_id, []):
            location_counts[qid] += 1

    if not location_counts:
        logger.debug("No locations found for %d articles", len(article_ids))
        return None

    types = location_types or {}

    def sort_key(qid: str) -> tuple[int, int, str]:
        specificity = LOCATION_TYPE_SPECIFICITY.get(types.get(qid, ""), 99)
        return (specificity, -location_counts[qid], qid)

    result = min(location_counts.keys(), key=sort_key)
    logger.debug(
        "Resolved story location to %s (type=%s, mentions=%d)",
        result,
        types.get(result, "unknown"),
        location_counts[result],
    )
    return result


def resolve_story_states(
    article_ids: list[str],
    article_states: dict[str, list[str]],
) -> list[str]:
    """Collect distinct state QIDs mentioned by name across the story's articles."""
    if not article_ids:
        return []

    qids: set[str] = set()
    for article_id in article_ids:
        qids.update(article_states.get(article_id, []))

    return sorted(qids)


def auto_attach_states(
    location_qids: list[str],
    location_country_codes: dict[str, str | None],
    country_to_state: dict[str, str],
) -> list[str]:
    """
    Determine which state QIDs should be auto-attached based on a story's locations.

    For each location, look up its country_code and map to a state QID. Locations
    with no country_code (continents, oceans, supranational regions) are skipped
    safely. Missing country_to_state entries are skipped, not errored.

    Returns the deduped list of state QIDs implied by the story's locations.
    """
    state_qids: set[str] = set()
    for qid in location_qids:
        country_code = location_country_codes.get(qid)
        if not country_code:
            continue
        state_qid = country_to_state.get(country_code)
        if state_qid:
            state_qids.add(state_qid)
    return sorted(state_qids)


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


def resolve_story_organizations(
    article_ids: list[str],
    article_organizations: dict[str, list[str]],
) -> list[str]:
    """
    Collect all distinct organisation QIDs from a story's articles.

    Args:
        article_ids: List of article IDs belonging to the story
        article_organizations: Mapping of article_id -> list of wikidata_qids

    Returns:
        Sorted list of unique wikidata_qids, or empty list if none found.
    """
    if not article_ids:
        return []

    qids: set[str] = set()
    for article_id in article_ids:
        orgs = article_organizations.get(article_id, [])
        qids.update(orgs)

    if not qids:
        logger.debug("No organisations found for %d articles", len(article_ids))
        return []

    result = sorted(qids)
    logger.debug("Resolved %d organisations for story", len(result))
    return result
