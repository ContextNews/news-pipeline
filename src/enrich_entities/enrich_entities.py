"""Enrich the knowledge base with Wikidata lookups for unresolved entities."""

from __future__ import annotations

import logging

from enrich_entities.models import EnrichedEntity, WikidataCandidate
from enrich_entities.wikidata import (
    classify_as_location,
    classify_as_person,
    fetch_wikidata_entity_data,
    get_english_aliases,
    search_entity,
)

logger = logging.getLogger(__name__)


def enrich_entities(
    unresolved_gpe: dict[str, list[str]],
    unresolved_persons: dict[str, list[str]],
    delay: float = 0.5,
) -> list[EnrichedEntity]:
    """
    Look up unresolved entity names on Wikidata and return enriched entities.

    Args:
        unresolved_gpe: {entity_name: [article_id, ...]} for GPE entities absent from KB
        unresolved_persons: {entity_name: [article_id, ...]} for PERSON entities absent from KB
        delay: Seconds to wait between Wikidata API calls

    Returns:
        List of EnrichedEntity objects ready to be written to the KB.
    """
    results: list[EnrichedEntity] = []

    for name, article_ids in unresolved_gpe.items():
        entity = _try_enrich(name, article_ids, entity_type="location", delay=delay)
        if entity:
            results.append(entity)

    for name, article_ids in unresolved_persons.items():
        entity = _try_enrich(name, article_ids, entity_type="person", delay=delay)
        if entity:
            results.append(entity)

    logger.info(
        "Enriched %d entities from Wikidata (%d GPE, %d PERSON)",
        len(results),
        sum(1 for e in results if e.entity_type == "location"),
        sum(1 for e in results if e.entity_type == "person"),
    )
    return results


def _try_enrich(
    name: str,
    article_ids: list[str],
    entity_type: str,
    delay: float,
) -> EnrichedEntity | None:
    candidates = search_entity(name, delay)
    chosen = _disambiguate(name, candidates)
    if not chosen:
        return None

    entity_data = fetch_wikidata_entity_data(chosen.qid, delay)
    if not entity_data:
        logger.debug("Could not fetch Wikidata data for QID %s (%s)", chosen.qid, name)
        return None

    if entity_type == "location":
        location = classify_as_location(chosen.qid, entity_data, delay)
        if not location:
            logger.debug("QID %s is not a location, skipping '%s'", chosen.qid, name)
            return None
        person = None
        canonical_name = location.name
        description = location.description
    else:
        person = classify_as_person(chosen.qid, entity_data, delay)
        if not person:
            logger.debug("QID %s is not a person, skipping '%s'", chosen.qid, name)
            return None
        location = None
        canonical_name = person.name
        description = person.description

    aliases = get_english_aliases(entity_data)
    # Ensure the original entity name is included as an alias
    if name not in aliases and name.title() not in aliases:
        aliases.append(name)
    aliases = sorted(set(aliases))

    return EnrichedEntity(
        entity_name=name,
        entity_type=entity_type,
        qid=chosen.qid,
        name=canonical_name,
        description=description,
        location=location,
        person=person,
        aliases=aliases,
        article_ids=list(dict.fromkeys(article_ids)),
    )


def _disambiguate(
    name: str,
    candidates: list[WikidataCandidate],
) -> WikidataCandidate | None:
    """
    Select a single candidate from search results.

    Accepts automatically if there is exactly one result, or if exactly one
    result has a label that exactly matches the entity name (case-insensitive).
    Skips ambiguous cases to avoid polluting the KB with incorrect entries.
    """
    if not candidates:
        logger.debug("No Wikidata candidates found for: %s", name)
        return None

    if len(candidates) == 1:
        return candidates[0]

    exact = [c for c in candidates if c.label.upper() == name.upper()]
    if len(exact) == 1:
        return exact[0]

    logger.info(
        "Ambiguous Wikidata results for '%s' (%d candidates), skipping",
        name,
        len(candidates),
    )
    return None
