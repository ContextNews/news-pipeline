"""Resolve GPE and PERSON entities using disambiguation heuristics."""

from __future__ import annotations

import logging
from collections import defaultdict

from resolve_entities.models import (
    ArticleLocation,
    ArticlePerson,
    LocationCandidate,
    PersonCandidate,
)

logger = logging.getLogger(__name__)


def resolve_entities(
    article_gpe_entities: dict[str, list[str]],
    article_person_entities: dict[str, list[str]],
    alias_to_locations: dict[str, list[LocationCandidate]],
    alias_to_persons: dict[str, list[PersonCandidate]],
) -> tuple[list[ArticleLocation], list[ArticlePerson]]:
    """
    Resolve GPE and PERSON entities to their reference entries.

    Resolves GPE entities first, then uses the resolved country codes
    as context for person disambiguation.

    Args:
        article_gpe_entities: {article_id: [GPE_NAME, ...]} (uppercase)
        article_person_entities: {article_id: [PERSON_NAME, ...]} (uppercase)
        alias_to_locations: {ALIAS: [LocationCandidate, ...]}
        alias_to_persons: {ALIAS: [PersonCandidate, ...]}

    Returns:
        Tuple of (resolved locations, resolved persons)
    """
    locations = _resolve_locations(article_gpe_entities, alias_to_locations)

    # Build per-article country codes from resolved locations
    article_country_codes = _build_article_country_codes(locations, alias_to_locations)

    persons = _resolve_persons(
        article_person_entities, alias_to_persons, article_country_codes
    )

    logger.info(
        "Resolved %d locations and %d persons",
        len(locations),
        len(persons),
    )
    return locations, persons


# ---------------------------------------------------------------------------
# Location resolution (ported from resolve_article_locations)
# ---------------------------------------------------------------------------


def _resolve_locations(
    article_gpe_entities: dict[str, list[str]],
    alias_to_locations: dict[str, list[LocationCandidate]],
) -> list[ArticleLocation]:
    """Resolve GPE entities to locations using disambiguation heuristics."""
    results: list[ArticleLocation] = []

    for article_id, entity_names in article_gpe_entities.items():
        entity_candidates: dict[str, list[LocationCandidate]] = {}
        for name in entity_names:
            if name in alias_to_locations:
                entity_candidates[name] = alias_to_locations[name]

        context = _build_location_context(entity_candidates)

        for entity_name, candidates in entity_candidates.items():
            resolved = _disambiguate_location(candidates, context)
            for location in resolved:
                results.append(
                    ArticleLocation(
                        article_id=article_id,
                        wikidata_qid=location.wikidata_qid,
                        name=entity_name,
                    )
                )

    return results


def _build_location_context(
    entity_candidates: dict[str, list[LocationCandidate]],
) -> set[str]:
    """Extract unambiguous country codes and location names for context."""
    context: set[str] = set()
    for candidates in entity_candidates.values():
        if len(candidates) == 1:
            loc = candidates[0]
            if loc.country_code:
                context.add(loc.country_code)
            if loc.location_type == "country":
                context.add(loc.name.upper())
    return context


def _disambiguate_location(
    candidates: list[LocationCandidate],
    context: set[str],
) -> list[LocationCandidate]:
    """Apply heuristics to select the best location candidate(s)."""
    if len(candidates) == 1:
        return candidates

    # Heuristic 1: Filter by context (country codes or country names in article)
    contextual = [
        c for c in candidates if c.country_code in context or c.name.upper() in context
    ]
    if len(contextual) == 1:
        return contextual
    if contextual:
        candidates = contextual

    # Heuristic 2: Prefer countries over other types
    countries = [c for c in candidates if c.location_type == "country"]
    if len(countries) == 1:
        return countries

    # Heuristic 3: Type hierarchy - country > state > city
    type_priority = {"country": 0, "state": 1, "city": 2}
    candidates = sorted(candidates, key=lambda c: type_priority.get(c.location_type, 99))

    best_type = candidates[0].location_type
    return [c for c in candidates if c.location_type == best_type]


# ---------------------------------------------------------------------------
# Person resolution
# ---------------------------------------------------------------------------


def _build_article_country_codes(
    locations: list[ArticleLocation],
    alias_to_locations: dict[str, list[LocationCandidate]],
) -> dict[str, set[str]]:
    """
    Build per-article country code sets from resolved locations.

    Uses the alias lookup to find the country_code for each resolved location.
    """
    article_country_codes: dict[str, set[str]] = defaultdict(set)

    for loc in locations:
        candidates = alias_to_locations.get(loc.name, [])
        for candidate in candidates:
            if candidate.wikidata_qid == loc.wikidata_qid and candidate.country_code:
                article_country_codes[loc.article_id].add(candidate.country_code)
                break

    return dict(article_country_codes)


def _resolve_persons(
    article_person_entities: dict[str, list[str]],
    alias_to_persons: dict[str, list[PersonCandidate]],
    article_country_codes: dict[str, set[str]],
) -> list[ArticlePerson]:
    """Resolve PERSON entities to persons using disambiguation heuristics."""
    results: list[ArticlePerson] = []

    for article_id, entity_names in article_person_entities.items():
        country_context = article_country_codes.get(article_id, set())

        for entity_name in entity_names:
            candidates = alias_to_persons.get(entity_name, [])
            if not candidates:
                continue

            resolved = _disambiguate_person(candidates, country_context)
            for person in resolved:
                results.append(
                    ArticlePerson(
                        article_id=article_id,
                        wikidata_qid=person.wikidata_qid,
                        name=entity_name,
                    )
                )

    return results


def _disambiguate_person(
    candidates: list[PersonCandidate],
    country_context: set[str],
) -> list[PersonCandidate]:
    """Apply heuristics to select the best person candidate(s)."""
    if len(candidates) == 1:
        return candidates

    # Heuristic 1: Filter by nationality matching resolved country codes
    if country_context:
        nationality_matches = [
            c
            for c in candidates
            if c.nationalities
            and any(n in country_context for n in c.nationalities)
        ]
        if len(nationality_matches) == 1:
            return nationality_matches
        if nationality_matches:
            candidates = nationality_matches

    return candidates
