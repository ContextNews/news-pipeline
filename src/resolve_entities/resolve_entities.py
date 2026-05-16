"""Resolve GPE and PERSON entities using disambiguation heuristics."""

from __future__ import annotations

import logging
from collections import defaultdict

from resolve_entities.models import (
    ArticleLocation,
    ArticleOrganization,
    ArticlePerson,
    ArticleState,
    LocationCandidate,
    PersonCandidate,
    StateCandidate,
)

logger = logging.getLogger(__name__)


def resolve_entities(
    article_gpe_entities: dict[str, list[str]],
    article_person_entities: dict[str, list[str]],
    alias_to_locations: dict[str, list[LocationCandidate]],
    alias_to_persons: dict[str, list[PersonCandidate]],
    article_org_entities: dict[str, list[str]] | None = None,
    alias_to_organizations: dict[str, list[str]] | None = None,
    alias_to_states: dict[str, list[StateCandidate]] | None = None,
) -> tuple[
    list[ArticleLocation],
    list[ArticleState],
    list[ArticlePerson],
    list[ArticleOrganization],
]:
    """
    Resolve GPE, PERSON, and ORG entities to their reference entries.

    GPE entities are resolved against states first (preferred) and fall back
    to locations. State and location ISO codes feed the country-code context
    used to disambiguate persons.

    Args:
        article_gpe_entities: {article_id: [GPE_NAME, ...]} (uppercase)
        article_person_entities: {article_id: [PERSON_NAME, ...]} (uppercase)
        alias_to_locations: {ALIAS: [LocationCandidate, ...]}
        alias_to_persons: {ALIAS: [PersonCandidate, ...]}
        article_org_entities: {article_id: [ORG_NAME, ...]} (uppercase)
        alias_to_organizations: {ALIAS: [qid, ...]}
        alias_to_states: {ALIAS: [StateCandidate, ...]}

    Returns:
        Tuple of (locations, states, persons, organizations)
    """
    alias_to_states = alias_to_states or {}

    states, locations = _resolve_gpe(
        article_gpe_entities, alias_to_states, alias_to_locations
    )

    article_country_codes = _build_article_country_codes(
        locations, states, alias_to_locations, alias_to_states
    )

    persons = _resolve_persons(
        article_person_entities, alias_to_persons, article_country_codes
    )

    organizations = _resolve_organizations(
        article_org_entities or {}, alias_to_organizations or {}
    )

    logger.info(
        "Resolved %d locations, %d states, %d persons, and %d organizations",
        len(locations),
        len(states),
        len(persons),
        len(organizations),
    )
    return locations, states, persons, organizations


# ---------------------------------------------------------------------------
# GPE resolution (states preferred over locations)
# ---------------------------------------------------------------------------


def _resolve_gpe(
    article_gpe_entities: dict[str, list[str]],
    alias_to_states: dict[str, list[StateCandidate]],
    alias_to_locations: dict[str, list[LocationCandidate]],
) -> tuple[list[ArticleState], list[ArticleLocation]]:
    """Resolve each GPE entity to either a state (preferred) or a location."""
    states: list[ArticleState] = []
    locations: list[ArticleLocation] = []

    for article_id, entity_names in article_gpe_entities.items():
        location_candidates_by_name: dict[str, list[LocationCandidate]] = {}
        resolved_states_in_article: list[StateCandidate] = []
        for name in entity_names:
            state_candidates = alias_to_states.get(name)
            if state_candidates:
                # Prefer state. Ambiguous states are skipped (no fallback to location).
                if len(state_candidates) == 1:
                    chosen = state_candidates[0]
                    states.append(
                        ArticleState(
                            article_id=article_id,
                            wikidata_qid=chosen.wikidata_qid,
                            name=name,
                        )
                    )
                    resolved_states_in_article.append(chosen)
                else:
                    logger.debug(
                        "Skipping ambiguous state alias %s (%d candidates) in article %s",
                        name, len(state_candidates), article_id,
                    )
                continue

            if name in alias_to_locations:
                location_candidates_by_name[name] = alias_to_locations[name]
            else:
                logger.debug("No state or location alias found for GPE entity: %s", name)

        # Build per-article context from unambiguous locations AND resolved states.
        # State context replaces the country-name signal we used to get from
        # location_type='country' rows before the country→state migration.
        location_context = _build_location_context(location_candidates_by_name)
        for state in resolved_states_in_article:
            location_context.add(state.iso_alpha_2)
            location_context.add(state.name.upper())

        for entity_name, candidates in location_candidates_by_name.items():
            resolved = _disambiguate_location(candidates, location_context)
            for location in resolved:
                locations.append(
                    ArticleLocation(
                        article_id=article_id,
                        wikidata_qid=location.wikidata_qid,
                        name=entity_name,
                    )
                )

    return states, locations


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
    states: list[ArticleState],
    alias_to_locations: dict[str, list[LocationCandidate]],
    alias_to_states: dict[str, list[StateCandidate]],
) -> dict[str, set[str]]:
    """
    Build per-article country code sets from resolved locations and states.

    Both location.country_code (for cities/regions) and state.iso_alpha_2
    contribute to the context. State coverage is critical once country-shaped
    kb_locations rows are cleaned up: country mentions only appear in
    alias_to_states after that point.
    """
    article_country_codes: dict[str, set[str]] = defaultdict(set)

    for loc in locations:
        for candidate in alias_to_locations.get(loc.name, []):
            if candidate.wikidata_qid == loc.wikidata_qid and candidate.country_code:
                article_country_codes[loc.article_id].add(candidate.country_code)
                break

    for state in states:
        for candidate in alias_to_states.get(state.name, []):
            if candidate.wikidata_qid == state.wikidata_qid:
                article_country_codes[state.article_id].add(candidate.iso_alpha_2)
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
                logger.debug("No person alias found for PERSON entity: %s", entity_name)
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


# ---------------------------------------------------------------------------
# Organization resolution
# ---------------------------------------------------------------------------


def _resolve_organizations(
    article_org_entities: dict[str, list[str]],
    alias_to_organizations: dict[str, list[str]],
) -> list[ArticleOrganization]:
    """
    Resolve ORG entities to organisations.

    Conservative rule: accept only aliases that map to exactly one QID.
    Ambiguous aliases (e.g. "Apple" the company vs the fruit) are skipped
    to avoid polluting the KB link table.
    """
    results: list[ArticleOrganization] = []

    for article_id, entity_names in article_org_entities.items():
        for entity_name in entity_names:
            candidates = alias_to_organizations.get(entity_name, [])
            if not candidates:
                logger.debug("No organisation alias found for ORG entity: %s", entity_name)
                continue
            if len(candidates) > 1:
                logger.debug(
                    "Skipping ambiguous ORG alias %s (%d candidates)",
                    entity_name,
                    len(candidates),
                )
                continue
            results.append(
                ArticleOrganization(
                    article_id=article_id,
                    wikidata_qid=candidates[0],
                    name=entity_name,
                )
            )

    return results
