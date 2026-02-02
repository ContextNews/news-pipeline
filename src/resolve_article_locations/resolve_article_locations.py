"""Resolve GPE entities to locations using disambiguation heuristics."""

from __future__ import annotations

from resolve_article_locations.models import ArticleLocation, LocationCandidate


def resolve_article_locations(
    article_entities: dict[str, list[str]],
    alias_to_locations: dict[str, list[LocationCandidate]],
) -> list[ArticleLocation]:
    """
    Resolve GPE entities to locations using disambiguation heuristics.

    Args:
        article_entities: Mapping of article_id to list of GPE entity names (uppercase)
        alias_to_locations: Mapping of alias (uppercase) to list of candidate locations

    Returns:
        List of ArticleLocation mappings
    """
    results = []

    for article_id, entity_names in article_entities.items():
        # Get all candidates for all entities in this article
        entity_candidates: dict[str, list[LocationCandidate]] = {}
        for name in entity_names:
            if name in alias_to_locations:
                entity_candidates[name] = alias_to_locations[name]

        # Build context: all resolved country/state names from unambiguous entities
        context = _build_context(entity_candidates)

        # Resolve each entity
        for entity_name, candidates in entity_candidates.items():
            resolved = _disambiguate(candidates, context)
            for location in resolved:
                results.append(
                    ArticleLocation(
                        article_id=article_id,
                        wikidata_qid=location.wikidata_qid,
                        name=entity_name,
                    )
                )

    return results


def _build_context(entity_candidates: dict[str, list[LocationCandidate]]) -> set[str]:
    """
    Extract unambiguous country codes and location names for context.
    """
    context = set()
    for candidates in entity_candidates.values():
        if len(candidates) == 1:
            loc = candidates[0]
            if loc.country_code:
                context.add(loc.country_code)
            if loc.location_type == "country":
                context.add(loc.name.upper())
    return context


def _disambiguate(
    candidates: list[LocationCandidate],
    context: set[str],
) -> list[LocationCandidate]:
    """
    Apply heuristics to select the best candidate(s).
    """
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

    # Return the highest priority type
    best_type = candidates[0].location_type
    return [c for c in candidates if c.location_type == best_type]
