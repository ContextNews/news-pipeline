from news_normalize.config import MAX_LOCATIONS
from news_normalize.extract.schema import Entity, Location


def rank_locations(entities: list[Entity], headline: str) -> list[Location]:
    """
    Rank locations from entities by frequency and headline presence.

    Returns top N locations with confidence scores.
    """
    # Filter to location-like entities (GPE = geopolitical entity, LOC = location)
    location_entities = [e for e in entities if e.type in ("GPE", "LOC")]

    if not location_entities:
        return []

    headline_lower = headline.lower()

    # Score locations
    scored: list[tuple[Entity, float]] = []
    for entity in location_entities:
        # Base score from mention frequency (normalized)
        max_count = max(e.count for e in location_entities)
        frequency_score = entity.count / max_count if max_count > 0 else 0

        # Bonus for headline presence
        headline_bonus = 0.3 if entity.text.lower() in headline_lower else 0

        confidence = min(1.0, frequency_score * 0.7 + headline_bonus)
        scored.append((entity, confidence))

    # Sort by confidence descending
    scored.sort(key=lambda x: x[1], reverse=True)

    # Return top N
    return [
        Location(name=entity.text, confidence=round(confidence, 2))
        for entity, confidence in scored[:MAX_LOCATIONS]
    ]
