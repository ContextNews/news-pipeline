"""Location extraction and ranking."""

from news_normalize.config import MAX_LOCATIONS
from news_normalize.extract.city_mapper import get_location_for_city
from news_normalize.extract.country_normalizer import normalize_country
from news_normalize.extract.region_mapper import get_country_for_region
from news_normalize.extract.schema import Entity, Location


def classify_location(name: str) -> tuple[str, str | None, str, str | None]:
    """
    Classify a location and determine its type and parent country.

    Args:
        name: The location name to classify

    Returns:
        Tuple of (canonical_name, country_code, type, parent_region)
        - type is "country", "region", "city", or "unknown"
        - parent_region is set for cities that belong to a known region
    """
    # Try country first
    country_result = normalize_country(name)
    if country_result:
        return (country_result[0], country_result[1], "country", None)

    cleaned = name.lower().strip()

    # Try region/state
    region_country = get_country_for_region(cleaned)
    if region_country:
        return (name.title(), region_country, "region", None)

    # Try city
    city_result = get_location_for_city(cleaned)
    if city_result:
        country_code, parent_region = city_result
        return (name.title(), country_code, "city", parent_region)

    # Unknown location
    return (name, None, "unknown", None)


def rank_locations(entities: list[Entity], headline: str) -> list[Location]:
    """
    Rank locations from entities by frequency and headline presence.

    Normalizes country names (e.g., "UK", "Britain" → "United Kingdom"),
    classifies regions and cities, and deduplicates variations before scoring.

    Returns top N locations with confidence scores.
    """
    # Filter to location-like entities (GPE = geopolitical entity, LOC = location)
    location_entities = [e for e in entities if e.type in ("GPE", "LOC")]

    if not location_entities:
        return []

    headline_lower = headline.lower()

    # Group entities by normalized name to deduplicate variations
    # Key: (canonical_name, country_code, loc_type, parent_region)
    # Value: (list of original texts, combined count, any in headline)
    grouped: dict[
        tuple[str, str | None, str, str | None],
        tuple[list[str], int, bool]
    ] = {}

    for entity in location_entities:
        original_text = entity.text
        canonical_name, country_code, loc_type, parent_region = classify_location(original_text)

        key = (canonical_name, country_code, loc_type, parent_region)

        if key not in grouped:
            grouped[key] = ([], 0, False)

        originals, total_count, in_headline = grouped[key]
        originals.append(original_text)
        total_count += entity.count
        in_headline = in_headline or (original_text.lower() in headline_lower)
        grouped[key] = (originals, total_count, in_headline)

    if not grouped:
        return []

    # Score each unique location
    max_count = max(data[1] for data in grouped.values())

    scored: list[tuple[tuple[str, str | None, str, str | None], list[str], float]] = []
    for key, (originals, total_count, in_headline) in grouped.items():
        # Base score from combined mention frequency (normalized)
        frequency_score = total_count / max_count if max_count > 0 else 0

        # Bonus for headline presence
        headline_bonus = 0.3 if in_headline else 0

        confidence = min(1.0, frequency_score * 0.7 + headline_bonus)
        scored.append((key, originals, confidence))

    # Sort by confidence descending
    scored.sort(key=lambda x: x[2], reverse=True)

    # Return top N as Location objects
    results: list[Location] = []
    for (canonical_name, country_code, loc_type, parent_region), originals, confidence in scored[:MAX_LOCATIONS]:
        # Use the most common original text, or first if tie
        original = max(set(originals), key=originals.count)

        results.append(Location(
            name=canonical_name,
            confidence=round(confidence, 2),
            original=original,
            country_code=country_code,
            type=loc_type,
            parent_region=parent_region,
        ))

    return results
