"""Location extraction and ranking from GPE entities."""

import logging

from news_normalize.geo.resolver import resolve_gpe
from news_normalize.schema import Entity, Location, SubEntity

logger = logging.getLogger(__name__)


def extract_locations(entities: list[Entity], headline: str) -> list[Location]:
    """
    Extract and rank locations from GPE entities.

    Groups entities by country, with sub-entities (cities, regions, states)
    nested under their parent country. Returns all locations sorted by confidence.

    Args:
        entities: List of Entity objects from NER
        headline: Article headline for headline presence detection

    Returns:
        List of Location objects, sorted by confidence descending
    """
    # Filter to GPE entities only
    gpe_entities = [e for e in entities if e.type == "GPE"]

    if not gpe_entities:
        return []

    headline_lower = headline.lower()

    # Build country dict: {country_code: LocationBuilder}
    countries: dict[str, _LocationBuilder] = {}

    for entity in gpe_entities:
        result = resolve_gpe(entity.text)

        if result is None:
            logger.debug(f"Could not resolve GPE: {entity.text}")
            continue

        country_code, country_name, is_country = result
        entity_in_headline = entity.text.lower() in headline_lower

        # Get or create country entry
        if country_code not in countries:
            countries[country_code] = _LocationBuilder(
                name=country_name,
                country_code=country_code,
            )

        builder = countries[country_code]

        if is_country:
            # Entity is a country - add count directly
            builder.country_count += entity.count
            if entity_in_headline:
                builder.country_in_headline = True
        else:
            # Entity is a sub-entity (city/region/state)
            builder.add_sub_entity(entity.text, entity.count, entity_in_headline)

    if not countries:
        return []

    # Calculate confidence and build Location objects
    max_count = max(b.total_count for b in countries.values())

    locations = []
    for builder in countries.values():
        confidence = _calculate_confidence(
            builder.total_count, max_count, builder.in_headline
        )
        locations.append(builder.build(confidence))

    # Sort by confidence descending
    locations.sort(key=lambda loc: loc.confidence, reverse=True)

    return locations


class _LocationBuilder:
    """Helper class to accumulate location data before building final Location."""

    def __init__(self, name: str, country_code: str):
        self.name = name
        self.country_code = country_code
        self.country_count = 0
        self.country_in_headline = False
        self.sub_entities: dict[str, tuple[int, bool]] = {}  # name -> (count, in_headline)

    def add_sub_entity(self, name: str, count: int, in_headline: bool) -> None:
        """Add or update a sub-entity."""
        if name in self.sub_entities:
            existing_count, existing_headline = self.sub_entities[name]
            self.sub_entities[name] = (
                existing_count + count,
                existing_headline or in_headline,
            )
        else:
            self.sub_entities[name] = (count, in_headline)

    @property
    def total_count(self) -> int:
        """Total count including country and all sub-entities."""
        sub_count = sum(count for count, _ in self.sub_entities.values())
        return self.country_count + sub_count

    @property
    def in_headline(self) -> bool:
        """True if country or any sub-entity appears in headline."""
        if self.country_in_headline:
            return True
        return any(in_hl for _, in_hl in self.sub_entities.values())

    def build(self, confidence: float) -> Location:
        """Build the final Location object."""
        sub_entity_list = [
            SubEntity(name=name, count=count, in_headline=in_hl)
            for name, (count, in_hl) in self.sub_entities.items()
        ]
        # Sort sub-entities by count descending
        sub_entity_list.sort(key=lambda s: s.count, reverse=True)

        return Location(
            name=self.name,
            country_code=self.country_code,
            count=self.total_count,
            in_headline=self.in_headline,
            confidence=round(confidence, 2),
            sub_entities=sub_entity_list,
        )


def _calculate_confidence(count: int, max_count: int, in_headline: bool) -> float:
    """
    Calculate confidence score for a location.

    confidence = (normalized_frequency * 0.7) + headline_bonus
    """
    normalized_frequency = count / max_count if max_count > 0 else 0
    headline_bonus = 0.3 if in_headline else 0
    return min(1.0, normalized_frequency * 0.7 + headline_bonus)
