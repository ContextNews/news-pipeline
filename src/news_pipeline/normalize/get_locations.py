"""Location extraction from GPE entities."""

import logging
from functools import lru_cache

import pycountry
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

from news_pipeline.normalize.models import Entity, Location, SubEntity

logger = logging.getLogger(__name__)

# Country aliases
COUNTRY_ALIASES = {
    "uk": "GB", "u.k.": "GB", "britain": "GB", "england": "GB",
    "us": "US", "u.s.": "US", "usa": "US", "america": "US",
    "uae": "AE", "russia": "RU", "holland": "NL",
    "south korea": "KR", "korea": "KR", "north korea": "KP",
    "taiwan": "TW", "china": "CN",
}

_geocoder: Nominatim | None = None


def _get_geocoder() -> Nominatim:
    global _geocoder
    if _geocoder is None:
        _geocoder = Nominatim(user_agent="news-normalize/1.0")
    return _geocoder


@lru_cache(maxsize=10000)
def _resolve_gpe(name: str) -> tuple[str, str, bool] | None:
    """Resolve GPE to (country_code, country_name, is_country)."""
    if not name:
        return None

    cleaned = name.strip().lower()

    # Check aliases
    if cleaned in COUNTRY_ALIASES:
        code = COUNTRY_ALIASES[cleaned]
        country = pycountry.countries.get(alpha_2=code)
        return (code, getattr(country, "common_name", country.name), True) if country else None

    # Check pycountry
    try:
        country = pycountry.countries.lookup(name)
        return (country.alpha_2, getattr(country, "common_name", country.name), True)
    except LookupError:
        pass

    # Check subdivisions
    try:
        sub = pycountry.subdivisions.lookup(name)
        country = pycountry.countries.get(alpha_2=sub.country_code)
        if country:
            return (sub.country_code, getattr(country, "common_name", country.name), False)
    except LookupError:
        pass

    # Fallback to geocoder
    try:
        loc = _get_geocoder().geocode(name, addressdetails=True, language="en", timeout=5)
        if loc and "address" in loc.raw:
            code = loc.raw["address"].get("country_code", "").upper()
            if code and len(code) == 2:
                country = pycountry.countries.get(alpha_2=code)
                if country:
                    return (code, getattr(country, "common_name", country.name), False)
    except (GeocoderTimedOut, GeocoderServiceError, Exception) as e:
        logger.debug(f"Geocoder error for {name}: {e}")

    return None


def get_locations(entities: list[Entity], headline: str) -> list[Location]:
    """Extract locations from GPE entities, grouped by country."""
    gpe_entities = [e for e in entities if e.type == "GPE"]
    if not gpe_entities:
        return []

    headline_lower = headline.lower()
    countries: dict[str, dict] = {}

    for entity in gpe_entities:
        result = _resolve_gpe(entity.text)
        if not result:
            continue

        code, name, is_country = result
        in_headline = entity.text.lower() in headline_lower

        if code not in countries:
            countries[code] = {
                "name": name,
                "country_count": 0,
                "country_in_headline": False,
                "sub_entities": {},
            }

        if is_country:
            countries[code]["country_count"] += entity.count
            if in_headline:
                countries[code]["country_in_headline"] = True
        else:
            subs = countries[code]["sub_entities"]
            if entity.text not in subs:
                subs[entity.text] = {"count": 0, "in_headline": False}
            subs[entity.text]["count"] += entity.count
            if in_headline:
                subs[entity.text]["in_headline"] = True

    if not countries:
        return []

    # Calculate totals and confidence
    max_count = max(
        c["country_count"] + sum(s["count"] for s in c["sub_entities"].values())
        for c in countries.values()
    )

    locations = []
    for code, data in countries.items():
        total = data["country_count"] + sum(s["count"] for s in data["sub_entities"].values())
        in_headline = data["country_in_headline"] or any(s["in_headline"] for s in data["sub_entities"].values())
        confidence = min(1.0, (total / max_count * 0.7) + (0.3 if in_headline else 0))

        sub_entities = [
            SubEntity(name=name, count=s["count"], in_headline=s["in_headline"])
            for name, s in sorted(data["sub_entities"].items(), key=lambda x: -x[1]["count"])
        ]

        locations.append(Location(
            name=data["name"],
            country_code=code,
            count=total,
            in_headline=in_headline,
            confidence=round(confidence, 2),
            sub_entities=sub_entities,
        ))

    locations.sort(key=lambda x: -x.confidence)
    return locations
