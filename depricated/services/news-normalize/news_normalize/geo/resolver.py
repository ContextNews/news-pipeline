"""Resolve GPE entities to their parent country."""

import logging
from functools import lru_cache

import pycountry
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

from news_normalize.geo.country_normalizer import normalize_country, get_canonical_name

logger = logging.getLogger(__name__)

# Cache geocoder instance
_geocoder: Nominatim | None = None


def _get_geocoder() -> Nominatim:
    """Get or create the Nominatim geocoder instance."""
    global _geocoder
    if _geocoder is None:
        _geocoder = Nominatim(user_agent="news-normalize/1.0")
    return _geocoder


@lru_cache(maxsize=10000)
def resolve_gpe(name: str) -> tuple[str, str, bool] | None:
    """
    Resolve a GPE entity to its parent country.

    Args:
        name: The GPE entity text (e.g., "California", "London", "France")

    Returns:
        Tuple of (country_code, canonical_country_name, is_country) or None if unresolved.
        - is_country=True means the entity itself is a country
        - is_country=False means it's a sub-entity (city/region)
    """
    if not name or not name.strip():
        return None

    cleaned = name.strip()

    # 1. Check if it's a country
    country_result = normalize_country(cleaned)
    if country_result:
        canonical_name, country_code = country_result
        return (country_code, canonical_name, True)

    # 2. Try pycountry.subdivisions (regions/states/provinces)
    try:
        subdivision = pycountry.subdivisions.lookup(cleaned)
        country_code = subdivision.country_code
        canonical_name = get_canonical_name(country_code) or country_code
        return (country_code, canonical_name, False)
    except LookupError:
        pass

    # 3. Fallback to Nominatim for cities
    try:
        location = _get_geocoder().geocode(
            cleaned,
            addressdetails=True,
            language="en",
            timeout=5,
        )
        if location and "address" in location.raw:
            address = location.raw["address"]
            country_code = address.get("country_code", "").upper()
            if country_code and len(country_code) == 2:
                canonical_name = get_canonical_name(country_code) or address.get(
                    "country", country_code
                )
                return (country_code, canonical_name, False)
    except GeocoderTimedOut:
        logger.warning(f"Nominatim timeout resolving: {cleaned}")
    except GeocoderServiceError as e:
        logger.warning(f"Nominatim service error resolving {cleaned}: {e}")
    except Exception as e:
        logger.warning(f"Unexpected error resolving {cleaned}: {e}")

    return None
