"""Country name normalization using pycountry and custom aliases."""

import pycountry

# Custom aliases for variations not covered by pycountry
# Maps lowercase alias → ISO 3166-1 alpha-2 code
CUSTOM_ALIASES: dict[str, str] = {
    # United Kingdom variations
    "uk": "GB",
    "u.k.": "GB",
    "u.k": "GB",
    "britain": "GB",
    "great britain": "GB",
    "england": "GB",
    "scotland": "GB",
    "wales": "GB",
    "northern ireland": "GB",
    "the uk": "GB",
    "the united kingdom": "GB",

    # United States variations
    "us": "US",
    "u.s.": "US",
    "u.s": "US",
    "usa": "US",
    "u.s.a.": "US",
    "u.s.a": "US",
    "america": "US",
    "the us": "US",
    "the usa": "US",
    "the united states": "US",
    "the states": "US",

    # UAE variations
    "uae": "AE",
    "u.a.e.": "AE",
    "u.a.e": "AE",
    "the uae": "AE",
    "emirates": "AE",

    # Netherlands variations
    "holland": "NL",
    "the netherlands": "NL",

    # Other common variations
    "russia": "RU",
    "soviet union": "RU",  # Historical, maps to modern Russia
    "ussr": "RU",
    "burma": "MM",
    "ivory coast": "CI",
    "cote d'ivoire": "CI",
    "czech republic": "CZ",
    "czechia": "CZ",
    "swaziland": "SZ",
    "eswatini": "SZ",
    "macedonia": "MK",
    "north macedonia": "MK",
    "the philippines": "PH",
    "philippines": "PH",
    "the congo": "CD",
    "congo": "CD",
    "drc": "CD",
    "dr congo": "CD",
    "democratic republic of congo": "CD",
    "republic of congo": "CG",
    "congo-brazzaville": "CG",
    "south korea": "KR",
    "korea": "KR",  # In news context, usually means South Korea
    "north korea": "KP",
    "dprk": "KP",
    "taiwan": "TW",
    "republic of china": "TW",
    "roc": "TW",
    "china": "CN",
    "prc": "CN",
    "peoples republic of china": "CN",
    "people's republic of china": "CN",
    "mainland china": "CN",
    "the vatican": "VA",
    "vatican": "VA",
    "vatican city": "VA",
    "holy see": "VA",
    "palestine": "PS",
    "palestinian territories": "PS",
    "gaza": "PS",
    "west bank": "PS",
    "kosovo": "XK",  # Not in ISO, but commonly used
    "the gambia": "GM",
    "gambia": "GM",
    "the bahamas": "BS",
    "bahamas": "BS",
    "the maldives": "MV",
    "maldives": "MV",
    "the seychelles": "SC",
    "seychelles": "SC",
    "the comoros": "KM",
    "comoros": "KM",
    "the sudan": "SD",
    "sudan": "SD",
    "south sudan": "SS",
    "east timor": "TL",
    "timor-leste": "TL",
    "cape verde": "CV",
    "cabo verde": "CV",
}

# Names that are ambiguous (could be country or something else)
AMBIGUOUS_NAMES: set[str] = {
    "georgia",      # Country or US state
    "jordan",       # Country or person's name
    "chad",         # Country or person's name
    "china",        # Usually PRC, but could mean Taiwan in some contexts
    "guinea",       # Multiple countries: Guinea, Guinea-Bissau, Equatorial Guinea, Papua New Guinea
}

# Cache for the built index
_country_index: dict[str, str] | None = None


def _build_country_index() -> dict[str, str]:
    """Build a lookup index from all country names/codes to alpha-2 codes."""
    index: dict[str, str] = {}

    # Add all pycountry entries
    for country in pycountry.countries:
        alpha2 = country.alpha_2

        # Official name
        index[country.name.lower()] = alpha2

        # Common name if different
        if hasattr(country, "common_name"):
            index[country.common_name.lower()] = alpha2

        # Official name if different
        if hasattr(country, "official_name"):
            index[country.official_name.lower()] = alpha2

        # Alpha-2 code (e.g., "US", "GB")
        index[country.alpha_2.lower()] = alpha2

        # Alpha-3 code (e.g., "USA", "GBR")
        index[country.alpha_3.lower()] = alpha2

    # Add custom aliases (these override pycountry if there's a conflict)
    for alias, code in CUSTOM_ALIASES.items():
        index[alias.lower()] = code

    return index


def _get_country_index() -> dict[str, str]:
    """Get or build the country index (cached)."""
    global _country_index
    if _country_index is None:
        _country_index = _build_country_index()
    return _country_index


def normalize_country(name: str) -> tuple[str, str] | None:
    """
    Normalize a country name to its canonical form.

    Args:
        name: The country name/alias to normalize (e.g., "U.K.", "Britain", "GB")

    Returns:
        Tuple of (canonical_name, iso_alpha2_code) or None if not recognized.
        Example: ("United Kingdom", "GB")
    """
    if not name:
        return None

    # Clean and lowercase for lookup
    cleaned = name.strip().lower()

    # Remove common prefixes that might interfere
    if cleaned.startswith("the ") and cleaned not in _get_country_index():
        cleaned_no_the = cleaned[4:]
        if cleaned_no_the in _get_country_index():
            cleaned = cleaned_no_the

    # Look up in index
    index = _get_country_index()
    alpha2 = index.get(cleaned)

    if alpha2 is None:
        return None

    # Get canonical name from pycountry
    try:
        country = pycountry.countries.get(alpha_2=alpha2)
        if country:
            # Prefer common_name if available (e.g., "Taiwan" vs "Taiwan, Province of China")
            if hasattr(country, "common_name"):
                canonical = country.common_name
            else:
                canonical = country.name
            return (canonical, alpha2)
    except (KeyError, LookupError):
        pass

    # Fallback for codes not in pycountry (e.g., Kosovo "XK")
    # Return the original name capitalized
    return (name.title(), alpha2)


def is_ambiguous(name: str) -> bool:
    """Check if a location name is ambiguous (could be country or something else)."""
    return name.strip().lower() in AMBIGUOUS_NAMES


def get_canonical_name(alpha2: str) -> str | None:
    """Get the canonical country name for an ISO alpha-2 code."""
    try:
        country = pycountry.countries.get(alpha_2=alpha2)
        if country:
            if hasattr(country, "common_name"):
                return country.common_name
            return country.name
    except (KeyError, LookupError):
        pass
    return None
