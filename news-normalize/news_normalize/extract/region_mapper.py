"""Region/state to country mapping."""

# Maps lowercase region/state names to ISO 3166-1 alpha-2 country codes
REGION_TO_COUNTRY: dict[str, str] = {
    # United States - All 50 states
    "alabama": "US",
    "alaska": "US",
    "arizona": "US",
    "arkansas": "US",
    "california": "US",
    "colorado": "US",
    "connecticut": "US",
    "delaware": "US",
    "florida": "US",
    "georgia": "US",  # Note: Also a country, but in US context usually the state
    "hawaii": "US",
    "idaho": "US",
    "illinois": "US",
    "indiana": "US",
    "iowa": "US",
    "kansas": "US",
    "kentucky": "US",
    "louisiana": "US",
    "maine": "US",
    "maryland": "US",
    "massachusetts": "US",
    "michigan": "US",
    "minnesota": "US",
    "mississippi": "US",
    "missouri": "US",
    "montana": "US",
    "nebraska": "US",
    "nevada": "US",
    "new hampshire": "US",
    "new jersey": "US",
    "new mexico": "US",
    "new york": "US",  # State (city handled separately)
    "north carolina": "US",
    "north dakota": "US",
    "ohio": "US",
    "oklahoma": "US",
    "oregon": "US",
    "pennsylvania": "US",
    "rhode island": "US",
    "south carolina": "US",
    "south dakota": "US",
    "tennessee": "US",
    "texas": "US",
    "utah": "US",
    "vermont": "US",
    "virginia": "US",
    "washington": "US",  # State (DC handled separately)
    "west virginia": "US",
    "wisconsin": "US",
    "wyoming": "US",
    # US Territories
    "puerto rico": "US",
    "guam": "US",
    "us virgin islands": "US",
    "american samoa": "US",

    # United Kingdom - Constituent countries
    "england": "GB",
    "scotland": "GB",
    "wales": "GB",
    "northern ireland": "GB",

    # Canada - Provinces and territories
    "ontario": "CA",
    "quebec": "CA",
    "british columbia": "CA",
    "alberta": "CA",
    "manitoba": "CA",
    "saskatchewan": "CA",
    "nova scotia": "CA",
    "new brunswick": "CA",
    "newfoundland": "CA",
    "newfoundland and labrador": "CA",
    "prince edward island": "CA",
    "northwest territories": "CA",
    "yukon": "CA",
    "nunavut": "CA",

    # Australia - States and territories
    "new south wales": "AU",
    "victoria": "AU",
    "queensland": "AU",
    "western australia": "AU",
    "south australia": "AU",
    "tasmania": "AU",
    "northern territory": "AU",
    "australian capital territory": "AU",

    # Germany - States (Länder)
    "bavaria": "DE",
    "baden-württemberg": "DE",
    "baden-wurttemberg": "DE",
    "berlin": "DE",
    "brandenburg": "DE",
    "bremen": "DE",
    "hamburg": "DE",
    "hesse": "DE",
    "lower saxony": "DE",
    "mecklenburg-vorpommern": "DE",
    "north rhine-westphalia": "DE",
    "rhineland-palatinate": "DE",
    "saarland": "DE",
    "saxony": "DE",
    "saxony-anhalt": "DE",
    "schleswig-holstein": "DE",
    "thuringia": "DE",

    # Spain - Autonomous communities
    "catalonia": "ES",
    "andalusia": "ES",
    "madrid": "ES",
    "valencia": "ES",
    "galicia": "ES",
    "basque country": "ES",
    "castile and león": "ES",
    "castile-la mancha": "ES",

    # Italy - Regions
    "lombardy": "IT",
    "lazio": "IT",
    "campania": "IT",
    "sicily": "IT",
    "veneto": "IT",
    "piedmont": "IT",
    "emilia-romagna": "IT",
    "tuscany": "IT",

    # France - Regions
    "île-de-france": "FR",
    "ile-de-france": "FR",
    "provence": "FR",
    "brittany": "FR",
    "normandy": "FR",
    "occitanie": "FR",
    "nouvelle-aquitaine": "FR",

    # China - Major regions/provinces
    "guangdong": "CN",
    "shandong": "CN",
    "henan": "CN",
    "sichuan": "CN",
    "jiangsu": "CN",
    "hebei": "CN",
    "hunan": "CN",
    "anhui": "CN",
    "hubei": "CN",
    "zhejiang": "CN",
    "xinjiang": "CN",
    "tibet": "CN",
    "hong kong": "CN",
    "macau": "CN",

    # India - Major states
    "maharashtra": "IN",
    "uttar pradesh": "IN",
    "bihar": "IN",
    "west bengal": "IN",
    "tamil nadu": "IN",
    "karnataka": "IN",
    "gujarat": "IN",
    "rajasthan": "IN",
    "kerala": "IN",
    "punjab": "IN",

    # Brazil - States
    "são paulo": "BR",
    "sao paulo": "BR",
    "rio de janeiro": "BR",
    "minas gerais": "BR",
    "bahia": "BR",
    "paraná": "BR",
    "parana": "BR",
    "rio grande do sul": "BR",

    # Russia - Major regions
    "moscow oblast": "RU",
    "siberia": "RU",
    "crimea": "RU",  # Disputed
    "chechnya": "RU",

    # Japan - Regions
    "hokkaido": "JP",
    "kanto": "JP",
    "kansai": "JP",
    "kyushu": "JP",
    "okinawa": "JP",
}


def get_country_for_region(region_name: str) -> str | None:
    """
    Get the country code for a region/state name.

    Args:
        region_name: The region or state name to look up

    Returns:
        ISO alpha-2 country code or None if not found
    """
    return REGION_TO_COUNTRY.get(region_name.lower().strip())
