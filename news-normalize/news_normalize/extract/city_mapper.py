"""City to country (and region) mapping."""

# Maps lowercase city names to (country_code, region_name or None)
CITY_TO_LOCATION: dict[str, tuple[str, str | None]] = {
    # United States - Major cities
    "new york city": ("US", "New York"),
    "nyc": ("US", "New York"),
    "manhattan": ("US", "New York"),
    "brooklyn": ("US", "New York"),
    "los angeles": ("US", "California"),
    "la": ("US", "California"),
    "chicago": ("US", "Illinois"),
    "houston": ("US", "Texas"),
    "phoenix": ("US", "Arizona"),
    "philadelphia": ("US", "Pennsylvania"),
    "san antonio": ("US", "Texas"),
    "san diego": ("US", "California"),
    "dallas": ("US", "Texas"),
    "san jose": ("US", "California"),
    "austin": ("US", "Texas"),
    "jacksonville": ("US", "Florida"),
    "fort worth": ("US", "Texas"),
    "columbus": ("US", "Ohio"),
    "charlotte": ("US", "North Carolina"),
    "san francisco": ("US", "California"),
    "indianapolis": ("US", "Indiana"),
    "seattle": ("US", "Washington"),
    "denver": ("US", "Colorado"),
    "washington dc": ("US", None),
    "washington d.c.": ("US", None),
    "d.c.": ("US", None),
    "boston": ("US", "Massachusetts"),
    "el paso": ("US", "Texas"),
    "nashville": ("US", "Tennessee"),
    "detroit": ("US", "Michigan"),
    "portland": ("US", "Oregon"),  # Default to Oregon (larger)
    "las vegas": ("US", "Nevada"),
    "memphis": ("US", "Tennessee"),
    "louisville": ("US", "Kentucky"),
    "baltimore": ("US", "Maryland"),
    "milwaukee": ("US", "Wisconsin"),
    "albuquerque": ("US", "New Mexico"),
    "tucson": ("US", "Arizona"),
    "fresno": ("US", "California"),
    "sacramento": ("US", "California"),
    "atlanta": ("US", "Georgia"),
    "miami": ("US", "Florida"),
    "oakland": ("US", "California"),
    "minneapolis": ("US", "Minnesota"),
    "cleveland": ("US", "Ohio"),
    "pittsburgh": ("US", "Pennsylvania"),
    "new orleans": ("US", "Louisiana"),
    "st. louis": ("US", "Missouri"),
    "saint louis": ("US", "Missouri"),
    "cincinnati": ("US", "Ohio"),
    "orlando": ("US", "Florida"),
    "tampa": ("US", "Florida"),
    "honolulu": ("US", "Hawaii"),
    "silicon valley": ("US", "California"),

    # United Kingdom - Major cities
    "london": ("GB", "England"),
    "birmingham": ("GB", "England"),  # Default to UK Birmingham
    "manchester": ("GB", "England"),
    "leeds": ("GB", "England"),
    "liverpool": ("GB", "England"),
    "newcastle": ("GB", "England"),
    "bristol": ("GB", "England"),
    "sheffield": ("GB", "England"),
    "nottingham": ("GB", "England"),
    "leicester": ("GB", "England"),
    "edinburgh": ("GB", "Scotland"),
    "glasgow": ("GB", "Scotland"),
    "cardiff": ("GB", "Wales"),
    "belfast": ("GB", "Northern Ireland"),
    "oxford": ("GB", "England"),
    "cambridge": ("GB", "England"),

    # Canada - Major cities
    "toronto": ("CA", "Ontario"),
    "montreal": ("CA", "Quebec"),
    "vancouver": ("CA", "British Columbia"),
    "calgary": ("CA", "Alberta"),
    "edmonton": ("CA", "Alberta"),
    "ottawa": ("CA", "Ontario"),
    "winnipeg": ("CA", "Manitoba"),
    "quebec city": ("CA", "Quebec"),

    # Australia - Major cities
    "sydney": ("AU", "New South Wales"),
    "melbourne": ("AU", "Victoria"),
    "brisbane": ("AU", "Queensland"),
    "perth": ("AU", "Western Australia"),
    "adelaide": ("AU", "South Australia"),
    "canberra": ("AU", "Australian Capital Territory"),

    # Germany - Major cities
    "berlin": ("DE", None),
    "munich": ("DE", "Bavaria"),
    "frankfurt": ("DE", "Hesse"),
    "hamburg": ("DE", None),
    "cologne": ("DE", "North Rhine-Westphalia"),
    "düsseldorf": ("DE", "North Rhine-Westphalia"),
    "dusseldorf": ("DE", "North Rhine-Westphalia"),
    "stuttgart": ("DE", "Baden-Württemberg"),

    # France - Major cities
    "paris": ("FR", "Île-de-France"),
    "marseille": ("FR", "Provence"),
    "lyon": ("FR", None),
    "toulouse": ("FR", None),
    "nice": ("FR", "Provence"),
    "nantes": ("FR", None),
    "strasbourg": ("FR", None),
    "bordeaux": ("FR", "Nouvelle-Aquitaine"),

    # Spain - Major cities
    "madrid": ("ES", None),
    "barcelona": ("ES", "Catalonia"),
    "valencia": ("ES", None),
    "seville": ("ES", "Andalusia"),
    "bilbao": ("ES", "Basque Country"),

    # Italy - Major cities
    "rome": ("IT", "Lazio"),
    "milan": ("IT", "Lombardy"),
    "naples": ("IT", "Campania"),
    "turin": ("IT", "Piedmont"),
    "florence": ("IT", "Tuscany"),
    "venice": ("IT", "Veneto"),

    # Netherlands
    "amsterdam": ("NL", None),
    "rotterdam": ("NL", None),
    "the hague": ("NL", None),

    # Belgium
    "brussels": ("BE", None),
    "antwerp": ("BE", None),

    # Switzerland
    "zurich": ("CH", None),
    "geneva": ("CH", None),
    "bern": ("CH", None),

    # Austria
    "vienna": ("AT", None),

    # Poland
    "warsaw": ("PL", None),
    "krakow": ("PL", None),

    # Czech Republic
    "prague": ("CZ", None),

    # Russia - Major cities
    "moscow": ("RU", None),
    "st. petersburg": ("RU", None),
    "saint petersburg": ("RU", None),

    # China - Major cities
    "beijing": ("CN", None),
    "shanghai": ("CN", None),
    "guangzhou": ("CN", "Guangdong"),
    "shenzhen": ("CN", "Guangdong"),
    "hong kong": ("CN", None),
    "chengdu": ("CN", "Sichuan"),
    "wuhan": ("CN", "Hubei"),
    "xi'an": ("CN", None),
    "xian": ("CN", None),
    "hangzhou": ("CN", "Zhejiang"),
    "nanjing": ("CN", "Jiangsu"),
    "tianjin": ("CN", None),

    # Japan - Major cities
    "tokyo": ("JP", "Kanto"),
    "osaka": ("JP", "Kansai"),
    "kyoto": ("JP", "Kansai"),
    "yokohama": ("JP", "Kanto"),
    "nagoya": ("JP", None),
    "sapporo": ("JP", "Hokkaido"),
    "fukuoka": ("JP", "Kyushu"),
    "hiroshima": ("JP", None),

    # South Korea
    "seoul": ("KR", None),
    "busan": ("KR", None),
    "incheon": ("KR", None),

    # India - Major cities
    "mumbai": ("IN", "Maharashtra"),
    "bombay": ("IN", "Maharashtra"),
    "delhi": ("IN", None),
    "new delhi": ("IN", None),
    "bangalore": ("IN", "Karnataka"),
    "bengaluru": ("IN", "Karnataka"),
    "hyderabad": ("IN", None),
    "chennai": ("IN", "Tamil Nadu"),
    "kolkata": ("IN", "West Bengal"),
    "calcutta": ("IN", "West Bengal"),
    "ahmedabad": ("IN", "Gujarat"),
    "pune": ("IN", "Maharashtra"),

    # Middle East
    "dubai": ("AE", None),
    "abu dhabi": ("AE", None),
    "doha": ("QA", None),
    "riyadh": ("SA", None),
    "jeddah": ("SA", None),
    "tehran": ("IR", None),
    "tel aviv": ("IL", None),
    "jerusalem": ("IL", None),
    "beirut": ("LB", None),
    "amman": ("JO", None),
    "baghdad": ("IQ", None),
    "cairo": ("EG", None),
    "alexandria": ("EG", None),

    # Africa - Major cities
    "johannesburg": ("ZA", None),
    "cape town": ("ZA", None),
    "lagos": ("NG", None),
    "nairobi": ("KE", None),
    "casablanca": ("MA", None),
    "addis ababa": ("ET", None),

    # South America - Major cities
    "são paulo": ("BR", "São Paulo"),
    "sao paulo": ("BR", "São Paulo"),
    "rio de janeiro": ("BR", "Rio de Janeiro"),
    "rio": ("BR", "Rio de Janeiro"),
    "buenos aires": ("AR", None),
    "lima": ("PE", None),
    "bogotá": ("CO", None),
    "bogota": ("CO", None),
    "santiago": ("CL", None),
    "caracas": ("VE", None),

    # Mexico
    "mexico city": ("MX", None),
    "guadalajara": ("MX", None),
    "monterrey": ("MX", None),
    "tijuana": ("MX", None),
    "cancun": ("MX", None),

    # Southeast Asia
    "singapore": ("SG", None),
    "bangkok": ("TH", None),
    "jakarta": ("ID", None),
    "kuala lumpur": ("MY", None),
    "manila": ("PH", None),
    "ho chi minh city": ("VN", None),
    "hanoi": ("VN", None),

    # Other major cities
    "istanbul": ("TR", None),
    "ankara": ("TR", None),
    "athens": ("GR", None),
    "lisbon": ("PT", None),
    "dublin": ("IE", None),
    "stockholm": ("SE", None),
    "oslo": ("NO", None),
    "copenhagen": ("DK", None),
    "helsinki": ("FI", None),
    "kiev": ("UA", None),
    "kyiv": ("UA", None),
}


def get_location_for_city(city_name: str) -> tuple[str, str | None] | None:
    """
    Get the country code and region for a city name.

    Args:
        city_name: The city name to look up

    Returns:
        Tuple of (country_code, region_name) or None if not found.
        region_name may be None if city is not associated with a specific region.
    """
    return CITY_TO_LOCATION.get(city_name.lower().strip())
