from dataclasses import dataclass


@dataclass
class ArticleLocation:
    article_id: str
    wikidata_qid: str
    name: str  # the GPE entity name that matched


@dataclass
class LocationCandidate:
    wikidata_qid: str
    name: str
    location_type: str
    country_code: str | None
