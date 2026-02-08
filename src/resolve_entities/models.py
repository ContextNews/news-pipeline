from dataclasses import dataclass


@dataclass
class LocationCandidate:
    wikidata_qid: str
    name: str
    location_type: str
    country_code: str | None


@dataclass
class PersonCandidate:
    wikidata_qid: str
    name: str
    description: str | None
    nationalities: list[str] | None


@dataclass
class ArticleLocation:
    article_id: str
    wikidata_qid: str
    name: str  # the GPE entity name that matched


@dataclass
class ArticlePerson:
    article_id: str
    wikidata_qid: str
    name: str  # the PERSON entity name that matched
