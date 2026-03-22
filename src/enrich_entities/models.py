"""Data models for enrich_entities pipeline stage."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class WikidataCandidate:
    """A candidate entity returned from a Wikidata search."""

    qid: str
    label: str
    description: str | None


@dataclass
class KBLocation:
    """A location entity to be added to the knowledge base."""

    qid: str
    name: str
    description: str | None
    location_type: str       # 'country' | 'state' | 'city' | 'region'
    country_code: str | None  # ISO 3166-1 alpha-2


@dataclass
class KBPerson:
    """A person entity to be added to the knowledge base."""

    qid: str
    name: str
    description: str | None
    nationalities: list[str] | None  # ISO 3166-1 alpha-2 country codes


@dataclass
class EnrichedEntity:
    """An entity enriched from Wikidata, ready to be written to the KB and linked to articles."""

    entity_name: str         # original name from article_entity_mentions
    entity_type: str         # 'location' | 'person'
    qid: str
    name: str                # canonical Wikidata label
    description: str | None
    location: KBLocation | None
    person: KBPerson | None
    aliases: list[str]       # strings to add to kb_entity_aliases
    article_ids: list[str]   # articles to link in article_entities_resolved
