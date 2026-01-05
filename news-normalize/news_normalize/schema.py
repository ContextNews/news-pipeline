from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Entity:
    text: str
    type: str
    count: int


@dataclass
class SubEntity:
    """A city, region, or state within a country."""

    name: str
    count: int
    in_headline: bool


@dataclass
class Location:
    """A country with its sub-entities (cities, regions, states)."""

    name: str  # Canonical country name (e.g., "United Kingdom")
    country_code: str  # ISO 3166-1 alpha-2 (e.g., "GB")
    count: int  # Total mentions (country + all sub-entities)
    in_headline: bool  # True if country OR any sub-entity in headline
    confidence: float  # Score based on frequency + headline presence
    sub_entities: list[SubEntity] = field(default_factory=list)


@dataclass
class PreparedArticle:
    """Intermediate state holding raw fields + cleaned content before NER."""

    # Raw fields from input (preserved as-is)
    article_id: str
    source: str
    url: str
    published_at: datetime
    fetched_at: datetime
    headline: str
    body: str
    content: Optional[str]
    resolution: dict

    # Prepared for NER
    content_clean: Optional[str]

    # Error tracking (for pipeline failures, not content issues)
    error: Optional[str] = None


@dataclass
class NormalizedArticle:
    """
    Output article with raw fields preserved and normalization fields added.

    Raw fields (preserved from input):
        article_id, source, url, published_at, headline, body, content,
        fetched_at, resolution

    Added by normalization:
        content_clean, entities, locations, ner_model, normalized_at

    Embedding fields (optional, None if embeddings disabled):
        embedding_headline, embedding_content, embedding_combined,
        embedding_model, embedding_dim, embedding_chunks
    """

    # Raw fields from input (preserved as-is)
    article_id: str
    source: str
    url: str
    published_at: datetime
    fetched_at: datetime
    headline: str
    body: str
    content: Optional[str]
    resolution: dict

    # Fields added by normalization
    content_clean: Optional[str]
    ner_model: str
    entities: list[Entity] = field(default_factory=list)
    locations: list[Location] = field(default_factory=list)
    normalized_at: datetime = field(default_factory=_utcnow)

    # Embedding fields (optional - None if embeddings disabled)
    embedding_headline: Optional[list[float]] = None
    embedding_content: Optional[list[float]] = None
    embedding_combined: Optional[list[float]] = None
    embedding_model: Optional[str] = None
    embedding_dim: Optional[int] = None
    embedding_chunks: Optional[int] = None

    def to_dict(self) -> dict:
        return {
            # Raw fields (preserved)
            "article_id": self.article_id,
            "source": self.source,
            "url": self.url,
            "published_at": self.published_at,
            "fetched_at": self.fetched_at,
            "headline": self.headline,
            "body": self.body,
            "content": self.content,
            "resolution": self.resolution,
            # Added by normalization
            "content_clean": self.content_clean,
            "entities": [{"text": e.text, "type": e.type, "count": e.count} for e in self.entities],
            "locations": [
                {
                    "name": loc.name,
                    "country_code": loc.country_code,
                    "count": loc.count,
                    "in_headline": loc.in_headline,
                    "confidence": loc.confidence,
                    "sub_entities": [
                        {
                            "name": sub.name,
                            "count": sub.count,
                            "in_headline": sub.in_headline,
                        }
                        for sub in loc.sub_entities
                    ],
                }
                for loc in self.locations
            ],
            "ner_model": self.ner_model,
            "normalized_at": self.normalized_at,
            # Embedding fields
            "embedding_headline": self.embedding_headline,
            "embedding_content": self.embedding_content,
            "embedding_combined": self.embedding_combined,
            "embedding_model": self.embedding_model,
            "embedding_dim": self.embedding_dim,
            "embedding_chunks": self.embedding_chunks,
        }
