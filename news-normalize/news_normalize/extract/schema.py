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
class Location:
    name: str
    confidence: float


@dataclass
class NormalizedArticle:
    """
    Output article with raw fields preserved and normalization fields added.

    Raw fields (preserved from input):
        article_id, source, url, published_at, headline, body, content,
        fetched_at, resolution

    Added by normalization:
        content_clean, entities, locations, ner_model, normalized_at
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
            "locations": [{"name": loc.name, "confidence": loc.confidence} for loc in self.locations],
            "ner_model": self.ner_model,
            "normalized_at": self.normalized_at,
        }
