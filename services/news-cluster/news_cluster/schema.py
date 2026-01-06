"""Schema definitions for clustering output."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class ArticleSummary:
    """Summary of an article within a story."""
    article_id: str
    headline: str
    source: str


@dataclass
class StoryArticles:
    """A story with its constituent articles (denormalized view)."""
    story_id: str
    title: str
    locations: list["HierarchicalLocation"]
    articles: list[ArticleSummary]

    def to_dict(self) -> dict:
        return {
            "story_id": self.story_id,
            "title": self.title,
            "locations": [loc.to_dict() for loc in self.locations],
            "articles": [
                {"article_id": a.article_id, "headline": a.headline, "source": a.source}
                for a in self.articles
            ],
        }


@dataclass
class Entity:
    """Aggregated entity from a story's articles."""
    text: str
    type: str
    count: int


@dataclass
class SubLocation:
    """A city or region within a country."""
    name: str
    type: str  # "region" or "city"
    mention_count: int


@dataclass
class HierarchicalLocation:
    """A country with optional sub-locations (regions and cities)."""
    name: str
    country_code: str
    confidence: float
    mention_count: int
    regions: list[SubLocation] = field(default_factory=list)
    cities: list[SubLocation] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "country_code": self.country_code,
            "confidence": round(self.confidence, 2),
            "mention_count": self.mention_count,
            "regions": [
                {"name": r.name, "type": r.type, "mention_count": r.mention_count}
                for r in self.regions
            ],
            "cities": [
                {"name": c.name, "type": c.type, "mention_count": c.mention_count}
                for c in self.cities
            ],
        }


# Keep old Location for backward compatibility during transition
@dataclass
class Location:
    """Aggregated location from a story's articles (legacy flat structure)."""
    name: str
    confidence: float
    country_code: Optional[str] = None


@dataclass
class ArticleStoryMap:
    """Maps an article to its assigned story."""
    article_id: str
    story_id: Optional[str]  # None if unclustered
    cluster_label: int  # -1 for unclustered
    assigned_at: datetime = field(default_factory=_utcnow)

    def to_dict(self) -> dict:
        return {
            "article_id": self.article_id,
            "story_id": self.story_id,
            "cluster_label": self.cluster_label,
            "assigned_at": self.assigned_at,
        }


@dataclass
class Story:
    """A clustered story aggregating multiple articles."""
    story_id: str
    title: str
    article_count: int
    sources: list[str]
    top_entities: list[Entity]
    locations: list[HierarchicalLocation]
    story_embedding: list[float]
    start_published_at: datetime
    end_published_at: datetime
    created_at: datetime = field(default_factory=_utcnow)

    def to_dict(self) -> dict:
        return {
            "story_id": self.story_id,
            "title": self.title,
            "article_count": self.article_count,
            "sources": self.sources,
            "top_entities": [
                {"text": e.text, "type": e.type, "count": e.count}
                for e in self.top_entities
            ],
            "locations": [loc.to_dict() for loc in self.locations],
            "story_embedding": self.story_embedding,
            "start_published_at": self.start_published_at,
            "end_published_at": self.end_published_at,
            "created_at": self.created_at,
        }
