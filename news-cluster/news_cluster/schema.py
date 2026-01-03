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
    locations: list["Location"]
    articles: list[ArticleSummary]

    def to_dict(self) -> dict:
        return {
            "story_id": self.story_id,
            "title": self.title,
            "locations": [
                {"name": loc.name, "confidence": loc.confidence}
                for loc in self.locations
            ],
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
class Location:
    """Aggregated location from a story's articles."""
    name: str
    confidence: float


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
    locations: list[Location]
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
            "locations": [
                {"name": loc.name, "confidence": loc.confidence}
                for loc in self.locations
            ],
            "story_embedding": self.story_embedding,
            "start_published_at": self.start_published_at,
            "end_published_at": self.end_published_at,
            "created_at": self.created_at,
        }
