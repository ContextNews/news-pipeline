"""Story Pydantic models."""

from datetime import datetime

from pydantic import BaseModel, Field

from news_api.models.article import Entity


class SubLocation(BaseModel):
    """Sub-location within a country (region or city)."""

    name: str
    type: str
    mention_count: int


class HierarchicalLocation(BaseModel):
    """Hierarchical location with country and sub-locations."""

    name: str
    country_code: str
    confidence: float
    mention_count: int
    regions: list[SubLocation] = Field(default_factory=list)
    cities: list[SubLocation] = Field(default_factory=list)


class StoryResponse(BaseModel):
    """Story response model."""

    story_id: str
    title: str
    article_count: int
    sources: list[str]
    top_entities: list[Entity]
    locations: list[HierarchicalLocation]
    start_published_at: datetime
    end_published_at: datetime
    created_at: datetime

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None,
        }


class StoryListResponse(BaseModel):
    """Paginated list of stories."""

    stories: list[StoryResponse]
    total: int
    limit: int
    offset: int


class ArticleSummary(BaseModel):
    """Summary of an article within a story."""

    article_id: str
    headline: str
    source: str
    url: str | None = None
    published_at: datetime | None = None


class StoryArticlesResponse(BaseModel):
    """Story with its constituent articles."""

    story_id: str
    title: str
    locations: list[HierarchicalLocation] = Field(default_factory=list)
    articles: list[ArticleSummary]
