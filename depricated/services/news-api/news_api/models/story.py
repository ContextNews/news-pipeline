"""Story Pydantic models."""

from datetime import datetime

from pydantic import BaseModel, Field

from news_api.models.article import Entity


class StorySubEntity(BaseModel):
    """Sub-entity (city/region/state) within a country."""

    name: str
    mention_count: int
    in_headline_ratio: float


class StoryLocation(BaseModel):
    """Country-level location with aggregated sub-entities."""

    name: str
    country_code: str
    confidence: float
    mention_count: int
    in_headline_ratio: float
    sub_entities: list[StorySubEntity] = Field(default_factory=list)


class StoryResponse(BaseModel):
    """Story response model."""

    story_id: str
    title: str
    article_count: int
    sources: list[str]
    top_entities: list[Entity]
    locations: list[StoryLocation]
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
    locations: list[StoryLocation] = Field(default_factory=list)
    articles: list[ArticleSummary]
