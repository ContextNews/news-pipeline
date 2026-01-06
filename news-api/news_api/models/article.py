"""Article Pydantic models."""

from datetime import datetime

from pydantic import BaseModel, Field


class Entity(BaseModel):
    """Named entity extracted from article."""

    text: str
    type: str
    count: int


class SubEntity(BaseModel):
    """A city, region, or state within a country."""

    name: str
    count: int
    in_headline: bool


class Location(BaseModel):
    """A country with its sub-entities (cities, regions, states)."""

    name: str  # Canonical country name
    country_code: str  # ISO 3166-1 alpha-2
    count: int  # Total mentions (country + all sub-entities)
    in_headline: bool  # True if country OR any sub-entity in headline
    confidence: float  # Score based on frequency + headline presence
    sub_entities: list[SubEntity] = Field(default_factory=list)


class ArticleResponse(BaseModel):
    """Article response model."""

    article_id: str
    source: str
    headline: str
    url: str
    published_at: datetime
    fetched_at: datetime
    content_clean: str | None = None
    entities: list[Entity] = Field(default_factory=list)
    locations: list[Location] = Field(default_factory=list)
    embedding_combined: list[float] | None = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None,
        }


class ArticleListResponse(BaseModel):
    """Paginated list of articles."""

    articles: list[ArticleResponse]
    total: int
    limit: int
    offset: int
