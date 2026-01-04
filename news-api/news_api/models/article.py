"""Article Pydantic models."""

from datetime import datetime

from pydantic import BaseModel, Field


class Entity(BaseModel):
    """Named entity extracted from article."""

    text: str
    type: str
    count: int


class Location(BaseModel):
    """Location extracted from article."""

    name: str
    confidence: float
    original: str | None = None
    country_code: str | None = None
    type: str = "unknown"
    parent_region: str | None = None


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
