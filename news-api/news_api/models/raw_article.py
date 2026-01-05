"""Raw article Pydantic models."""

from datetime import datetime

from pydantic import BaseModel


class Resolution(BaseModel):
    """Article resolution metadata."""

    success: bool
    method: str | None = None
    error: str | None = None


class RawArticleResponse(BaseModel):
    """Raw article response model."""

    article_id: str
    source: str
    headline: str
    body: str
    content: str | None = None
    url: str
    published_at: datetime
    fetched_at: datetime
    resolution: Resolution

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None,
        }


class RawArticleListResponse(BaseModel):
    """Paginated list of raw articles."""

    articles: list[RawArticleResponse]
    total: int
    limit: int
    offset: int
