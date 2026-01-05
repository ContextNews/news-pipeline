"""Raw article API endpoints."""

from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from news_api.config import APIConfig, get_config
from news_api.models.raw_article import RawArticleListResponse, RawArticleResponse
from news_api.services.raw_article_service import RawArticleService

router = APIRouter(prefix="/raw-articles", tags=["raw-articles"])


def get_raw_article_service(config: Annotated[APIConfig, Depends(get_config)]) -> RawArticleService:
    """Dependency to get raw article service."""
    return RawArticleService(config)


@router.get("", response_model=RawArticleListResponse)
async def list_raw_articles(
    service: Annotated[RawArticleService, Depends(get_raw_article_service)],
    dt: Annotated[date | None, Query(alias="date", description="Date to query (YYYY-MM-DD)")] = None,
    source: Annotated[str | None, Query(description="Filter by source")] = None,
    limit: Annotated[int, Query(ge=1, le=200, description="Max results")] = 50,
    offset: Annotated[int, Query(ge=0, description="Pagination offset")] = 0,
):
    """List raw articles with optional filtering.

    Returns raw articles as ingested from news sources for a given date.
    These articles have not been normalized or enriched with entities/locations.
    """
    query_date = dt or date.today()

    articles, total = service.list_articles(
        dt=query_date,
        source=source,
        limit=limit,
        offset=offset,
    )

    return RawArticleListResponse(
        articles=[RawArticleResponse(**a) for a in articles],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/{article_id}", response_model=RawArticleResponse)
async def get_raw_article(
    article_id: str,
    service: Annotated[RawArticleService, Depends(get_raw_article_service)],
    dt: Annotated[date | None, Query(alias="date", description="Date hint for faster lookup")] = None,
):
    """Get a single raw article by ID.

    Providing a date hint improves lookup performance by limiting the search
    to a single partition.
    """
    article = service.get_article(
        article_id=article_id,
        dt=dt,
    )

    if article is None:
        raise HTTPException(status_code=404, detail="Article not found")

    return RawArticleResponse(**article)
