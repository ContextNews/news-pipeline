"""Article API endpoints."""

from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from news_api.config import APIConfig, get_config
from news_api.models.article import ArticleListResponse, ArticleResponse
from news_api.services.article_service import ArticleService

router = APIRouter(prefix="/articles", tags=["articles"])


def get_article_service(config: Annotated[APIConfig, Depends(get_config)]) -> ArticleService:
    """Dependency to get article service."""
    return ArticleService(config)


@router.get("", response_model=ArticleListResponse)
async def list_articles(
    service: Annotated[ArticleService, Depends(get_article_service)],
    dt: Annotated[date | None, Query(alias="date", description="Date to query (YYYY-MM-DD)")] = None,
    source: Annotated[str | None, Query(description="Filter by source")] = None,
    limit: Annotated[int, Query(ge=1, le=200, description="Max results")] = 50,
    offset: Annotated[int, Query(ge=0, description="Pagination offset")] = 0,
    include_embeddings: Annotated[bool, Query(description="Include embedding vectors")] = False,
):
    """List articles with optional filtering.

    Returns normalized articles for a given date with support for:
    - Filtering by source
    - Pagination
    - Optional embedding vectors
    """
    query_date = dt or date.today()

    articles, total = service.list_articles(
        dt=query_date,
        source=source,
        limit=limit,
        offset=offset,
        include_embeddings=include_embeddings,
    )

    return ArticleListResponse(
        articles=[ArticleResponse(**a) for a in articles],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/{article_id}", response_model=ArticleResponse)
async def get_article(
    article_id: str,
    service: Annotated[ArticleService, Depends(get_article_service)],
    dt: Annotated[date | None, Query(alias="date", description="Date hint for faster lookup")] = None,
    include_embeddings: Annotated[bool, Query(description="Include embedding vectors")] = False,
):
    """Get a single article by ID.

    Providing a date hint improves lookup performance by limiting the search
    to a single partition.
    """
    article = service.get_article(
        article_id=article_id,
        dt=dt,
        include_embeddings=include_embeddings,
    )

    if article is None:
        raise HTTPException(status_code=404, detail="Article not found")

    return ArticleResponse(**article)
