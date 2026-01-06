"""Story API endpoints."""

from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from news_api.config import APIConfig, get_config
from news_api.models.story import (
    StoryArticlesResponse,
    StoryListResponse,
    StoryResponse,
)
from news_api.services.story_service import StoryService

router = APIRouter(prefix="/stories", tags=["stories"])


def get_story_service(config: Annotated[APIConfig, Depends(get_config)]) -> StoryService:
    """Dependency to get story service."""
    return StoryService(config)


@router.get("", response_model=StoryListResponse)
async def list_stories(
    service: Annotated[StoryService, Depends(get_story_service)],
    dt: Annotated[date | None, Query(alias="date", description="Date to query (YYYY-MM-DD)")] = None,
    min_articles: Annotated[int, Query(ge=1, description="Minimum article count")] = 1,
    source: Annotated[str | None, Query(description="Filter by source")] = None,
    country: Annotated[str | None, Query(description="Filter by country code (US, GB, etc.)")] = None,
    limit: Annotated[int, Query(ge=1, le=200, description="Max results")] = 50,
    offset: Annotated[int, Query(ge=0, description="Pagination offset")] = 0,
):
    """List stories with optional filtering.

    Returns clustered stories for a given date with support for:
    - Filtering by minimum article count
    - Filtering by source (story must include articles from this source)
    - Filtering by country code
    - Pagination
    """
    query_date = dt or date.today()

    stories, total = service.list_stories(
        dt=query_date,
        min_articles=min_articles,
        source=source,
        country=country,
        limit=limit,
        offset=offset,
    )

    return StoryListResponse(
        stories=[StoryResponse(**s) for s in stories],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/{story_id}", response_model=StoryResponse)
async def get_story(
    story_id: str,
    service: Annotated[StoryService, Depends(get_story_service)],
    dt: Annotated[date | None, Query(alias="date", description="Date hint for faster lookup")] = None,
):
    """Get a single story by ID.

    Providing a date hint improves lookup performance by limiting the search
    to a single partition.
    """
    story = service.get_story(story_id=story_id, dt=dt)

    if story is None:
        raise HTTPException(status_code=404, detail="Story not found")

    return StoryResponse(**story)


@router.get("/{story_id}/articles", response_model=StoryArticlesResponse)
async def get_story_articles(
    story_id: str,
    service: Annotated[StoryService, Depends(get_story_service)],
    dt: Annotated[date | None, Query(alias="date", description="Date hint for faster lookup")] = None,
):
    """Get a story with its full article list.

    Returns the story with all its constituent articles, including
    article ID, headline, source, URL, and publication time.
    """
    story_articles = service.get_story_articles(story_id=story_id, dt=dt)

    if story_articles is None:
        raise HTTPException(status_code=404, detail="Story not found")

    return StoryArticlesResponse(**story_articles)
