"""Story data access service."""

from datetime import date, datetime, timedelta

from news_api.config import APIConfig
from news_api.io.parquet import load_stories, load_story_articles


class StoryService:
    """Service for accessing clustered stories."""

    def __init__(self, config: APIConfig):
        self.config = config
        self._stories_cache: dict[date, tuple[datetime, list[dict]]] = {}
        self._story_articles_cache: dict[date, tuple[datetime, list[dict]]] = {}

    def _get_cached(
        self, cache: dict[date, tuple[datetime, list[dict]]], dt: date
    ) -> list[dict] | None:
        """Get data from cache if valid."""
        if not self.config.cache.enabled:
            return None

        if dt not in cache:
            return None

        cached_at, data = cache[dt]
        ttl = timedelta(seconds=self.config.cache.ttl_seconds)

        if datetime.now() - cached_at > ttl:
            del cache[dt]
            return None

        return data

    def _set_cached(
        self, cache: dict[date, tuple[datetime, list[dict]]], dt: date, data: list[dict]
    ) -> None:
        """Cache data for a date."""
        if self.config.cache.enabled:
            cache[dt] = (datetime.now(), data)

    def _load_stories(self, dt: date) -> list[dict]:
        """Load stories for a date (with caching)."""
        cached = self._get_cached(self._stories_cache, dt)
        if cached is not None:
            return cached

        stories = load_stories(self.config, dt)
        self._set_cached(self._stories_cache, dt, stories)
        return stories

    def _load_story_articles(self, dt: date) -> list[dict]:
        """Load story_articles for a date (with caching)."""
        cached = self._get_cached(self._story_articles_cache, dt)
        if cached is not None:
            return cached

        story_articles = load_story_articles(self.config, dt)
        self._set_cached(self._story_articles_cache, dt, story_articles)
        return story_articles

    def list_stories(
        self,
        dt: date,
        min_articles: int = 1,
        source: str | None = None,
        country: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[dict], int]:
        """List stories for a date with optional filtering.

        Args:
            dt: Date to query
            min_articles: Minimum article count
            source: Filter by source (story must include this source)
            country: Filter by country code (story must have location in this country)
            limit: Maximum results to return
            offset: Pagination offset

        Returns:
            Tuple of (stories, total_count)
        """
        stories = self._load_stories(dt)

        # Apply min_articles filter
        if min_articles > 1:
            stories = [s for s in stories if s.get("article_count", 0) >= min_articles]

        # Apply source filter
        if source:
            stories = [s for s in stories if source in s.get("sources", [])]

        # Apply country filter
        if country:
            stories = [
                s for s in stories
                if any(
                    loc.get("country_code") == country
                    for loc in s.get("locations", [])
                )
            ]

        total = len(stories)

        # Apply pagination
        stories = stories[offset : offset + limit]

        return stories, total

    def get_story(self, story_id: str, dt: date | None = None) -> dict | None:
        """Get a single story by ID.

        Args:
            story_id: Story ID to find
            dt: Date hint (improves performance)

        Returns:
            Story dict or None if not found
        """
        # If date provided, search that partition
        if dt:
            dates_to_search = [dt]
        else:
            # Search last 7 days
            today = date.today()
            dates_to_search = [today - timedelta(days=i) for i in range(7)]

        for search_date in dates_to_search:
            stories = self._load_stories(search_date)
            for story in stories:
                if story.get("story_id") == story_id:
                    return story

        return None

    def get_story_articles(self, story_id: str, dt: date | None = None) -> dict | None:
        """Get a story with its full article list.

        Args:
            story_id: Story ID to find
            dt: Date hint (improves performance)

        Returns:
            StoryArticles dict or None if not found
        """
        # If date provided, search that partition
        if dt:
            dates_to_search = [dt]
        else:
            # Search last 7 days
            today = date.today()
            dates_to_search = [today - timedelta(days=i) for i in range(7)]

        for search_date in dates_to_search:
            story_articles_list = self._load_story_articles(search_date)
            for sa in story_articles_list:
                if sa.get("story_id") == story_id:
                    return sa

        return None
