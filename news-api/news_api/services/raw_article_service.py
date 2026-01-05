"""Raw article data access service."""

from datetime import date, datetime, timedelta

from news_api.config import APIConfig
from news_api.io.jsonl import load_raw_articles


class RawArticleService:
    """Service for accessing raw articles from JSONL files."""

    def __init__(self, config: APIConfig):
        self.config = config
        self._cache: dict[date, tuple[datetime, list[dict]]] = {}

    def _get_cached_articles(self, dt: date) -> list[dict] | None:
        """Get articles from cache if valid."""
        if not self.config.cache.enabled:
            return None

        if dt not in self._cache:
            return None

        cached_at, articles = self._cache[dt]
        ttl = timedelta(seconds=self.config.cache.ttl_seconds)

        if datetime.now() - cached_at > ttl:
            del self._cache[dt]
            return None

        return articles

    def _set_cached_articles(self, dt: date, articles: list[dict]) -> None:
        """Cache articles for a date."""
        if self.config.cache.enabled:
            self._cache[dt] = (datetime.now(), articles)

    def _load_articles(self, dt: date) -> list[dict]:
        """Load articles for a date (with caching)."""
        cached = self._get_cached_articles(dt)
        if cached is not None:
            return cached

        articles = load_raw_articles(self.config, dt)
        self._set_cached_articles(dt, articles)
        return articles

    def list_articles(
        self,
        dt: date,
        source: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[dict], int]:
        """List raw articles for a date with optional filtering.

        Args:
            dt: Date to query
            source: Filter by source (optional)
            limit: Maximum results to return
            offset: Pagination offset

        Returns:
            Tuple of (articles, total_count)
        """
        articles = self._load_articles(dt)

        # Apply source filter
        if source:
            articles = [a for a in articles if a.get("source") == source]

        total = len(articles)

        # Apply pagination
        articles = articles[offset : offset + limit]

        return articles, total

    def get_article(
        self,
        article_id: str,
        dt: date | None = None,
    ) -> dict | None:
        """Get a single raw article by ID.

        Args:
            article_id: Article ID to find
            dt: Date hint (improves performance)

        Returns:
            Article dict or None if not found
        """
        # If date provided, search that partition
        if dt:
            dates_to_search = [dt]
        else:
            # Search last 7 days
            today = date.today()
            dates_to_search = [today - timedelta(days=i) for i in range(7)]

        for search_date in dates_to_search:
            articles = self._load_articles(search_date)
            for article in articles:
                if article.get("article_id") == article_id:
                    return article

        return None
