"""Financial Times RSS feed fetcher."""

from datetime import datetime
from typing import Iterable

from news_ingest.sources.rss import fetch_rss_articles

RSS_FEEDS = [
    "https://www.ft.com/rss/home",
    "https://www.ft.com/world?format=rss",
    "https://www.ft.com/companies?format=rss",
    "https://www.ft.com/technology?format=rss",
    "https://www.ft.com/markets?format=rss",
]


def fetch_articles(since: datetime) -> Iterable[dict]:
    """Fetch articles from Financial Times RSS feeds published after `since`."""
    return fetch_rss_articles(RSS_FEEDS, since)
