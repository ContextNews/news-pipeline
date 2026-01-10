"""Yahoo News RSS feed fetcher."""

from datetime import datetime
from typing import Iterable

from news_ingest.sources.rss import fetch_rss_articles

RSS_FEEDS = [
    "https://news.yahoo.com/rss/",
    "https://news.yahoo.com/rss/world",
    "https://news.yahoo.com/rss/us",
    "https://news.yahoo.com/rss/politics",
    "https://news.yahoo.com/rss/business",
    "https://news.yahoo.com/rss/tech",
]


def fetch_articles(since: datetime) -> Iterable[dict]:
    """Fetch articles from Yahoo News RSS feeds published after `since`."""
    return fetch_rss_articles(RSS_FEEDS, since)
