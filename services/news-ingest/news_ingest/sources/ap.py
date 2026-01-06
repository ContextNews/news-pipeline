"""Associated Press RSS feed fetcher."""

from datetime import datetime
from typing import Iterable

from news_ingest.sources.rss import fetch_rss_articles

# AP News RSS feeds (via Google News)
RSS_FEEDS = [
    "https://news.google.com/rss/search?q=site:apnews.com&hl=en-US&gl=US&ceid=US:en",
]


def fetch_articles(since: datetime) -> Iterable[dict]:
    """Fetch articles from AP RSS feeds published after `since`."""
    return fetch_rss_articles(RSS_FEEDS, since)
