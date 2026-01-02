"""The Guardian RSS feed fetcher."""

from datetime import datetime
from typing import Iterable

from news_ingest.sources.rss import fetch_rss_articles

RSS_FEEDS = [
    "https://www.theguardian.com/world/rss",
    "https://www.theguardian.com/uk-news/rss",
    "https://www.theguardian.com/us-news/rss",
    "https://www.theguardian.com/business/rss",
    "https://www.theguardian.com/technology/rss",
    "https://www.theguardian.com/politics/rss",
]


def fetch_articles(since: datetime) -> Iterable[dict]:
    """Fetch articles from The Guardian RSS feeds published after `since`."""
    return fetch_rss_articles(RSS_FEEDS, since)
