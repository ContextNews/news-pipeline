"""Washington Post RSS feed fetcher."""

from datetime import datetime
from typing import Iterable

from news_ingest.sources.rss import fetch_rss_articles

RSS_FEEDS = [
    "https://feeds.washingtonpost.com/rss/world",
    "https://feeds.washingtonpost.com/rss/national",
    "https://feeds.washingtonpost.com/rss/politics",
    "https://feeds.washingtonpost.com/rss/business",
]


def fetch_articles(since: datetime) -> Iterable[dict]:
    """Fetch articles from Washington Post RSS feeds published after `since`."""
    return fetch_rss_articles(RSS_FEEDS, since)
