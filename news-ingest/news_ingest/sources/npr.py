"""NPR RSS feed fetcher."""

from datetime import datetime
from typing import Iterable

from news_ingest.sources.rss import fetch_rss_articles

RSS_FEEDS = [
    "https://feeds.npr.org/1001/rss.xml",  # News
    "https://feeds.npr.org/1003/rss.xml",  # National
    "https://feeds.npr.org/1004/rss.xml",  # World
    "https://feeds.npr.org/1006/rss.xml",  # Business
    "https://feeds.npr.org/1019/rss.xml",  # Technology
]


def fetch_articles(since: datetime) -> Iterable[dict]:
    """Fetch articles from NPR RSS feeds published after `since`."""
    return fetch_rss_articles(RSS_FEEDS, since)
