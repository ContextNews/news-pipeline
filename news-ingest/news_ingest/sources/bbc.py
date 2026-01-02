"""BBC News RSS feed fetcher."""

from datetime import datetime
from typing import Iterable

from news_ingest.sources.rss import fetch_rss_articles

RSS_FEEDS = [
    "https://feeds.bbci.co.uk/news/rss.xml",
    "https://feeds.bbci.co.uk/news/world/rss.xml",
    "https://feeds.bbci.co.uk/news/business/rss.xml",
    "https://feeds.bbci.co.uk/news/technology/rss.xml",
]


def fetch_articles(since: datetime) -> Iterable[dict]:
    """Fetch articles from BBC RSS feeds published after `since`."""
    return fetch_rss_articles(RSS_FEEDS, since)
