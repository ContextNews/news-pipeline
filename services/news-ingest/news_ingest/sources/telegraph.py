"""The Telegraph RSS feed fetcher."""

from datetime import datetime
from typing import Iterable

from news_ingest.sources.rss import fetch_rss_articles

RSS_FEEDS = [
    "https://www.telegraph.co.uk/rss.xml",
    "https://www.telegraph.co.uk/news/rss.xml",
    "https://www.telegraph.co.uk/politics/rss.xml",
    "https://www.telegraph.co.uk/business/rss.xml",
    "https://www.telegraph.co.uk/technology/rss.xml",
]


def fetch_articles(since: datetime) -> Iterable[dict]:
    """Fetch articles from The Telegraph RSS feeds published after `since`."""
    return fetch_rss_articles(RSS_FEEDS, since)
