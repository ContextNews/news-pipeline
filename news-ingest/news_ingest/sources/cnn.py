"""CNN RSS feed fetcher."""

from datetime import datetime
from typing import Iterable

from news_ingest.sources.rss import fetch_rss_articles

RSS_FEEDS = [
    "http://rss.cnn.com/rss/edition.rss",
    "http://rss.cnn.com/rss/edition_world.rss",
    "http://rss.cnn.com/rss/edition_us.rss",
    "http://rss.cnn.com/rss/money_news_international.rss",
    "http://rss.cnn.com/rss/edition_technology.rss",
]


def fetch_articles(since: datetime) -> Iterable[dict]:
    """Fetch articles from CNN RSS feeds published after `since`."""
    return fetch_rss_articles(RSS_FEEDS, since)
