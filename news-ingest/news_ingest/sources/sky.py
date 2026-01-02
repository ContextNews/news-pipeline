"""Sky News RSS feed fetcher."""

from datetime import datetime
from typing import Iterable

from news_ingest.sources.rss import fetch_rss_articles

RSS_FEEDS = [
    "https://feeds.skynews.com/feeds/rss/home.xml",
    "https://feeds.skynews.com/feeds/rss/world.xml",
    "https://feeds.skynews.com/feeds/rss/uk.xml",
    "https://feeds.skynews.com/feeds/rss/business.xml",
    "https://feeds.skynews.com/feeds/rss/technology.xml",
]


def fetch_articles(since: datetime) -> Iterable[dict]:
    """Fetch articles from Sky News RSS feeds published after `since`."""
    return fetch_rss_articles(RSS_FEEDS, since)
