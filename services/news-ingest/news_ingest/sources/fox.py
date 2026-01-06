"""Fox News RSS feed fetcher."""

from datetime import datetime
from typing import Iterable

from news_ingest.sources.rss import fetch_rss_articles

RSS_FEEDS = [
    "https://moxie.foxnews.com/google-publisher/latest.xml",
    "https://moxie.foxnews.com/google-publisher/world.xml",
    "https://moxie.foxnews.com/google-publisher/politics.xml",
    "https://moxie.foxnews.com/google-publisher/science.xml",
    "https://moxie.foxnews.com/google-publisher/tech.xml",
]


def fetch_articles(since: datetime) -> Iterable[dict]:
    """Fetch articles from Fox News RSS feeds published after `since`."""
    return fetch_rss_articles(RSS_FEEDS, since)
