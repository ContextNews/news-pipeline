"""Politico RSS feed fetcher."""

from datetime import datetime
from typing import Iterable

from news_ingest.sources.rss import fetch_rss_articles

RSS_FEEDS = [
    "https://www.politico.eu/feed/",
    "https://rss.politico.com/politics-news.xml",
    "https://rss.politico.com/congress.xml",
]


def fetch_articles(since: datetime) -> Iterable[dict]:
    """Fetch articles from Politico RSS feeds published after `since`."""
    return fetch_rss_articles(RSS_FEEDS, since)
