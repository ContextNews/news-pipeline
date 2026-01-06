"""Shared RSS feed fetching logic."""

import logging
from datetime import datetime, timezone, timedelta
from typing import Iterable

import feedparser
import requests
from dateutil.parser import parse as parse_date

# Common timezone abbreviations
TZINFOS = {
    "EST": timezone(timedelta(hours=-5)),
    "EDT": timezone(timedelta(hours=-4)),
    "CST": timezone(timedelta(hours=-6)),
    "CDT": timezone(timedelta(hours=-5)),
    "MST": timezone(timedelta(hours=-7)),
    "MDT": timezone(timedelta(hours=-6)),
    "PST": timezone(timedelta(hours=-8)),
    "PDT": timezone(timedelta(hours=-7)),
    "GMT": timezone.utc,
    "UTC": timezone.utc,
    "BST": timezone(timedelta(hours=1)),
}

logger = logging.getLogger(__name__)


def fetch_rss_articles(feed_urls: list[str], since: datetime) -> Iterable[dict]:
    """Fetch articles from RSS feeds published after `since`.

    Args:
        feed_urls: List of RSS feed URLs to fetch
        since: Only return articles published after this timestamp

    Yields:
        Article dictionaries with headline, body, url, published_at
    """
    if since.tzinfo is None:
        since = since.replace(tzinfo=timezone.utc)

    seen_urls = set()

    for feed_url in feed_urls:
        try:
            yield from _fetch_feed(feed_url, since, seen_urls)
        except Exception as e:
            logger.error(f"Failed to fetch feed {feed_url}: {e}")
            continue


def _fetch_feed(feed_url: str, since: datetime, seen_urls: set) -> Iterable[dict]:
    """Fetch and parse a single RSS feed."""
    response = requests.get(feed_url, timeout=30, headers={
        "User-Agent": "news-ingest/1.0 (RSS reader)"
    })
    response.raise_for_status()

    feed = feedparser.parse(response.content)

    for entry in feed.entries:
        try:
            article = _parse_entry(entry, since, seen_urls)
            if article is not None:
                yield article
        except Exception as e:
            logger.warning(f"Failed to parse entry: {e}")
            continue


def _parse_entry(entry, since: datetime, seen_urls: set) -> dict | None:
    """Parse a single RSS entry into an article dict."""
    url = entry.get("link")
    if not url or url in seen_urls:
        return None

    published_at = _parse_published_date(entry)
    if published_at is None or published_at <= since:
        return None

    headline = entry.get("title", "").strip()
    if not headline:
        return None

    body = entry.get("summary", "").strip()

    seen_urls.add(url)

    return {
        "headline": headline,
        "body": body,
        "url": url,
        "published_at": published_at.isoformat(),
    }


def _parse_published_date(entry) -> datetime | None:
    """Extract and parse the published date from an RSS entry."""
    published = entry.get("published") or entry.get("updated")
    if not published:
        return None

    try:
        dt = parse_date(published, tzinfos=TZINFOS)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None
