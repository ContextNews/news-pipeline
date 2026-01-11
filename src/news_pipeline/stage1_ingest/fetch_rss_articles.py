"""RSS feed fetching."""

import logging
from datetime import datetime, timezone, timedelta
from typing import Iterable

import feedparser
import requests
from dateutil.parser import parse as parse_date

from news_pipeline.stage1_ingest.sources import RSS_FEEDS
from news_pipeline.stage1_ingest.models import RSSArticle

logger = logging.getLogger(__name__)

# Timezone abbreviations for date parsing
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


def fetch_rss_articles(source: str, since: datetime) -> Iterable[RSSArticle]:
    """Fetch articles from a source's RSS feed published after `since`."""
    if since.tzinfo is None:
        since = since.replace(tzinfo=timezone.utc)

    feed_url = RSS_FEEDS.get(source)
    if not feed_url:
        logger.warning(f"Unknown source: {source}")
        return

    yield from _fetch_feed(feed_url, source, since, set())


def _fetch_feed(
    feed_url: str, source: str, since: datetime, seen_urls: set
) -> Iterable[RSSArticle]:
    """Fetch and parse a single RSS feed."""
    response = requests.get(
        feed_url,
        timeout=30,
        headers={"User-Agent": "news-ingest/1.0 (RSS reader)"},
    )
    response.raise_for_status()

    feed = feedparser.parse(response.content)

    for entry in feed.entries:
        try:
            article = _parse_entry(entry, source, since, seen_urls)
            if article is not None:
                yield article
        except Exception as e:
            logger.warning(f"Failed to parse entry: {e}")
            continue


def _parse_entry(
    entry, source: str, since: datetime, seen_urls: set
) -> RSSArticle | None:
    """Parse a single RSS entry into an RSSArticle."""
    url = entry.get("link")
    if not url or url in seen_urls:
        return None

    published_at = _parse_published_date(entry)
    if published_at is None or published_at <= since:
        return None

    title = entry.get("title", "").strip()
    if not title:
        return None

    summary = entry.get("summary", "").strip()

    seen_urls.add(url)

    return RSSArticle(
        source=source,
        title=title,
        summary=summary,
        url=url,
        published_at=published_at,
    )


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
