"""Data models for ingest_articles pipeline stage."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class RSSArticle:
    """Raw article parsed from an RSS feed entry."""
    source: str
    title: str
    summary: str
    url: str
    published_at: datetime


@dataclass
class ResolvedArticle:
    """Article with full text fetched and article ID generated."""
    id: str
    source: str
    title: str
    summary: str
    url: str
    published_at: datetime
    ingested_at: datetime
    text: Optional[str]


@dataclass
class CleanedArticle:
    """Article with text cleaned and normalized (HTML stripped, whitespace collapsed)."""
    id: str
    source: str
    title: str
    summary: str
    url: str
    published_at: datetime
    ingested_at: datetime
    text: Optional[str]
