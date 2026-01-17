from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

@dataclass
class RSSArticle:
    source: str
    title: str
    summary: str
    url: str
    published_at: datetime


@dataclass
class ResolvedArticle:
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
    id: str
    source: str
    title: str
    summary: str
    url: str
    published_at: datetime
    ingested_at: datetime
    text: Optional[str]
