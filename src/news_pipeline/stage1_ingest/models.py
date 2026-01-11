from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class IngestConfig:
    sources: list[str] = field(default_factory=list)
    lookback_hours: int = 24
    output: str = "s3"  # "s3" or "local"
    fetch_article_text: bool = True

@dataclass
class RSSArticle:
    source: str
    title: str
    summary: str
    url: str
    published_at: datetime

@dataclass
class RawArticle:
    id: str
    source: str
    title: str
    summary: str
    url: str
    published_at: datetime
    ingested_at: datetime
    text: Optional[str]
