from dataclasses import dataclass
from datetime import datetime
from typing import Optional


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
