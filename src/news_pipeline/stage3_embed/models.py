from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class EmbeddedArticle:
    id: str
    source: str
    title: str
    summary: str
    url: str
    published_at: datetime
    ingested_at: datetime
    text: Optional[str]
    embedded_text: Optional[str]
    embedding: Optional[list[float]]
    embedding_model: str
