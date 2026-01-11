from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class Entity:
    name: str
    type: str


@dataclass
class ExtractedArticle:
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
    entities: list[Entity] = field(default_factory=list)
