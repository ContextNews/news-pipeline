"""Data models for compute_embeddings pipeline stage."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class EmbeddedArticle:
    """Article with computed embedding vector."""
    id: str
    source: str
    title: str
    summary: str
    url: str
    published_at: datetime
    ingested_at: datetime
    text: Optional[str]
    embedded_text: str
    embedding: list[float]
    embedding_model: str
