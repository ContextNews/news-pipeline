"""Data models for cluster_articles pipeline stage."""

from dataclasses import dataclass
from datetime import datetime


@dataclass
class ClusteredArticle:
    """Article with assigned cluster label."""

    id: str
    source: str
    title: str
    summary: str | None
    url: str
    published_at: datetime
    ingested_at: datetime
    text: str | None
    cluster_id: int
    embedding_model: str
