"""Data models for classify_articles pipeline stage."""

from dataclasses import dataclass


@dataclass
class ClassifiedArticle:
    """Article with classified topic labels."""
    article_id: str
    topics: list[str]
    scores: dict[str, float]
