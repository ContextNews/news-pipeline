"""Data models for extract_entities pipeline stage."""

from dataclasses import dataclass


@dataclass
class ArticleEntity:
    """Named entity extracted from an article."""
    article_id: str
    entity_type: str
    entity_name: str
    in_title: bool
    count: int
    aliases: list[str] | None = None
