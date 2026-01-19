from dataclasses import dataclass


@dataclass
class ArticleEntity:
    article_id: str
    entity_type: str
    entity_name: str
    in_title: bool
    count: int
    aliases: list[str] | None = None
