from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class NormalizeConfig:
    output: str = "s3"  # "s3" or "local"
    period: str = ""  # For S3: date to process (defaults to today)
    spacy_model: str = "trf"
    embedding_enabled: bool = False
    embedding_model: str = "minilm"
    embedding_batch_size: int = 32


@dataclass
class Entity:
    text: str
    type: str
    count: int


@dataclass
class SubEntity:
    name: str
    count: int
    in_headline: bool


@dataclass
class Location:
    name: str
    country_code: str
    count: int
    in_headline: bool
    confidence: float
    sub_entities: list[SubEntity] = field(default_factory=list)


@dataclass
class NormalizedArticle:
    # From input (preserved)
    id: str
    source: str
    title: str
    summary: str
    url: str
    published_at: datetime
    fetched_at: datetime
    article_text: Optional[str]

    # Added by normalization
    content_clean: Optional[str]
    ner_model: str
    entities: list[Entity] = field(default_factory=list)
    locations: list[Location] = field(default_factory=list)
    normalized_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Embeddings (optional)
    embedding_text: Optional[str] = None
    embedding: Optional[list[float]] = None
    embedding_model: Optional[str] = None
