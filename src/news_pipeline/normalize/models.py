from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class NormalizeConfig:
    output: str = "s3"  # "s3" or "local"
    period: str = ""  # For S3: date to process (defaults to today)
    spacy_model: str = "trf"
    embedding_model: str = "minilm"
    embedding_batch_size: int = 32
    max_article_words: int = 250


@dataclass
class Entity:
    name: str
    type: str


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
    article_text_clean: Optional[str]
    article_text_processed: Optional[str]
    ner_model: str
    entities: list[Entity] = field(default_factory=list)
    normalized_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Embeddings
    embedding: Optional[list[float]] = None
    embedding_model: Optional[str] = None
