"""News normalization pipeline."""

import logging
from datetime import datetime, timezone

from news_pipeline.normalize.clean_text import clean_text
from news_pipeline.normalize.extract_entities import extract_entities_batch
from news_pipeline.normalize.get_locations import get_locations
from news_pipeline.normalize.compute_embedding import compute_embeddings_batch, prepare_embedding_text
from news_pipeline.normalize.models import NormalizedArticle

logger = logging.getLogger(__name__)


def normalize(
    raw_articles: list[dict],
    spacy_model: str = "trf",
    embedding_enabled: bool = False,
    embedding_model: str = "minilm",
    embedding_batch_size: int = 32,
) -> list[NormalizedArticle]:
    """
    Normalize raw articles: clean text, extract entities, get locations, compute embeddings.

    Args:
        raw_articles: List of raw article dicts from JSONL input
        spacy_model: spaCy model key (sm, lg, trf)
        embedding_enabled: Whether to compute embeddings
        embedding_model: Embedding model key (minilm, mpnet)
        embedding_batch_size: Batch size for embedding computation

    Returns:
        List of NormalizedArticle objects
    """
    if not raw_articles:
        logger.warning("No articles to process")
        return []

    logger.info(f"Normalizing {len(raw_articles)} articles")
    now = datetime.now(timezone.utc)

    # Clean article text and combine with title/summary for NER
    cleaned_texts = []
    ner_texts = []
    for raw in raw_articles:
        article_text = raw.get("article_text", {})
        text = article_text.get("text") if isinstance(article_text, dict) else None
        cleaned = clean_text(text)
        cleaned_texts.append(cleaned)

        # Combine title + summary for NER (not full article)
        parts = []
        if raw.get("title"):
            parts.append(raw["title"])
        if raw.get("summary"):
            parts.append(raw["summary"])
        ner_texts.append("\n\n".join(parts) if parts else None)

    # Batch NER on combined text
    logger.info(f"Running NER on {len(raw_articles)} articles (model: {spacy_model})")
    all_entities = extract_entities_batch(ner_texts, model_key=spacy_model)

    # Batch embeddings
    all_embeddings: list = [None] * len(raw_articles)
    embedding_texts: list = [None] * len(raw_articles)
    if embedding_enabled:
        logger.info(f"Preparing embedding texts for {len(raw_articles)} articles")
        embedding_texts = [
            prepare_embedding_text(
                title=raw.get("title"),
                summary=raw.get("summary"),
                article_text=cleaned_texts[i],
                model_key=embedding_model,
            )
            for i, raw in enumerate(raw_articles)
        ]
        logger.info(f"Computing embeddings for {len(raw_articles)} articles (model: {embedding_model})")
        all_embeddings = compute_embeddings_batch(
            embedding_texts,
            model_key=embedding_model,
            batch_size=embedding_batch_size,
        )

    # Build normalized articles
    results = []
    for i, raw in enumerate(raw_articles):
        entities = all_entities[i]
        locations = get_locations(entities, raw.get("title", ""))

        article_text = raw.get("article_text", {})
        text = article_text.get("text") if isinstance(article_text, dict) else None

        results.append(NormalizedArticle(
            id=raw["id"],
            source=raw["source"],
            title=raw.get("title", ""),
            summary=raw.get("summary", ""),
            url=raw["url"],
            published_at=_parse_datetime(raw.get("published_at")),
            fetched_at=_parse_datetime(raw.get("fetched_at")),
            article_text=text,
            content_clean=cleaned_texts[i],
            ner_model=spacy_model,
            entities=entities,
            locations=locations,
            normalized_at=now,
            embedding_text=embedding_texts[i],
            embedding=all_embeddings[i],
            embedding_model=embedding_model if all_embeddings[i] else None,
        ))

    logger.info(f"Normalized {len(results)} articles")
    return results


def _parse_datetime(value) -> datetime:
    """Parse datetime from ISO string or return as-is if already datetime."""
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
