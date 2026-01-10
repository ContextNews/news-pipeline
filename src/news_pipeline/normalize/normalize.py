"""News normalization pipeline."""

import logging
from datetime import datetime, timezone

from news_pipeline.normalize.clean_text import clean_text
from news_pipeline.normalize.extract_entities import extract_entities_batch
from news_pipeline.normalize.compute_embedding import compute_embeddings_batch, prepare_text
from news_pipeline.normalize.models import NormalizedArticle

logger = logging.getLogger(__name__)


def normalize(
    raw_articles: list[dict],
    spacy_model: str = "trf",
    embedding_model: str = "minilm",
    embedding_batch_size: int = 32,
    max_article_words: int = 250,
) -> list[NormalizedArticle]:
    """
    Normalize raw articles: clean text, extract entities, compute embeddings.

    Args:
        raw_articles: List of raw article dicts from JSONL input
        spacy_model: spaCy model key (sm, lg, trf)
        embedding_model: Embedding model key (minilm, mpnet)
        embedding_batch_size: Batch size for embedding computation
        max_article_words: Maximum words to include from article text

    Returns:
        List of NormalizedArticle objects
    """
    if not raw_articles:
        logger.warning("No articles to process")
        return []

    logger.info(f"Normalizing {len(raw_articles)} articles")
    now = datetime.now(timezone.utc)

    # Clean article text
    cleaned_texts = []
    for raw in raw_articles:
        text = raw.get("text")
        cleaned_texts.append(clean_text(text))

    # Prepare processed text (title + summary + article truncated to max words)
    # Used for both embeddings and NER
    logger.info(f"Preparing texts for {len(raw_articles)} articles (max_article_words={max_article_words})")
    processed_texts = [
        prepare_text(
            title=raw.get("title"),
            summary=raw.get("summary"),
            article_text=cleaned_texts[i],
            max_article_words=max_article_words,
        )
        for i, raw in enumerate(raw_articles)
    ]

    # Batch embeddings
    logger.info(f"Computing embeddings for {len(raw_articles)} articles (model: {embedding_model})")
    all_embeddings = compute_embeddings_batch(
        processed_texts,
        model_key=embedding_model,
        batch_size=embedding_batch_size,
    )

    # Batch NER on same processed text
    logger.info(f"Running NER on {len(raw_articles)} articles (model: {spacy_model})")
    all_entities = extract_entities_batch(processed_texts, model_key=spacy_model)

    # Build normalized articles
    results = []
    for i, raw in enumerate(raw_articles):
        results.append(NormalizedArticle(
            id=raw["id"],
            source=raw["source"],
            title=raw.get("title", ""),
            summary=raw.get("summary", ""),
            url=raw["url"],
            published_at=_parse_datetime(raw.get("published_at")),
            ingested_at=_parse_datetime(raw.get("ingested_at")),
            article_text=raw.get("text"),
            article_text_clean=cleaned_texts[i],
            article_text_processed=processed_texts[i],
            ner_model=spacy_model,
            entities=all_entities[i],
            normalized_at=now,
            embedding=all_embeddings[i],
            embedding_model=embedding_model,
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
