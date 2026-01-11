"""Core extraction logic."""

import logging
from typing import Optional

import spacy

from news_pipeline.stage4_extract.models import Entity, ExtractedArticle
from news_pipeline.utils.datetime import parse_datetime

logger = logging.getLogger(__name__)

SPACY_MODELS = {
    "sm": "en_core_web_sm",
    "lg": "en_core_web_lg",
    "trf": "en_core_web_trf",
}

DEFAULT_MODEL = "trf"
DEFAULT_BATCH_SIZE = 16

ENTITY_TYPES = {"PERSON", "ORG", "GPE", "LOC"}

_nlp_cache: dict[str, spacy.Language] = {}


def _get_nlp(model_key: str) -> spacy.Language:
    """Get or load spaCy model."""
    if model_key not in SPACY_MODELS:
        raise ValueError(f"Invalid model: {model_key}. Must be one of {list(SPACY_MODELS.keys())}")
    model_name = SPACY_MODELS[model_key]
    if model_name not in _nlp_cache:
        logger.info(f"Loading spaCy model: {model_name}")
        _nlp_cache[model_name] = spacy.load(model_name)
    return _nlp_cache[model_name]


def _entities_from_doc(doc) -> list[Entity]:
    """Extract unique entities from a spaCy doc."""
    seen: set[tuple[str, str]] = set()
    entities: list[Entity] = []
    for ent in doc.ents:
        if ent.label_ in ENTITY_TYPES:
            key = (ent.text, ent.label_)
            if key not in seen:
                seen.add(key)
                entities.append(Entity(name=ent.text, type=ent.label_))
    return entities


def extract_entities_batch(
    texts: list[Optional[str]],
    model_key: str = DEFAULT_MODEL,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> list[list[Entity]]:
    """Extract entities from multiple texts using batch processing."""
    nlp = _get_nlp(model_key)

    valid_indices = []
    valid_texts = []
    for i, text in enumerate(texts):
        if text:
            valid_indices.append(i)
            valid_texts.append(text)

    results: list[list[Entity]] = [[] for _ in texts]

    if valid_texts:
        for i, doc in enumerate(nlp.pipe(valid_texts, batch_size=batch_size)):
            results[valid_indices[i]] = _entities_from_doc(doc)

    return results


def extract(
    embedded_articles: list[dict],
    model_key: str = DEFAULT_MODEL,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> list[ExtractedArticle]:
    """Apply NER over embedded article text."""
    if not embedded_articles:
        logger.warning("No articles to extract")
        return []

    logger.info(f"Extracting entities from {len(embedded_articles)} articles")

    processed_texts = [article.get("embedded_text") for article in embedded_articles]

    all_entities = extract_entities_batch(
        processed_texts,
        model_key=model_key,
        batch_size=batch_size,
    )

    results = []
    for i, article in enumerate(embedded_articles):
        results.append(ExtractedArticle(
            id=article["id"],
            source=article["source"],
            title=article.get("title", ""),
            summary=article.get("summary", ""),
            url=article["url"],
            published_at=parse_datetime(article.get("published_at")),
            ingested_at=parse_datetime(article.get("ingested_at")),
            text=article.get("text"),
            embedded_text=article.get("embedded_text"),
            embedding=article.get("embedding"),
            embedding_model=article.get("embedding_model", ""),
            entities=all_entities[i],
        ))

    logger.info(f"Extracted entities for {len(results)} articles")
    return results
