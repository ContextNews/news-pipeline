"""News normalization pipeline."""

import logging
from datetime import datetime, timezone
from typing import Optional

from news_normalize.utils.config_loader import NormalizeConfig
from news_normalize.geo.locations import extract_locations
from news_normalize.nlp.ner import DEFAULT_SPACY_MODEL, SPACY_MODELS, extract_entities_batch
from news_normalize.schema import Entity, NormalizedArticle, PreparedArticle
from news_normalize.nlp.clean_text import clean_text

logger = logging.getLogger(__name__)


def run(
    raw_articles: list[dict],
    config: Optional[NormalizeConfig] = None,
) -> list[NormalizedArticle]:
    """
    Run the normalization pipeline on raw article data.

    Args:
        raw_articles: List of raw article dicts from JSONL input
        config: Optional NormalizeConfig. If None, uses defaults (sm model, no embeddings).

    Returns:
        List of normalized articles
    """
    if not raw_articles:
        logger.warning("No articles to process")
        return []

    # Use config values or defaults
    spacy_model = config.spacy_model if config else DEFAULT_SPACY_MODEL
    embedding_enabled = config.embedding_enabled if config else False

    logger.info(f"Normalizing {len(raw_articles)} articles")

    # Resolve model key to full model name
    ner_model = SPACY_MODELS[spacy_model]

    # First pass: prepare all articles
    prepared = [prepare_article(raw) for raw in raw_articles]

    # Batch NER on all cleaned content
    logger.info(f"Running batch NER on {len(prepared)} articles")
    texts = [p.content_clean for p in prepared]
    all_entities = extract_entities_batch(texts, model_key=spacy_model)

    # Batch embeddings (if enabled in config)
    if embedding_enabled and config:
        all_embeddings = compute_embeddings_for_articles(prepared, config)
    else:
        all_embeddings = [None] * len(prepared)

    # Second pass: finalize articles with entities and embeddings
    return [
        make_failed_article(prep, ner_model) if prep.error
        else finalize_article(prep, all_entities[i], ner_model, all_embeddings[i])
        for i, prep in enumerate(prepared)
    ]


def prepare_article(raw: dict) -> PreparedArticle:
    """
    Prepare article for batch NER.

    Preserves all raw fields from input and adds content_clean.
    Returns PreparedArticle with error field set if preparation fails.
    """
    try:
        return PreparedArticle(
            article_id=raw["article_id"],
            source=raw["source"],
            url=raw["url"],
            published_at=datetime.fromisoformat(raw["published_at"].replace("Z", "+00:00")),
            fetched_at=datetime.fromisoformat(raw["fetched_at"].replace("Z", "+00:00")),
            headline=raw["headline"],
            body=raw.get("body", ""),
            content=raw.get("content"),
            resolution=raw.get("resolution", {}),
            content_clean=clean_text(raw.get("content")),
        )
    except Exception as e:
        logger.error(f"Failed to prepare article {raw.get('article_id', 'unknown')}: {e}")
        return PreparedArticle(
            article_id=raw.get("article_id", "unknown"),
            source=raw.get("source", "unknown"),
            url=raw.get("url", ""),
            published_at=datetime.now(timezone.utc),
            fetched_at=datetime.now(timezone.utc),
            headline=raw.get("headline", ""),
            body=raw.get("body", ""),
            content=raw.get("content"),
            resolution=raw.get("resolution", {}),
            content_clean=None,
            error=str(e),
        )


def compute_embeddings_for_articles(
    prepared: list[PreparedArticle],
    config: NormalizeConfig,
) -> list:
    """
    Compute embeddings for all prepared articles.

    Returns list of ArticleEmbeddings (or None if embeddings disabled).
    """
    if not config.embedding_enabled:
        return [None] * len(prepared)

    # Lazy import to avoid loading torch when embeddings are disabled
    from news_normalize.nlp.embeddings import (
        ArticleEmbeddings,
        compute_embeddings_batch,
    )

    logger.info(f"Computing embeddings for {len(prepared)} articles (model: {config.embedding_model})")

    headlines = [p.headline for p in prepared]
    contents = [p.content_clean for p in prepared]

    return compute_embeddings_batch(
        headlines=headlines,
        contents=contents,
        model_key=config.embedding_model,
        weight_headline=config.embedding_weight_headline,
        weight_content=config.embedding_weight_content,
        batch_size=config.embedding_batch_size,
    )


def make_failed_article(prepared: PreparedArticle, ner_model: str) -> NormalizedArticle:
    """Create an article record when pipeline processing fails."""
    return NormalizedArticle(
        article_id=prepared.article_id,
        source=prepared.source,
        url=prepared.url,
        published_at=prepared.published_at,
        fetched_at=prepared.fetched_at,
        headline=prepared.headline,
        body=prepared.body,
        content=prepared.content,
        resolution={"success": False, "error": prepared.error},
        content_clean=None,
        ner_model=ner_model,
        entities=[],
        locations=[],
        normalized_at=datetime.now(timezone.utc),
    )


def finalize_article(
    prepared: PreparedArticle,
    entities: list[Entity],
    ner_model: str,
    embeddings: Optional["ArticleEmbeddings"] = None,
) -> NormalizedArticle:
    """Finalize article with pre-computed entities and optional embeddings."""
    locations = extract_locations(entities, prepared.headline) if prepared.content_clean else []

    # Extract embedding fields if provided
    emb_headline = embeddings.headline if embeddings else None
    emb_content = embeddings.content if embeddings else None
    emb_combined = embeddings.combined if embeddings else None
    emb_model = embeddings.model if embeddings else None
    emb_dim = embeddings.dim if embeddings else None
    emb_chunks = embeddings.chunks if embeddings else None

    return NormalizedArticle(
        # Raw fields (preserved)
        article_id=prepared.article_id,
        source=prepared.source,
        url=prepared.url,
        published_at=prepared.published_at,
        fetched_at=prepared.fetched_at,
        headline=prepared.headline,
        body=prepared.body,
        content=prepared.content,
        resolution=prepared.resolution,
        # Added by normalization
        content_clean=prepared.content_clean,
        ner_model=ner_model,
        entities=entities,
        locations=locations,
        normalized_at=datetime.now(timezone.utc),
        # Embedding fields
        embedding_headline=emb_headline,
        embedding_content=emb_content,
        embedding_combined=emb_combined,
        embedding_model=emb_model,
        embedding_dim=emb_dim,
        embedding_chunks=emb_chunks,
    )
