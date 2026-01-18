"""Core embedding computation logic."""

import logging
from typing import Any

from sentence_transformers import SentenceTransformer

from compute_embeddings.models import EmbeddedArticle

logger = logging.getLogger(__name__)


def _build_text_to_embed(
    article: Any,
    embed_title: bool,
    embed_summary: bool,
    embed_text: bool,
    word_limit: int | None,
) -> str:
    """Build the text string to embed from article fields."""
    parts = []

    if embed_title:
        title = _get_value(article, "title")
        if title:
            parts.append(title)

    if embed_summary:
        summary = _get_value(article, "summary")
        if summary:
            parts.append(summary)

    if embed_text:
        text = _get_value(article, "text")
        if text:
            parts.append(text)

    combined = " ".join(parts)

    if word_limit:
        words = combined.split()
        if len(words) > word_limit:
            combined = " ".join(words[:word_limit])

    return combined


def _get_value(obj: Any, key: str) -> Any:
    """Get value from dict or object attribute."""
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def compute_embeddings(
    articles: list[Any],
    model: str,
    batch_size: int = 32,
    embed_title: bool = True,
    embed_summary: bool = True,
    embed_text: bool = True,
    word_limit: int | None = None,
) -> list[EmbeddedArticle]:
    """
    Compute embeddings for a list of articles.

    Args:
        articles: List of article objects or dicts with title, summary, text fields
        model: Name of the sentence-transformers model to use
        batch_size: Batch size for encoding
        embed_title: Include title in text to embed
        embed_summary: Include summary in text to embed
        embed_text: Include text in text to embed
        word_limit: Maximum number of words to embed (None for no limit)

    Returns:
        List of EmbeddedArticle objects with embeddings
    """
    if not articles:
        logger.warning("No articles to embed")
        return []

    logger.info("Loading model: %s", model)
    encoder = SentenceTransformer(model)

    # Build texts to embed
    texts_to_embed = []
    for article in articles:
        text = _build_text_to_embed(
            article, embed_title, embed_summary, embed_text, word_limit
        )
        texts_to_embed.append(text)

    # Compute embeddings in batches
    logger.info("Computing embeddings for %d articles (batch_size=%d)", len(articles), batch_size)
    embeddings = encoder.encode(
        texts_to_embed,
        batch_size=batch_size,
        show_progress_bar=True,
        convert_to_numpy=True,
    )

    # Build result objects
    results = []
    for article, embedded_text, embedding in zip(articles, texts_to_embed, embeddings):
        results.append(
            EmbeddedArticle(
                id=_get_value(article, "id"),
                source=_get_value(article, "source"),
                title=_get_value(article, "title") or "",
                summary=_get_value(article, "summary") or "",
                url=_get_value(article, "url"),
                published_at=_get_value(article, "published_at"),
                ingested_at=_get_value(article, "ingested_at"),
                text=_get_value(article, "text"),
                embedded_text=embedded_text,
                embedding=embedding.tolist(),
                embedding_model=model,
            )
        )

    logger.info("Computed embeddings for %d articles", len(results))
    return results
