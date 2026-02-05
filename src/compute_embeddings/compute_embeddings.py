"""Core embedding computation logic."""

import logging
import re
from typing import Any

from sentence_transformers import SentenceTransformer

from compute_embeddings.models import EmbeddedArticle
from common.utils import get_value

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
        title = get_value(article, "title")
        if title:
            parts.append(title)

    if embed_summary:
        summary = get_value(article, "summary")
        if summary:
            parts.append(summary)

    if embed_text:
        text = get_value(article, "text")
        if text:
            parts.append(text)

    combined = " ".join(parts)

    if word_limit:
        sentences = _split_sentences(combined)
        selected = []
        word_count = 0
        for sentence in sentences:
            words = sentence.split()
            if not words:
                continue
            if word_count + len(words) > word_limit:
                break
            selected.append(sentence)
            word_count += len(words)
        combined = " ".join(selected)

    return combined


def _split_sentences(text: str) -> list[str]:
    """Split text into sentences with a simple punctuation heuristic."""
    if not text:
        return []
    matches = re.findall(r"[^.!?]+[.!?]+|[^.!?]+$", text)
    return [m.strip() for m in matches if m.strip()]


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

    # Filter out articles missing required fields
    valid_articles = []
    for article in articles:
        article_id = get_value(article, "id")
        url = get_value(article, "url")
        if not article_id or not url:
            logger.warning("Skipping article with missing id or url: id=%s, url=%s", article_id, url)
            continue
        valid_articles.append(article)

    if not valid_articles:
        logger.warning("No valid articles to embed after filtering")
        return []

    logger.info("Loading model: %s", model)
    encoder = SentenceTransformer(model)

    # Build texts to embed
    texts_to_embed = []
    for article in valid_articles:
        text = _build_text_to_embed(
            article, embed_title, embed_summary, embed_text, word_limit
        )
        if not text:
            logger.warning("Empty text to embed for article: id=%s", get_value(article, "id"))
        texts_to_embed.append(text)

    # Compute embeddings in batches
    logger.info("Computing embeddings for %d articles (batch_size=%d)", len(valid_articles), batch_size)
    embeddings = encoder.encode(
        texts_to_embed,
        batch_size=batch_size,
        show_progress_bar=True,
        convert_to_numpy=True,
    )

    # Build result objects
    results = []
    for article, embedded_text, embedding in zip(valid_articles, texts_to_embed, embeddings):
        results.append(
            EmbeddedArticle(
                id=get_value(article, "id"),
                source=get_value(article, "source"),
                title=get_value(article, "title") or "",
                summary=get_value(article, "summary") or "",
                url=get_value(article, "url"),
                published_at=get_value(article, "published_at"),
                ingested_at=get_value(article, "ingested_at"),
                text=get_value(article, "text"),
                embedded_text=embedded_text,
                embedding=embedding.tolist(),
                embedding_model=model,
            )
        )

    logger.info("Computed embeddings for %d articles", len(results))
    return results
