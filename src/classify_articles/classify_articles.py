"""Core article classification logic."""

import logging
from typing import Any

from transformers import pipeline as hf_pipeline

from classify_articles.models import ClassifiedArticle
from common.utils import get_value

logger = logging.getLogger(__name__)


def _build_input_text(article: Any, word_limit: int | None = None) -> str:
    """Build input text from article title, summary, and text fields."""
    parts = []

    title = get_value(article, "title")
    if title:
        parts.append(title)

    summary = get_value(article, "summary")
    if summary:
        parts.append(summary)

    text = get_value(article, "text")
    if text:
        parts.append(text)

    combined = " ".join(parts)

    if word_limit:
        words = combined.split()
        if len(words) > word_limit:
            combined = " ".join(words[:word_limit])

    return combined


def classify_articles(
    articles: list[Any],
    model: str = "ContextNews/news-classifier",
    batch_size: int = 32,
    threshold: float = 0.5,
    word_limit: int | None = None,
) -> list[ClassifiedArticle]:
    """
    Classify articles by topic using a HuggingFace text-classification model.

    Args:
        articles: List of article objects or dicts with title, summary, text fields
        model: Name of the HuggingFace model to use
        batch_size: Batch size for inference
        threshold: Minimum sigmoid score for a label to be included in topics
        word_limit: Maximum number of words in input text (None for no limit)

    Returns:
        List of ClassifiedArticle objects with topic labels and scores
    """
    if not articles:
        logger.warning("No articles to classify")
        return []

    valid_articles = []
    for article in articles:
        article_id = get_value(article, "id")
        if not article_id:
            logger.warning("Skipping article with missing id")
            continue

        text = _build_input_text(article, word_limit)
        if not text:
            logger.warning("Skipping article with empty text: id=%s", article_id)
            continue

        valid_articles.append((article_id, text))

    if not valid_articles:
        logger.warning("No valid articles to classify after filtering")
        return []

    logger.info("Loading model: %s", model)
    classifier = hf_pipeline("text-classification", model=model, top_k=None)

    article_ids = [a[0] for a in valid_articles]
    texts = [a[1] for a in valid_articles]

    logger.info("Classifying %d articles (batch_size=%d)", len(texts), batch_size)
    predictions = classifier(texts, batch_size=batch_size)

    results = []
    for article_id, preds in zip(article_ids, predictions):
        scores = {p["label"]: p["score"] for p in preds}
        topics = [label for label, score in scores.items() if score >= threshold]

        results.append(
            ClassifiedArticle(
                article_id=article_id,
                topics=topics,
                scores=scores,
            )
        )

    logger.info("Classified %d articles", len(results))
    return results
