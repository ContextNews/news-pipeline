"""Text embedding using sentence-transformers."""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

EMBEDDING_MODELS = {
    "minilm": {"name": "all-MiniLM-L6-v2", "dim": 384, "max_tokens": 256},
    "mpnet": {"name": "all-mpnet-base-v2", "dim": 768, "max_tokens": 384},
}

_model_cache: dict = {}


def _get_model(model_key: str):
    """Lazy load embedding model."""
    if model_key not in EMBEDDING_MODELS:
        raise ValueError(f"Invalid model: {model_key}")

    if model_key not in _model_cache:
        from sentence_transformers import SentenceTransformer
        model_name = EMBEDDING_MODELS[model_key]["name"]
        logger.info(f"Loading embedding model: {model_name}")
        _model_cache[model_key] = SentenceTransformer(model_name)

    return _model_cache[model_key]


def prepare_embedding_text(
    title: Optional[str],
    summary: Optional[str],
    article_text: Optional[str],
    model_key: str = "minilm",
) -> Optional[str]:
    """Combine title, summary, and article text, truncated to model's max tokens.

    Args:
        title: Article title
        summary: Article summary
        article_text: Cleaned article text
        model_key: Embedding model key

    Returns:
        Combined text truncated to max tokens, or None if no text
    """
    # Combine non-empty parts with newlines
    parts = []
    if title and title.strip():
        parts.append(title.strip())
    if summary and summary.strip():
        parts.append(summary.strip())
    if article_text and article_text.strip():
        parts.append(article_text.strip())

    if not parts:
        return None

    combined = "\n\n".join(parts)

    # Truncate to max tokens using the model's tokenizer
    model = _get_model(model_key)
    max_tokens = EMBEDDING_MODELS[model_key]["max_tokens"]

    tokenizer = model.tokenizer
    encoded = tokenizer(
        combined,
        truncation=True,
        max_length=max_tokens,
        return_attention_mask=False,
    )

    truncated = tokenizer.decode(encoded["input_ids"], skip_special_tokens=True)
    return truncated if truncated.strip() else None


def compute_embeddings_batch(
    texts: list[Optional[str]],
    model_key: str = "minilm",
    batch_size: int = 32,
) -> list[Optional[list[float]]]:
    """Compute embeddings for multiple texts in batches.

    Texts should already be prepared/truncated via prepare_embedding_text().

    Args:
        texts: List of texts (can contain None or empty strings)
        model_key: Embedding model key
        batch_size: Batch size for encoding

    Returns:
        List of embeddings (None for empty/None inputs)
    """
    if not texts:
        return []

    model = _get_model(model_key)

    # Filter valid texts and track indices
    results: list[Optional[list[float]]] = [None] * len(texts)
    valid_indices: list[int] = []
    valid_texts: list[str] = []

    for i, text in enumerate(texts):
        if text and text.strip():
            valid_indices.append(i)
            valid_texts.append(text)

    if not valid_texts:
        return results

    # Batch encode all texts
    logger.info(f"Batch encoding {len(valid_texts)} texts (batch_size={batch_size})")
    embeddings = model.encode(
        valid_texts,
        batch_size=batch_size,
        convert_to_numpy=True,
        show_progress_bar=False,
    )

    for idx, emb in zip(valid_indices, embeddings):
        results[idx] = emb.tolist()

    return results
