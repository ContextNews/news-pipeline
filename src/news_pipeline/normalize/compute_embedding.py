"""Text embedding using sentence-transformers."""

import logging
from typing import Optional

import numpy as np

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


def compute_embedding(text: Optional[str], model_key: str = "minilm") -> Optional[list[float]]:
    """Compute embedding for text. Returns None if text is empty."""
    if not text or not text.strip():
        return None

    model = _get_model(model_key)
    model_info = EMBEDDING_MODELS[model_key]

    # Check if chunking needed
    max_chars = model_info["max_tokens"] * 4
    if len(text) <= max_chars:
        embedding = model.encode(text, convert_to_numpy=True)
        return embedding.tolist()

    # Split into chunks and mean pool
    chunks = []
    for i in range(0, len(text), max_chars - 100):
        chunks.append(text[i:i + max_chars])

    embeddings = model.encode(chunks, convert_to_numpy=True)
    pooled = np.mean(embeddings, axis=0)
    return pooled.tolist()


def compute_embeddings_batch(
    texts: list[Optional[str]],
    model_key: str = "minilm",
    batch_size: int = 32,
) -> list[Optional[list[float]]]:
    """Compute embeddings for multiple texts in batches.

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
    model_info = EMBEDDING_MODELS[model_key]
    max_chars = model_info["max_tokens"] * 4

    # Track which texts need processing and which need chunking
    results: list[Optional[list[float]]] = [None] * len(texts)
    simple_indices: list[int] = []
    simple_texts: list[str] = []
    chunked_indices: list[int] = []

    for i, text in enumerate(texts):
        if not text or not text.strip():
            continue
        if len(text) <= max_chars:
            simple_indices.append(i)
            simple_texts.append(text)
        else:
            chunked_indices.append(i)

    # Batch encode simple texts
    if simple_texts:
        logger.info(f"Batch encoding {len(simple_texts)} texts (batch_size={batch_size})")
        embeddings = model.encode(simple_texts, batch_size=batch_size, convert_to_numpy=True)
        for idx, emb in zip(simple_indices, embeddings):
            results[idx] = emb.tolist()

    # Handle chunked texts individually (they need mean pooling)
    for i in chunked_indices:
        text = texts[i]
        chunks = []
        for j in range(0, len(text), max_chars - 100):
            chunks.append(text[j:j + max_chars])
        embeddings = model.encode(chunks, convert_to_numpy=True)
        pooled = np.mean(embeddings, axis=0)
        results[i] = pooled.tolist()

    if chunked_indices:
        logger.info(f"Processed {len(chunked_indices)} texts requiring chunking")

    return results
