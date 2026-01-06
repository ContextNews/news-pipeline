"""Text embedding generation using sentence-transformers."""

import logging
from dataclasses import dataclass
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)

# Lazy import to avoid loading torch at module import time
_SentenceTransformer = None


def _get_sentence_transformer_class():
    """Lazy import of SentenceTransformer."""
    global _SentenceTransformer
    if _SentenceTransformer is None:
        from sentence_transformers import SentenceTransformer
        _SentenceTransformer = SentenceTransformer
    return _SentenceTransformer


# Model registry with metadata
EMBEDDING_MODELS = {
    "minilm": {
        "name": "all-MiniLM-L6-v2",
        "dim": 384,
        "max_tokens": 256,
    },
    "mpnet": {
        "name": "all-mpnet-base-v2",
        "dim": 768,
        "max_tokens": 384,
    },
    "minilm-l12": {
        "name": "all-MiniLM-L12-v2",
        "dim": 384,
        "max_tokens": 256,
    },
}
DEFAULT_EMBEDDING_MODEL = "minilm"

# Model cache (lazy loading)
_embedding_cache: dict = {}


@dataclass
class ArticleEmbeddings:
    """Container for article embedding vectors."""
    headline: Optional[list[float]] = None
    content: Optional[list[float]] = None
    combined: Optional[list[float]] = None
    model: Optional[str] = None
    dim: Optional[int] = None
    chunks: Optional[int] = None


def get_embedding_model(model_key: str = DEFAULT_EMBEDDING_MODEL):
    """
    Get or load a sentence-transformer model.

    Args:
        model_key: Model key from EMBEDDING_MODELS registry

    Returns:
        SentenceTransformer model instance
    """
    if model_key not in EMBEDDING_MODELS:
        raise ValueError(
            f"Invalid model key: {model_key}. "
            f"Must be one of {list(EMBEDDING_MODELS.keys())}"
        )

    if model_key not in _embedding_cache:
        model_info = EMBEDDING_MODELS[model_key]
        SentenceTransformer = _get_sentence_transformer_class()
        logger.info(f"Loading embedding model: {model_info['name']}")
        _embedding_cache[model_key] = SentenceTransformer(model_info["name"])

    return _embedding_cache[model_key]


def get_model_info(model_key: str = DEFAULT_EMBEDDING_MODEL) -> dict:
    """Get model metadata without loading the model."""
    if model_key not in EMBEDDING_MODELS:
        raise ValueError(f"Invalid model key: {model_key}")
    return EMBEDDING_MODELS[model_key]


def split_into_chunks(
    text: str,
    max_chars: int = 1000,
    overlap_chars: int = 100,
) -> list[str]:
    """
    Split text into chunks for embedding.

    Uses a simple character-based approach with sentence boundary heuristics.
    Aims to split at sentence boundaries (. ! ?) when possible.

    Args:
        text: Text to split
        max_chars: Maximum characters per chunk (~4 chars/token, so 1000 chars ≈ 250 tokens)
        overlap_chars: Overlap between chunks for context preservation

    Returns:
        List of text chunks
    """
    if not text or len(text) <= max_chars:
        return [text] if text else []

    chunks = []
    start = 0

    while start < len(text):
        end = start + max_chars

        if end >= len(text):
            # Last chunk
            chunks.append(text[start:])
            break

        # Try to find a sentence boundary near the end
        # Look backwards from end position for . ! or ?
        search_start = max(start + max_chars - 200, start)
        best_break = end

        for i in range(end, search_start, -1):
            if text[i-1] in '.!?' and (i >= len(text) or text[i] in ' \n\t'):
                best_break = i
                break

        chunks.append(text[start:best_break])
        start = best_break - overlap_chars if best_break > overlap_chars else best_break

    return chunks


def embed_text(
    text: Optional[str],
    model_key: str = DEFAULT_EMBEDDING_MODEL,
) -> Optional[np.ndarray]:
    """
    Embed a single text, handling chunking for long texts.

    Args:
        text: Text to embed (can be None or empty)
        model_key: Model key from registry

    Returns:
        Embedding vector as numpy array, or None if text is empty
    """
    if not text or not text.strip():
        return None

    model = get_embedding_model(model_key)
    model_info = EMBEDDING_MODELS[model_key]

    # Estimate if chunking is needed (~4 chars per token)
    estimated_tokens = len(text) / 4
    max_tokens = model_info["max_tokens"]

    if estimated_tokens <= max_tokens * 0.9:  # 10% safety margin
        # Short text - embed directly
        return model.encode(text, convert_to_numpy=True)

    # Long text - chunk and mean pool
    chunks = split_into_chunks(text, max_chars=max_tokens * 4)
    if not chunks:
        return None

    chunk_embeddings = model.encode(chunks, convert_to_numpy=True)
    return np.mean(chunk_embeddings, axis=0)


def embed_text_with_chunks(
    text: Optional[str],
    model_key: str = DEFAULT_EMBEDDING_MODEL,
) -> tuple[Optional[np.ndarray], int]:
    """
    Embed text and return chunk count.

    Returns:
        Tuple of (embedding, chunk_count)
    """
    if not text or not text.strip():
        return None, 0

    model = get_embedding_model(model_key)
    model_info = EMBEDDING_MODELS[model_key]

    estimated_tokens = len(text) / 4
    max_tokens = model_info["max_tokens"]

    if estimated_tokens <= max_tokens * 0.9:
        return model.encode(text, convert_to_numpy=True), 1

    chunks = split_into_chunks(text, max_chars=max_tokens * 4)
    if not chunks:
        return None, 0

    chunk_embeddings = model.encode(chunks, convert_to_numpy=True)
    return np.mean(chunk_embeddings, axis=0), len(chunks)


def compute_combined_embedding(
    headline_emb: Optional[np.ndarray],
    content_emb: Optional[np.ndarray],
    weight_headline: float = 0.3,
    weight_content: float = 0.7,
) -> Optional[np.ndarray]:
    """
    Compute weighted combination of headline and content embeddings.

    Args:
        headline_emb: Headline embedding vector
        content_emb: Content embedding vector
        weight_headline: Weight for headline (default 0.3)
        weight_content: Weight for content (default 0.7)

    Returns:
        Combined embedding, normalized to unit length
    """
    if headline_emb is None and content_emb is None:
        return None

    if headline_emb is None:
        return content_emb

    if content_emb is None:
        return headline_emb

    # Weighted combination
    combined = (weight_headline * headline_emb) + (weight_content * content_emb)

    # Normalize to unit length
    norm = np.linalg.norm(combined)
    if norm > 0:
        combined = combined / norm

    return combined


def compute_article_embeddings(
    headline: Optional[str],
    content: Optional[str],
    model_key: str = DEFAULT_EMBEDDING_MODEL,
    weight_headline: float = 0.3,
    weight_content: float = 0.7,
) -> ArticleEmbeddings:
    """
    Compute all embeddings for a single article.

    Args:
        headline: Article headline
        content: Article content (cleaned)
        model_key: Embedding model key
        weight_headline: Weight for headline in combined embedding
        weight_content: Weight for content in combined embedding

    Returns:
        ArticleEmbeddings with all vectors populated
    """
    model_info = EMBEDDING_MODELS[model_key]

    # Embed headline
    headline_emb = embed_text(headline, model_key)

    # Embed content with chunk tracking
    content_emb, chunks = embed_text_with_chunks(content, model_key)

    # Compute combined embedding
    combined_emb = compute_combined_embedding(
        headline_emb, content_emb, weight_headline, weight_content
    )

    return ArticleEmbeddings(
        headline=headline_emb.tolist() if headline_emb is not None else None,
        content=content_emb.tolist() if content_emb is not None else None,
        combined=combined_emb.tolist() if combined_emb is not None else None,
        model=f"sentence-transformers/{model_info['name']}",
        dim=model_info["dim"],
        chunks=chunks if chunks > 0 else None,
    )


def compute_embeddings_batch(
    headlines: list[Optional[str]],
    contents: list[Optional[str]],
    model_key: str = DEFAULT_EMBEDDING_MODEL,
    weight_headline: float = 0.3,
    weight_content: float = 0.7,
    batch_size: int = 32,
) -> list[ArticleEmbeddings]:
    """
    Compute embeddings for multiple articles efficiently.

    Batches encoding operations for better GPU/CPU utilization.

    Args:
        headlines: List of headlines (can contain None)
        contents: List of contents (can contain None)
        model_key: Embedding model key
        weight_headline: Weight for headline in combined embedding
        weight_content: Weight for content in combined embedding
        batch_size: Batch size for encoding

    Returns:
        List of ArticleEmbeddings, one per article
    """
    if len(headlines) != len(contents):
        raise ValueError("Headlines and contents must have same length")

    n_articles = len(headlines)
    model = get_embedding_model(model_key)
    model_info = EMBEDDING_MODELS[model_key]
    max_chars = model_info["max_tokens"] * 4

    logger.info(f"Computing embeddings for {n_articles} articles")

    # Collect all texts to embed in batches
    # Track: (original_idx, text_type, chunk_idx, text)
    headline_texts: list[tuple[int, str]] = []
    content_chunks: list[tuple[int, int, str]] = []  # (article_idx, chunk_idx, text)
    chunk_counts: list[int] = []

    for i, (headline, content) in enumerate(zip(headlines, contents)):
        # Headlines
        if headline and headline.strip():
            headline_texts.append((i, headline))

        # Contents (with chunking)
        if content and content.strip():
            chunks = split_into_chunks(content, max_chars=max_chars)
            chunk_counts.append(len(chunks))
            for chunk_idx, chunk in enumerate(chunks):
                content_chunks.append((i, chunk_idx, chunk))
        else:
            chunk_counts.append(0)

    # Batch encode headlines
    headline_embeddings: dict[int, np.ndarray] = {}
    if headline_texts:
        texts = [t[1] for t in headline_texts]
        embeddings = model.encode(texts, batch_size=batch_size, convert_to_numpy=True)
        for (idx, _), emb in zip(headline_texts, embeddings):
            headline_embeddings[idx] = emb

    # Batch encode content chunks
    content_chunk_embeddings: dict[int, list[np.ndarray]] = {i: [] for i in range(n_articles)}
    if content_chunks:
        texts = [t[2] for t in content_chunks]
        embeddings = model.encode(texts, batch_size=batch_size, convert_to_numpy=True)
        for (article_idx, chunk_idx, _), emb in zip(content_chunks, embeddings):
            content_chunk_embeddings[article_idx].append(emb)

    # Assemble results
    results: list[ArticleEmbeddings] = []

    for i in range(n_articles):
        headline_emb = headline_embeddings.get(i)

        # Mean pool content chunks
        content_emb = None
        if content_chunk_embeddings[i]:
            content_emb = np.mean(content_chunk_embeddings[i], axis=0)

        # Combined embedding
        combined_emb = compute_combined_embedding(
            headline_emb, content_emb, weight_headline, weight_content
        )

        results.append(ArticleEmbeddings(
            headline=headline_emb.tolist() if headline_emb is not None else None,
            content=content_emb.tolist() if content_emb is not None else None,
            combined=combined_emb.tolist() if combined_emb is not None else None,
            model=f"sentence-transformers/{model_info['name']}",
            dim=model_info["dim"],
            chunks=chunk_counts[i] if chunk_counts[i] > 0 else None,
        ))

    logger.info(f"Computed embeddings for {n_articles} articles")
    return results
