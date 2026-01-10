"""Article text resolution logic."""

import logging

from news_ingest.config import Config
from news_ingest.resolve.extractors import extract_with_trafilatura, extract_with_readability
from news_ingest.schema import ResolveResult

logger = logging.getLogger(__name__)


def resolve_article(url: str, config: Config) -> ResolveResult:
    """Resolve full article text from URL.

    Try in order:
    1. trafilatura
    2. readability-lxml

    Args:
        url: The article URL to fetch
        config: Configuration object

    Returns:
        ResolveResult with text, method used, and any error
    """
    # Try trafilatura with retries
    for attempt in range(config.resolve_max_retries):
        try:
            text = extract_with_trafilatura(url)
            if text:
                return ResolveResult(text=text, method="trafilatura")
            break  # No retry if extraction returned None (not a network error)
        except Exception as e:
            logger.warning(f"trafilatura attempt {attempt + 1} failed for {url}: {e}")
            if attempt == config.resolve_max_retries - 1:
                pass  # Fall through to readability

    # Try readability as fallback
    for attempt in range(config.resolve_max_retries):
        try:
            text = extract_with_readability(url, config)
            if text:
                return ResolveResult(text=text, method="readability")
            break
        except Exception as e:
            logger.warning(f"readability attempt {attempt + 1} failed for {url}: {e}")
            if attempt == config.resolve_max_retries - 1:
                return ResolveResult(text=None, method=None, error=str(e))

    return ResolveResult(text=None, method=None, error="All extraction methods failed")
