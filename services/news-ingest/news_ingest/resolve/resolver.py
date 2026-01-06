"""Article text resolution logic."""

import logging
from dataclasses import dataclass
from typing import Optional

from news_ingest.config import get_config
from news_ingest.resolve.extractors import extract_with_trafilatura, extract_with_readability

logger = logging.getLogger(__name__)


@dataclass
class ResolveResult:
    """Result of article text resolution."""
    text: Optional[str]
    method: Optional[str]
    error: Optional[str] = None

    @property
    def success(self) -> bool:
        """Whether resolution was successful."""
        return self.text is not None


def resolve_article(url: str) -> ResolveResult:
    """Resolve full article text from URL.

    Try in order:
    1. trafilatura
    2. readability-lxml

    Args:
        url: The article URL to fetch

    Returns:
        ResolveResult with text, method used, and any error
    """
    config = get_config()

    # Try trafilatura with retries
    for attempt in range(config.resolve.max_retries):
        try:
            text = extract_with_trafilatura(url)
            if text:
                return ResolveResult(text=text, method="trafilatura")
            break  # No retry if extraction returned None (not a network error)
        except Exception as e:
            logger.warning(f"trafilatura attempt {attempt + 1} failed for {url}: {e}")
            if attempt == config.resolve.max_retries - 1:
                pass  # Fall through to readability

    # Try readability as fallback
    for attempt in range(config.resolve.max_retries):
        try:
            text = extract_with_readability(url)
            if text:
                return ResolveResult(text=text, method="readability")
            break
        except Exception as e:
            logger.warning(f"readability attempt {attempt + 1} failed for {url}: {e}")
            if attempt == config.resolve.max_retries - 1:
                return ResolveResult(text=None, method=None, error=str(e))

    return ResolveResult(text=None, method=None, error="All extraction methods failed")
