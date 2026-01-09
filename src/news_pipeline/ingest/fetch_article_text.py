import logging
from typing import Optional

import requests
import trafilatura
from readability import Document
from lxml import html as lxml_html

from news_pipeline.ingest.models import FetchedArticleText

logger = logging.getLogger(__name__)

def fetch_article_text(url: str) -> FetchedArticleText:
    """
    Fetch full article text from URL.

    Order:
    1. trafilatura
    2. readability-lxml

    Each tried once. If both fail → failure.
    """

    # 1. Try trafilatura
    try:
        text = fetch_with_trafilatura(url)
        if text:
            return FetchedArticleText(text=text, method="trafilatura")
    except Exception as e:
        logger.warning(f"trafilatura failed for {url}: {e}")

    # 2. Fallback to readability
    try:
        text = fetch_with_readability(url)
        if text:
            return FetchedArticleText(text=text, method="readability")
    except Exception as e:
        logger.warning(f"readability failed for {url}: {e}")
        return FetchedArticleText(text=None, method=None, error=str(e))

    # Both methods failed
    return FetchedArticleText(
        text=None,
        method=None,
        error="All extraction methods failed"
    )


def fetch_with_trafilatura(url: str) -> Optional[str]:
    downloaded = trafilatura.fetch_url(url)
    if not downloaded:
        return None
    return trafilatura.extract(downloaded)


def fetch_with_readability(url: str) -> Optional[str]:
    response = requests.get(url, timeout=10)
    response.raise_for_status()

    doc = Document(response.text)
    summary_html = doc.summary()

    tree = lxml_html.fromstring(summary_html)
    text = tree.text_content()

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return "\n".join(lines) if lines else None