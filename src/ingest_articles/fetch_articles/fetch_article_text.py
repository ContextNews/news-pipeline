import logging
from typing import Optional

import requests
import trafilatura
from readability import Document
from lxml import html as lxml_html

logger = logging.getLogger(__name__)


def fetch_article_text(url: str) -> Optional[str]:
    """
    Fetch full article text from URL.

    Order:
    1. trafilatura
    2. readability-lxml

    Each tried once. If both fail -> returns None.
    """

    # 1. Try trafilatura
    try:
        text = fetch_with_trafilatura(url)
        if text:
            return text
    except Exception as e:
        logger.warning("trafilatura failed for %s: %s", url, e)

    # 2. Fallback to readability
    try:
        text = fetch_with_readability(url)
        if text:
            return text
    except Exception as e:
        logger.warning("readability failed for %s: %s", url, e)

    # Both methods failed
    return None


def fetch_with_trafilatura(url: str) -> Optional[str]:
    downloaded = trafilatura.fetch_url(url)
    if not downloaded:
        return None
    return trafilatura.extract(downloaded)


def fetch_with_readability(url: str) -> Optional[str]:
    response = requests.get(
        url,
        timeout=10,
        headers={"User-Agent": "news-ingest/1.0 (RSS reader)"},
    )
    response.raise_for_status()

    doc = Document(response.text)
    summary_html = doc.summary()

    tree = lxml_html.fromstring(summary_html)
    text = tree.text_content()

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return "\n".join(lines) if lines else None
