"""Article text extraction backends."""

from typing import Optional

import requests
import trafilatura
from lxml import html as lxml_html
from readability import Document

from news_ingest.config import Config


def extract_with_trafilatura(url: str) -> Optional[str]:
    """Extract article text using trafilatura."""
    downloaded = trafilatura.fetch_url(url)
    if downloaded is None:
        return None
    return trafilatura.extract(downloaded)


def extract_with_readability(url: str, config: Config) -> Optional[str]:
    """Extract article text using readability-lxml as fallback."""
    response = requests.get(url, timeout=config.resolve_request_timeout)
    response.raise_for_status()
    doc = Document(response.text)
    summary_html = doc.summary()
    # Convert HTML to plain text
    tree = lxml_html.fromstring(summary_html)
    text = tree.text_content()
    # Clean up whitespace
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return "\n".join(lines) if lines else None
