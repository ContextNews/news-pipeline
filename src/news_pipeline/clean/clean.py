"""Text cleaning functions."""

import re
from typing import Optional


def clean_text(text: Optional[str]) -> Optional[str]:
    """Clean text by stripping HTML, fixing escapes, and collapsing whitespace."""
    if not text:
        return None
    # Strip HTML tags (keep text content)
    text = re.sub(r"<[^>]+>", " ", text)
    # Remove escaped quotes
    text = text.replace('\\"', '"')
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text if text else None
