"""Text cleaning functions."""

import re
from typing import Optional


def clean_text(text: Optional[str]) -> Optional[str]:
    """Clean text by collapsing whitespace. Returns None if input is empty."""
    if not text:
        return None
    text = re.sub(r"\s+", " ", text).strip()
    return text if text else None
