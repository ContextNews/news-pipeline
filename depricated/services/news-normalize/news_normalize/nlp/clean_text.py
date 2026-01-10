import re
from typing import Optional


def clean_text(text: Optional[str]) -> Optional[str]:
    """
    Clean article text by collapsing whitespace.

    Returns None if input is None or empty.
    """
    if not text:
        return None

    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()

    return text if text else None
