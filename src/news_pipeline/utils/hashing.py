"""Hashing utilities."""

import hashlib


def generate_article_id(source: str, url: str) -> str:
    """Generate a unique article ID from source and URL."""
    return hashlib.sha256(f"{source}:{url}".encode()).hexdigest()[:16]
