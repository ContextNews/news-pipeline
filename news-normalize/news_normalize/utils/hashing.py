import hashlib


def hash_url(url: str) -> str:
    """Generate a stable hash for a URL."""
    return hashlib.sha256(url.encode()).hexdigest()[:16]
