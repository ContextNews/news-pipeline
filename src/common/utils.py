"""Common utility functions."""

from typing import Any


def get_value(obj: Any, key: str) -> Any:
    """Get value from dict or object attribute."""
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)
