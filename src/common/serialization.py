"""Serialization utilities."""

from dataclasses import asdict
from datetime import datetime


def serialize_dataclass(obj) -> dict:
    """Serialize a dataclass to dict, converting datetimes to ISO strings."""
    data = asdict(obj)
    for key, value in data.items():
        if isinstance(value, datetime):
            data[key] = value.isoformat()
        elif isinstance(value, dict):
            for k, v in value.items():
                if isinstance(v, datetime):
                    value[k] = v.isoformat()
    return data
