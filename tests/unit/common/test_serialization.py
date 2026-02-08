"""Tests for common.serialization module."""

from dataclasses import dataclass
from datetime import datetime, timezone

from common.serialization import serialize_dataclass


@dataclass
class SampleData:
    name: str
    value: int


@dataclass
class SampleWithDatetime:
    name: str
    created_at: datetime


@dataclass
class SampleWithNestedDict:
    name: str
    metadata: dict


class TestSerializeDataclass:
    def test_basic_dataclass_to_dict(self) -> None:
        obj = SampleData(name="test", value=42)
        result = serialize_dataclass(obj)
        assert result == {"name": "test", "value": 42}

    def test_datetime_field_to_iso_string(self) -> None:
        dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        obj = SampleWithDatetime(name="test", created_at=dt)
        result = serialize_dataclass(obj)
        assert result["created_at"] == "2024-01-01T12:00:00+00:00"

    def test_nested_dict_datetime_handling(self) -> None:
        dt = datetime(2024, 6, 15, 8, 30, 0, tzinfo=timezone.utc)
        obj = SampleWithNestedDict(name="test", metadata={"updated_at": dt})
        result = serialize_dataclass(obj)
        assert result["metadata"]["updated_at"] == "2024-06-15T08:30:00+00:00"
