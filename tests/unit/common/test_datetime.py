"""Tests for common.datetime module."""

from datetime import datetime, timezone

from common.datetime import parse_datetime


class TestParseDatetime:
    def test_none_returns_current_utc(self) -> None:
        before = datetime.now(timezone.utc)
        result = parse_datetime(None)
        after = datetime.now(timezone.utc)
        assert before <= result <= after
        assert result.tzinfo is not None

    def test_datetime_passthrough(self) -> None:
        dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert parse_datetime(dt) is dt

    def test_iso_string_parsing(self) -> None:
        result = parse_datetime("2024-01-01T12:00:00+00:00")
        assert result == datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    def test_iso_string_with_z_suffix(self) -> None:
        result = parse_datetime("2024-01-01T12:00:00Z")
        assert result == datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
