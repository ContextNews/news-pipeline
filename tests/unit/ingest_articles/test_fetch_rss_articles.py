"""Tests for ingest_articles.fetch_articles.fetch_rss_articles module."""

from datetime import datetime, timezone, timedelta
from unittest.mock import patch, Mock

from ingest_articles.fetch_articles.fetch_rss_articles import (
    fetch_rss_articles,
    _parse_entry,
    _parse_published_date,
)


class TestFetchRssArticles:
    @patch("ingest_articles.fetch_articles.fetch_rss_articles.RSS_FEEDS", {})
    def test_unknown_source_yields_nothing(self) -> None:
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        result = list(fetch_rss_articles("unknown", since))
        assert result == []

    @patch("ingest_articles.fetch_articles.fetch_rss_articles.RSS_FEEDS", {"bbc": "https://bbc.com/rss"})
    @patch("ingest_articles.fetch_articles.fetch_rss_articles._fetch_feed")
    def test_adds_utc_to_naive_since(self, mock_fetch_feed) -> None:
        mock_fetch_feed.return_value = []
        since = datetime(2024, 1, 1)  # naive
        list(fetch_rss_articles("bbc", since))
        call_args = mock_fetch_feed.call_args[0]
        assert call_args[2].tzinfo == timezone.utc


class TestParseEntry:
    def test_returns_none_for_missing_url(self) -> None:
        entry = {"title": "Test", "published": "Mon, 01 Jan 2024 12:00:00 GMT"}
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        assert _parse_entry(entry, "test", since, set()) is None

    def test_returns_none_for_duplicate_url(self) -> None:
        entry = {
            "link": "https://example.com/1",
            "title": "Test",
            "published": "Mon, 01 Jan 2024 12:00:00 GMT",
        }
        since = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        seen = {"https://example.com/1"}
        assert _parse_entry(entry, "test", since, seen) is None

    def test_returns_none_for_empty_title(self) -> None:
        entry = {
            "link": "https://example.com/1",
            "title": "",
            "published": "Mon, 01 Jan 2024 12:00:00 GMT",
        }
        since = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert _parse_entry(entry, "test", since, set()) is None

    def test_returns_none_for_old_article(self) -> None:
        entry = {
            "link": "https://example.com/1",
            "title": "Test",
            "published": "Mon, 01 Jan 2023 12:00:00 GMT",
        }
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        assert _parse_entry(entry, "test", since, set()) is None

    def test_parses_valid_entry(self) -> None:
        entry = {
            "link": "https://example.com/article",
            "title": "Test Title",
            "summary": "Test summary",
            "published": "Mon, 01 Jan 2024 12:00:00 GMT",
        }
        since = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        result = _parse_entry(entry, "test", since, set())
        assert result is not None
        assert result.title == "Test Title"
        assert result.url == "https://example.com/article"

    def test_adds_url_to_seen_urls(self) -> None:
        entry = {
            "link": "https://example.com/article",
            "title": "Test",
            "summary": "",
            "published": "Mon, 01 Jan 2024 12:00:00 GMT",
        }
        since = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        seen = set()
        _parse_entry(entry, "test", since, seen)
        assert "https://example.com/article" in seen


class TestParsePublishedDate:
    def test_parses_rfc2822_with_gmt(self) -> None:
        entry = {"published": "Mon, 01 Jan 2024 12:00:00 GMT"}
        result = _parse_published_date(entry)
        assert result == datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    def test_parses_est_timezone(self) -> None:
        entry = {"published": "Mon, 01 Jan 2024 12:00:00 EST"}
        result = _parse_published_date(entry)
        assert result is not None
        assert result.tzinfo is not None
        assert result.utcoffset() == timedelta(hours=-5)

    def test_parses_bst_timezone(self) -> None:
        entry = {"published": "Mon, 01 Jul 2024 12:00:00 BST"}
        result = _parse_published_date(entry)
        assert result is not None
        assert result.utcoffset() == timedelta(hours=1)

    def test_falls_back_to_updated_field(self) -> None:
        entry = {"updated": "Mon, 01 Jan 2024 12:00:00 GMT"}
        result = _parse_published_date(entry)
        assert result is not None

    def test_returns_none_for_missing_date(self) -> None:
        assert _parse_published_date({}) is None

    def test_returns_none_for_invalid_date(self) -> None:
        assert _parse_published_date({"published": "not a date"}) is None

    def test_naive_datetime_gets_utc(self) -> None:
        entry = {"published": "2024-01-01 12:00:00"}
        result = _parse_published_date(entry)
        assert result is not None
        assert result.tzinfo == timezone.utc
