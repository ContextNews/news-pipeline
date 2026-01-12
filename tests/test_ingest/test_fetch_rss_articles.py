"""Tests for fetch_rss_articles module."""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, Mock

from news_pipeline.stage1_ingest.fetch_rss_articles import (
    fetch_rss_articles,
    _fetch_feed,
    _parse_entry,
    _parse_published_date,
)
from news_pipeline.stage1_ingest.models import RSSArticle


class TestFetchRssArticles:
    """Tests for the main fetch_rss_articles function."""

    @patch("news_pipeline.stage1_ingest.fetch_rss_articles.RSS_FEEDS", {"bbc": "https://bbc.com/rss"})
    @patch("news_pipeline.stage1_ingest.fetch_rss_articles._fetch_feed")
    def test_fetches_from_known_source(self, mock_fetch_feed):
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        mock_article = RSSArticle(
            source="bbc",
            title="Test Article",
            summary="Test summary",
            url="https://bbc.com/article",
            published_at=datetime(2024, 1, 2, tzinfo=timezone.utc),
        )
        mock_fetch_feed.return_value = [mock_article]

        result = list(fetch_rss_articles("bbc", since))

        assert len(result) == 1
        assert result[0].title == "Test Article"
        mock_fetch_feed.assert_called_once()

    @patch("news_pipeline.stage1_ingest.fetch_rss_articles.RSS_FEEDS", {})
    def test_returns_empty_for_unknown_source(self):
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)

        result = list(fetch_rss_articles("unknown", since))

        assert result == []

    @patch("news_pipeline.stage1_ingest.fetch_rss_articles.RSS_FEEDS", {"bbc": "https://bbc.com/rss"})
    @patch("news_pipeline.stage1_ingest.fetch_rss_articles._fetch_feed")
    def test_adds_utc_timezone_to_naive_datetime(self, mock_fetch_feed):
        since = datetime(2024, 1, 1)  # naive datetime
        mock_fetch_feed.return_value = []

        list(fetch_rss_articles("bbc", since))

        call_args = mock_fetch_feed.call_args[0]
        assert call_args[2].tzinfo == timezone.utc


class TestFetchFeed:
    """Tests for _fetch_feed function."""

    @patch("news_pipeline.stage1_ingest.fetch_rss_articles.feedparser")
    @patch("news_pipeline.stage1_ingest.fetch_rss_articles.requests")
    def test_parses_feed_entries(self, mock_requests, mock_feedparser):
        mock_response = Mock()
        mock_response.content = b"<rss>...</rss>"
        mock_requests.get.return_value = mock_response

        mock_feedparser.parse.return_value = Mock(
            entries=[
                {
                    "link": "https://example.com/1",
                    "title": "Article 1",
                    "summary": "Summary 1",
                    "published": "Mon, 01 Jan 2024 12:00:00 GMT",
                }
            ]
        )

        since = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        result = list(_fetch_feed("https://example.com/rss", "test", since, set()))

        assert len(result) == 1
        assert result[0].title == "Article 1"

    @patch("news_pipeline.stage1_ingest.fetch_rss_articles.requests")
    def test_sets_user_agent_header(self, mock_requests):
        mock_response = Mock()
        mock_response.content = b"<rss></rss>"
        mock_requests.get.return_value = mock_response

        with patch("news_pipeline.stage1_ingest.fetch_rss_articles.feedparser") as mock_fp:
            mock_fp.parse.return_value = Mock(entries=[])
            list(_fetch_feed("https://example.com/rss", "test", datetime.now(timezone.utc), set()))

        mock_requests.get.assert_called_once()
        call_kwargs = mock_requests.get.call_args[1]
        assert "User-Agent" in call_kwargs["headers"]


class TestParseEntry:
    """Tests for _parse_entry function."""

    def test_parses_valid_entry(self):
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
        assert result.summary == "Test summary"
        assert result.url == "https://example.com/article"
        assert result.source == "test"

    def test_returns_none_for_missing_url(self):
        entry = {"title": "Test", "published": "Mon, 01 Jan 2024 12:00:00 GMT"}
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)

        result = _parse_entry(entry, "test", since, set())

        assert result is None

    def test_returns_none_for_duplicate_url(self):
        entry = {
            "link": "https://example.com/article",
            "title": "Test",
            "published": "Mon, 01 Jan 2024 12:00:00 GMT",
        }
        since = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        seen_urls = {"https://example.com/article"}

        result = _parse_entry(entry, "test", since, seen_urls)

        assert result is None

    def test_returns_none_for_old_article(self):
        entry = {
            "link": "https://example.com/article",
            "title": "Test",
            "published": "Mon, 01 Jan 2023 12:00:00 GMT",
        }
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)

        result = _parse_entry(entry, "test", since, set())

        assert result is None

    def test_returns_none_for_missing_title(self):
        entry = {
            "link": "https://example.com/article",
            "title": "",
            "published": "Mon, 01 Jan 2024 12:00:00 GMT",
        }
        since = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        result = _parse_entry(entry, "test", since, set())

        assert result is None

    def test_adds_url_to_seen_urls(self):
        entry = {
            "link": "https://example.com/article",
            "title": "Test",
            "summary": "Summary",
            "published": "Mon, 01 Jan 2024 12:00:00 GMT",
        }
        since = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        seen_urls = set()

        _parse_entry(entry, "test", since, seen_urls)

        assert "https://example.com/article" in seen_urls


class TestParsePubDate:
    """Tests for _parse_published_date function."""

    def test_parses_rfc2822_format(self):
        entry = {"published": "Mon, 01 Jan 2024 12:00:00 GMT"}

        result = _parse_published_date(entry)

        assert result == datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    def test_parses_with_timezone_abbreviation(self):
        entry = {"published": "Mon, 01 Jan 2024 12:00:00 EST"}

        result = _parse_published_date(entry)

        assert result is not None
        assert result.tzinfo is not None

    def test_falls_back_to_updated_field(self):
        entry = {"updated": "Mon, 01 Jan 2024 12:00:00 GMT"}

        result = _parse_published_date(entry)

        assert result is not None

    def test_returns_none_for_missing_date(self):
        entry = {}

        result = _parse_published_date(entry)

        assert result is None

    def test_returns_none_for_invalid_date(self):
        entry = {"published": "not a date"}

        result = _parse_published_date(entry)

        assert result is None

    def test_adds_utc_to_naive_datetime(self):
        entry = {"published": "2024-01-01 12:00:00"}

        result = _parse_published_date(entry)

        assert result is not None
        assert result.tzinfo == timezone.utc
