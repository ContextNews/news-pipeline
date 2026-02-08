"""Tests for ingest_articles.helpers module."""

from unittest.mock import patch

import pytest

from ingest_articles.helpers import parse_sources


class TestParseSources:
    @patch(
        "ingest_articles.helpers.RSS_FEEDS",
        {"bbc": "url1", "cnn": "url2", "fox": "url3"},
    )
    def test_none_returns_all_sources(self) -> None:
        result = parse_sources(None)
        assert set(result) == {"bbc", "cnn", "fox"}

    @patch(
        "ingest_articles.helpers.RSS_FEEDS",
        {"bbc": "url1", "cnn": "url2", "fox": "url3"},
    )
    def test_all_string_returns_all_sources(self) -> None:
        result = parse_sources("all")
        assert set(result) == {"bbc", "cnn", "fox"}

    @patch(
        "ingest_articles.helpers.RSS_FEEDS",
        {"bbc": "url1", "cnn": "url2", "fox": "url3"},
    )
    def test_valid_comma_separated(self) -> None:
        result = parse_sources("bbc,cnn")
        assert set(result) == {"bbc", "cnn"}

    @patch(
        "ingest_articles.helpers.RSS_FEEDS",
        {"bbc": "url1", "cnn": "url2"},
    )
    def test_invalid_sources_raise_value_error(self) -> None:
        with pytest.raises(ValueError):
            parse_sources("invalid_source")
