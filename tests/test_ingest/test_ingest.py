"""Tests for ingest module."""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, Mock

from news_pipeline.stage1_ingest.ingest import ingest
from news_pipeline.stage1_ingest.models import RSSArticle, RawArticle, FetchedArticleText


@pytest.fixture
def mock_rss_article():
    return RSSArticle(
        source="bbc",
        title="Test Article",
        summary="Test summary",
        url="https://bbc.com/article",
        published_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    )


@pytest.fixture
def mock_article_text():
    return FetchedArticleText(text="Full article text", method="trafilatura")


class TestIngest:
    """Tests for the main ingest function."""

    @patch("news_pipeline.stage1_ingest.ingest.fetch_text")
    @patch("news_pipeline.stage1_ingest.ingest.fetch_rss_articles")
    def test_returns_raw_articles(self, mock_fetch_rss, mock_fetch_text, mock_rss_article, mock_article_text):
        mock_fetch_rss.return_value = [mock_rss_article]
        mock_fetch_text.return_value = mock_article_text

        result = ingest(["bbc"], lookback_hours=24)

        assert len(result) == 1
        assert isinstance(result[0], RawArticle)
        assert result[0].title == "Test Article"
        assert result[0].source == "bbc"

    @patch("news_pipeline.stage1_ingest.ingest.fetch_text")
    @patch("news_pipeline.stage1_ingest.ingest.fetch_rss_articles")
    def test_generates_article_id(self, mock_fetch_rss, mock_fetch_text, mock_rss_article, mock_article_text):
        mock_fetch_rss.return_value = [mock_rss_article]
        mock_fetch_text.return_value = mock_article_text

        result = ingest(["bbc"])

        assert result[0].id is not None
        assert len(result[0].id) == 16  # SHA256 truncated to 16 chars

    @patch("news_pipeline.stage1_ingest.ingest.fetch_text")
    @patch("news_pipeline.stage1_ingest.ingest.fetch_rss_articles")
    def test_fetches_article_text_when_enabled(self, mock_fetch_rss, mock_fetch_text, mock_rss_article, mock_article_text):
        mock_fetch_rss.return_value = [mock_rss_article]
        mock_fetch_text.return_value = mock_article_text

        result = ingest(["bbc"], fetch_article_text=True)

        mock_fetch_text.assert_called_once_with(mock_rss_article.url)
        assert result[0].article_text.text == "Full article text"

    @patch("news_pipeline.stage1_ingest.ingest.fetch_text")
    @patch("news_pipeline.stage1_ingest.ingest.fetch_rss_articles")
    def test_skips_article_text_when_disabled(self, mock_fetch_rss, mock_fetch_text, mock_rss_article):
        mock_fetch_rss.return_value = [mock_rss_article]

        result = ingest(["bbc"], fetch_article_text=False)

        mock_fetch_text.assert_not_called()
        assert result[0].article_text.text is None

    @patch("news_pipeline.stage1_ingest.ingest.fetch_text")
    @patch("news_pipeline.stage1_ingest.ingest.fetch_rss_articles")
    def test_processes_multiple_sources(self, mock_fetch_rss, mock_fetch_text, mock_article_text):
        bbc_article = RSSArticle(
            source="bbc", title="BBC Article", summary="", url="https://bbc.com/1",
            published_at=datetime.now(timezone.utc)
        )
        cnn_article = RSSArticle(
            source="cnn", title="CNN Article", summary="", url="https://cnn.com/1",
            published_at=datetime.now(timezone.utc)
        )
        mock_fetch_rss.side_effect = [[bbc_article], [cnn_article]]
        mock_fetch_text.return_value = mock_article_text

        result = ingest(["bbc", "cnn"])

        assert len(result) == 2
        assert result[0].source == "bbc"
        assert result[1].source == "cnn"

    @patch("news_pipeline.stage1_ingest.ingest.fetch_rss_articles")
    def test_continues_on_source_error(self, mock_fetch_rss, mock_rss_article):
        mock_fetch_rss.side_effect = [Exception("Network error"), [mock_rss_article]]

        with patch("news_pipeline.ingest.ingest.fetch_text") as mock_fetch_text:
            mock_fetch_text.return_value = FetchedArticleText(text=None)
            result = ingest(["failing-source", "bbc"], fetch_article_text=False)

        assert len(result) == 1
        assert result[0].source == "bbc"

    @patch("news_pipeline.stage1_ingest.ingest.fetch_rss_articles")
    def test_returns_empty_list_when_no_articles(self, mock_fetch_rss):
        mock_fetch_rss.return_value = []

        result = ingest(["bbc"])

        assert result == []

    @patch("news_pipeline.stage1_ingest.ingest.fetch_text")
    @patch("news_pipeline.stage1_ingest.ingest.fetch_rss_articles")
    def test_sets_fetched_at_timestamp(self, mock_fetch_rss, mock_fetch_text, mock_rss_article, mock_article_text):
        mock_fetch_rss.return_value = [mock_rss_article]
        mock_fetch_text.return_value = mock_article_text

        before = datetime.now(timezone.utc)
        result = ingest(["bbc"])
        after = datetime.now(timezone.utc)

        assert before <= result[0].fetched_at <= after

    @patch("news_pipeline.stage1_ingest.ingest.fetch_text")
    @patch("news_pipeline.stage1_ingest.ingest.fetch_rss_articles")
    def test_lookback_hours_default(self, mock_fetch_rss, mock_fetch_text):
        mock_fetch_rss.return_value = []

        ingest(["bbc"])

        # Verify fetch_rss_articles was called with a since time ~24 hours ago
        call_args = mock_fetch_rss.call_args[0]
        since = call_args[1]
        expected = datetime.now(timezone.utc) - timedelta(hours=24)
        assert abs((since - expected).total_seconds()) < 5

    @patch("news_pipeline.stage1_ingest.ingest.fetch_text")
    @patch("news_pipeline.stage1_ingest.ingest.fetch_rss_articles")
    def test_custom_lookback_hours(self, mock_fetch_rss, mock_fetch_text):
        mock_fetch_rss.return_value = []

        ingest(["bbc"], lookback_hours=48)

        call_args = mock_fetch_rss.call_args[0]
        since = call_args[1]
        expected = datetime.now(timezone.utc) - timedelta(hours=48)
        assert abs((since - expected).total_seconds()) < 5
