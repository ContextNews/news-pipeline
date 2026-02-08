"""Tests for ingest_articles.fetch_articles.fetch_articles module."""

from datetime import datetime, timezone
from unittest.mock import patch

from ingest_articles.fetch_articles.fetch_articles import fetch_articles
from ingest_articles.models import RSSArticle


@patch("ingest_articles.fetch_articles.fetch_articles.generate_article_id")
@patch("ingest_articles.fetch_articles.fetch_articles.fetch_text")
@patch("ingest_articles.fetch_articles.fetch_articles.fetch_rss_articles")
class TestFetchArticles:
    def test_generates_article_ids(self, mock_rss, mock_text, mock_id) -> None:
        mock_rss.return_value = [
            RSSArticle(source="bbc", title="T", summary="S",
                       url="https://bbc.com/1",
                       published_at=datetime(2024, 1, 1, tzinfo=timezone.utc)),
        ]
        mock_text.return_value = "body"
        mock_id.return_value = "abc123def456ghij"

        result = fetch_articles(["bbc"], lookback_hours=12)

        assert len(result) == 1
        assert result[0].id == "abc123def456ghij"
        mock_id.assert_called_once_with("bbc", "https://bbc.com/1")

    def test_continues_on_source_error(self, mock_rss, mock_text, mock_id) -> None:
        mock_rss.side_effect = [
            Exception("Network error"),
            [RSSArticle(source="cnn", title="T", summary="S",
                        url="https://cnn.com/1",
                        published_at=datetime(2024, 1, 1, tzinfo=timezone.utc))],
        ]
        mock_text.return_value = None
        mock_id.return_value = "id1234567890abcd"

        result = fetch_articles(["failing", "cnn"], lookback_hours=12)

        assert len(result) == 1
        assert result[0].source == "cnn"

    def test_sets_ingested_at_timestamp(self, mock_rss, mock_text, mock_id) -> None:
        mock_rss.return_value = [
            RSSArticle(source="bbc", title="T", summary="S",
                       url="https://bbc.com/1",
                       published_at=datetime(2024, 1, 1, tzinfo=timezone.utc)),
        ]
        mock_text.return_value = None
        mock_id.return_value = "id1234567890abcd"

        before = datetime.now(timezone.utc)
        result = fetch_articles(["bbc"], lookback_hours=12)
        after = datetime.now(timezone.utc)

        assert before <= result[0].ingested_at <= after

    def test_processes_multiple_sources(self, mock_rss, mock_text, mock_id) -> None:
        bbc_article = RSSArticle(
            source="bbc", title="BBC", summary="",
            url="https://bbc.com/1",
            published_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        cnn_article = RSSArticle(
            source="cnn", title="CNN", summary="",
            url="https://cnn.com/1",
            published_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        mock_rss.side_effect = [[bbc_article], [cnn_article]]
        mock_text.return_value = "text"
        mock_id.side_effect = ["id_bbc_12345678", "id_cnn_12345678"]

        result = fetch_articles(["bbc", "cnn"], lookback_hours=12)

        assert len(result) == 2
        assert result[0].source == "bbc"
        assert result[1].source == "cnn"
