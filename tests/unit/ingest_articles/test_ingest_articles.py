"""Tests for ingest_articles.ingest_articles orchestration."""

from unittest.mock import patch

from ingest_articles.models import CleanedArticle, ResolvedArticle


class TestIngestArticles:
    @patch("ingest_articles.ingest_articles.clean")
    @patch("ingest_articles.ingest_articles.fetch_articles")
    def test_happy_path_returns_cleaned_articles(self, mock_fetch, mock_clean) -> None:
        raw = [ResolvedArticle(
            id="abc", source="bbc", title="T", summary="S",
            url="https://example.com", published_at=None, ingested_at=None, text="body",
        )]
        cleaned = [CleanedArticle(
            id="abc", source="bbc", title="T", summary="S",
            url="https://example.com", published_at=None, ingested_at=None, text="body",
        )]
        mock_fetch.return_value = raw
        mock_clean.return_value = cleaned

        from ingest_articles.ingest_articles import ingest_articles
        result = ingest_articles(["bbc"], lookback_hours=12)

        assert result == cleaned
        mock_fetch.assert_called_once_with(["bbc"], 12)
        mock_clean.assert_called_once_with(raw)

    @patch("ingest_articles.ingest_articles.clean")
    @patch("ingest_articles.ingest_articles.fetch_articles")
    def test_empty_fetch_returns_empty(self, mock_fetch, mock_clean) -> None:
        mock_fetch.return_value = []

        from ingest_articles.ingest_articles import ingest_articles
        result = ingest_articles(["bbc"], lookback_hours=12)

        assert result == []
        mock_clean.assert_not_called()

    @patch("ingest_articles.ingest_articles.clean")
    @patch("ingest_articles.ingest_articles.fetch_articles")
    def test_empty_clean_returns_empty(self, mock_fetch, mock_clean) -> None:
        mock_fetch.return_value = [ResolvedArticle(
            id="abc", source="bbc", title="T", summary="S",
            url="https://example.com", published_at=None, ingested_at=None, text="body",
        )]
        mock_clean.return_value = []

        from ingest_articles.ingest_articles import ingest_articles
        result = ingest_articles(["bbc"], lookback_hours=12)

        assert result == []
