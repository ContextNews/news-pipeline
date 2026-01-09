"""Tests for fetch_article_text module."""

import pytest
from unittest.mock import patch, Mock

from news_pipeline.ingest.fetch_article_text import (
    fetch_article_text,
    fetch_with_trafilatura,
    fetch_with_readability,
)


class TestFetchArticleText:
    """Tests for the main fetch_article_text function."""

    @patch("news_pipeline.ingest.fetch_article_text.fetch_with_trafilatura")
    def test_returns_trafilatura_result_when_successful(self, mock_trafilatura):
        mock_trafilatura.return_value = "Article content from trafilatura"

        result = fetch_article_text("https://example.com/article")

        assert result.text == "Article content from trafilatura"
        assert result.method == "trafilatura"
        assert result.error is None

    @patch("news_pipeline.ingest.fetch_article_text.fetch_with_readability")
    @patch("news_pipeline.ingest.fetch_article_text.fetch_with_trafilatura")
    def test_falls_back_to_readability_when_trafilatura_returns_none(
        self, mock_trafilatura, mock_readability
    ):
        mock_trafilatura.return_value = None
        mock_readability.return_value = "Article content from readability"

        result = fetch_article_text("https://example.com/article")

        assert result.text == "Article content from readability"
        assert result.method == "readability"
        assert result.error is None

    @patch("news_pipeline.ingest.fetch_article_text.fetch_with_readability")
    @patch("news_pipeline.ingest.fetch_article_text.fetch_with_trafilatura")
    def test_falls_back_to_readability_when_trafilatura_raises(
        self, mock_trafilatura, mock_readability
    ):
        mock_trafilatura.side_effect = Exception("trafilatura error")
        mock_readability.return_value = "Article content from readability"

        result = fetch_article_text("https://example.com/article")

        assert result.text == "Article content from readability"
        assert result.method == "readability"

    @patch("news_pipeline.ingest.fetch_article_text.fetch_with_readability")
    @patch("news_pipeline.ingest.fetch_article_text.fetch_with_trafilatura")
    def test_returns_error_when_both_methods_fail(
        self, mock_trafilatura, mock_readability
    ):
        mock_trafilatura.return_value = None
        mock_readability.return_value = None

        result = fetch_article_text("https://example.com/article")

        assert result.text is None
        assert result.method is None
        assert result.error == "All extraction methods failed"

    @patch("news_pipeline.ingest.fetch_article_text.fetch_with_readability")
    @patch("news_pipeline.ingest.fetch_article_text.fetch_with_trafilatura")
    def test_returns_error_when_readability_raises(
        self, mock_trafilatura, mock_readability
    ):
        mock_trafilatura.return_value = None
        mock_readability.side_effect = Exception("readability error")

        result = fetch_article_text("https://example.com/article")

        assert result.text is None
        assert result.error == "readability error"


class TestFetchWithTrafilatura:
    """Tests for fetch_with_trafilatura function."""

    @patch("news_pipeline.ingest.fetch_article_text.trafilatura")
    def test_returns_extracted_text(self, mock_trafilatura):
        mock_trafilatura.fetch_url.return_value = "<html>content</html>"
        mock_trafilatura.extract.return_value = "Extracted text"

        result = fetch_with_trafilatura("https://example.com/article")

        assert result == "Extracted text"
        mock_trafilatura.fetch_url.assert_called_once_with("https://example.com/article")

    @patch("news_pipeline.ingest.fetch_article_text.trafilatura")
    def test_returns_none_when_fetch_fails(self, mock_trafilatura):
        mock_trafilatura.fetch_url.return_value = None

        result = fetch_with_trafilatura("https://example.com/article")

        assert result is None


class TestFetchWithReadability:
    """Tests for fetch_with_readability function."""

    @patch("news_pipeline.ingest.fetch_article_text.requests")
    def test_returns_extracted_text(self, mock_requests):
        mock_response = Mock()
        mock_response.text = """
        <html>
            <body>
                <article>
                    <h1>Title</h1>
                    <p>First paragraph.</p>
                    <p>Second paragraph.</p>
                </article>
            </body>
        </html>
        """
        mock_requests.get.return_value = mock_response

        result = fetch_with_readability("https://example.com/article")

        assert result is not None
        mock_requests.get.assert_called_once_with("https://example.com/article", timeout=10)

    @patch("news_pipeline.ingest.fetch_article_text.requests")
    def test_raises_on_http_error(self, mock_requests):
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("404 Not Found")
        mock_requests.get.return_value = mock_response

        with pytest.raises(Exception, match="404 Not Found"):
            fetch_with_readability("https://example.com/article")
