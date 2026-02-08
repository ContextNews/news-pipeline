"""Tests for ingest_articles.fetch_articles.fetch_article_text module."""

from unittest.mock import patch, Mock

import pytest

from ingest_articles.fetch_articles.fetch_article_text import (
    fetch_article_text,
    fetch_with_trafilatura,
    fetch_with_readability,
)


class TestFetchArticleText:
    @patch("ingest_articles.fetch_articles.fetch_article_text.fetch_with_trafilatura")
    def test_returns_trafilatura_result(self, mock_traf) -> None:
        mock_traf.return_value = "Trafilatura text"
        assert fetch_article_text("https://example.com") == "Trafilatura text"

    @patch("ingest_articles.fetch_articles.fetch_article_text.fetch_with_readability")
    @patch("ingest_articles.fetch_articles.fetch_article_text.fetch_with_trafilatura")
    def test_falls_back_to_readability_on_none(self, mock_traf, mock_read) -> None:
        mock_traf.return_value = None
        mock_read.return_value = "Readability text"
        assert fetch_article_text("https://example.com") == "Readability text"

    @patch("ingest_articles.fetch_articles.fetch_article_text.fetch_with_readability")
    @patch("ingest_articles.fetch_articles.fetch_article_text.fetch_with_trafilatura")
    def test_falls_back_to_readability_on_exception(self, mock_traf, mock_read) -> None:
        mock_traf.side_effect = Exception("fail")
        mock_read.return_value = "Readability text"
        assert fetch_article_text("https://example.com") == "Readability text"

    @patch("ingest_articles.fetch_articles.fetch_article_text.fetch_with_readability")
    @patch("ingest_articles.fetch_articles.fetch_article_text.fetch_with_trafilatura")
    def test_returns_none_when_both_fail(self, mock_traf, mock_read) -> None:
        mock_traf.return_value = None
        mock_read.return_value = None
        assert fetch_article_text("https://example.com") is None

    @patch("ingest_articles.fetch_articles.fetch_article_text.fetch_with_readability")
    @patch("ingest_articles.fetch_articles.fetch_article_text.fetch_with_trafilatura")
    def test_returns_none_when_readability_raises(self, mock_traf, mock_read) -> None:
        mock_traf.return_value = None
        mock_read.side_effect = Exception("fail")
        assert fetch_article_text("https://example.com") is None


class TestFetchWithTrafilatura:
    @patch("ingest_articles.fetch_articles.fetch_article_text.trafilatura")
    def test_returns_extracted_text(self, mock_traf) -> None:
        mock_traf.fetch_url.return_value = "<html>content</html>"
        mock_traf.extract.return_value = "Extracted text"
        result = fetch_with_trafilatura("https://example.com")
        assert result == "Extracted text"
        mock_traf.fetch_url.assert_called_once_with("https://example.com")

    @patch("ingest_articles.fetch_articles.fetch_article_text.trafilatura")
    def test_returns_none_when_fetch_fails(self, mock_traf) -> None:
        mock_traf.fetch_url.return_value = None
        assert fetch_with_trafilatura("https://example.com") is None


class TestFetchWithReadability:
    @patch("ingest_articles.fetch_articles.fetch_article_text.lxml_html")
    @patch("ingest_articles.fetch_articles.fetch_article_text.Document")
    @patch("ingest_articles.fetch_articles.fetch_article_text.requests")
    def test_returns_extracted_text(self, mock_req, mock_doc, mock_lxml) -> None:
        mock_response = Mock()
        mock_response.text = "<html><body><p>Content</p></body></html>"
        mock_req.get.return_value = mock_response
        mock_doc.return_value.summary.return_value = "<p>Content</p>"
        mock_tree = Mock()
        mock_tree.text_content.return_value = "Content"
        mock_lxml.fromstring.return_value = mock_tree

        result = fetch_with_readability("https://example.com")
        assert result == "Content"

    @patch("ingest_articles.fetch_articles.fetch_article_text.requests")
    def test_raises_on_http_error(self, mock_req) -> None:
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("404")
        mock_req.get.return_value = mock_response

        with pytest.raises(Exception, match="404"):
            fetch_with_readability("https://example.com")
