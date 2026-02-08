"""Tests for ingest_articles.clean_articles.clean module."""

from datetime import datetime, timezone

from ingest_articles.clean_articles.clean import clean, clean_text


class TestCleanText:
    def test_strips_html_tags(self) -> None:
        assert clean_text("<h1>Title</h1> <p>Body</p>") == "Title Body"

    def test_removes_escaped_quotes(self) -> None:
        assert clean_text('He said \\"hello\\"') == 'He said "hello"'

    def test_collapses_whitespace(self) -> None:
        assert clean_text("multiple   spaces   here") == "multiple spaces here"

    def test_strips_html_escapes_and_whitespace(self) -> None:
        raw = ' <h1>Title</h1>  \n  \\"quoted\\"  '
        assert clean_text(raw) == 'Title "quoted"'

    def test_none_returns_none(self) -> None:
        assert clean_text(None) is None

    def test_empty_string_returns_none(self) -> None:
        assert clean_text("") is None

    def test_whitespace_only_returns_none(self) -> None:
        assert clean_text("   \n\t ") is None


class TestClean:
    def test_cleans_article_fields(self) -> None:
        raw_articles = [
            {
                "id": "abc123",
                "source": "bbc",
                "title": "<h1> Hello </h1>",
                "summary": 'Summary with \\"quotes\\" and   extra   spaces',
                "url": "https://example.com/article",
                "published_at": "2024-01-01T12:00:00Z",
                "ingested_at": datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc),
                "text": "<p>Body</p>\n\nMore",
            }
        ]
        result = clean(raw_articles)

        assert len(result) == 1
        cleaned = result[0]
        assert cleaned.id == "abc123"
        assert cleaned.title == "Hello"
        assert cleaned.summary == 'Summary with "quotes" and extra spaces'
        assert cleaned.text == "Body More"
        assert cleaned.published_at == datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    def test_skips_article_with_missing_id(self) -> None:
        raw_articles = [{"url": "https://example.com"}]
        assert clean(raw_articles) == []

    def test_skips_article_with_missing_url(self) -> None:
        raw_articles = [{"id": "abc"}]
        assert clean(raw_articles) == []

    def test_empty_input_returns_empty(self) -> None:
        assert clean([]) == []

    def test_none_input_returns_empty(self) -> None:
        assert clean(None) == []
