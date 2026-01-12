"""Unit tests for cleaning stage."""

from datetime import datetime, timezone

from news_pipeline.stage2_clean.clean import clean, clean_text


class TestCleanText:
    def test_strips_html_escapes_and_whitespace(self):
        raw = ' <h1>Title</h1>  \n  \\"quoted\\"  '

        result = clean_text(raw)

        assert result == 'Title "quoted"'

    def test_returns_none_for_empty_or_none(self):
        assert clean_text(None) is None
        assert clean_text("") is None
        assert clean_text("   \n\t ") is None


class TestClean:
    def test_cleans_article_fields(self):
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
        assert cleaned.source == "bbc"
        assert cleaned.url == "https://example.com/article"
        assert cleaned.title == "Hello"
        assert cleaned.summary == 'Summary with "quotes" and extra spaces'
        assert cleaned.text == "Body More"
        assert cleaned.published_at == datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert cleaned.ingested_at == datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc)

    def test_missing_optional_fields(self):
        raw_articles = [
            {
                "id": "xyz789",
                "source": "cnn",
                "url": "https://cnn.com/article",
                "published_at": "2024-02-01T10:00:00+00:00",
                "ingested_at": "2024-02-01T10:05:00Z",
            }
        ]

        result = clean(raw_articles)

        assert len(result) == 1
        cleaned = result[0]
        assert cleaned.title == ""
        assert cleaned.summary == ""
        assert cleaned.text is None
        assert cleaned.published_at == datetime(2024, 2, 1, 10, 0, 0, tzinfo=timezone.utc)
        assert cleaned.ingested_at == datetime(2024, 2, 1, 10, 5, 0, tzinfo=timezone.utc)

    def test_returns_empty_list_on_empty_input(self):
        assert clean([]) == []
        assert clean(None) == []
