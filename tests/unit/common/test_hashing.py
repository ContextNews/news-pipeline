"""Tests for common.hashing module."""

from common.hashing import generate_article_id


class TestGenerateArticleId:
    def test_deterministic_output(self) -> None:
        result1 = generate_article_id("bbc", "https://bbc.com/article")
        result2 = generate_article_id("bbc", "https://bbc.com/article")
        assert result1 == result2

    def test_returns_16_char_hex_string(self) -> None:
        result = generate_article_id("bbc", "https://bbc.com/article")
        assert len(result) == 16
        assert all(c in "0123456789abcdef" for c in result)

    def test_different_source_produces_different_id(self) -> None:
        result1 = generate_article_id("bbc", "https://example.com/article")
        result2 = generate_article_id("cnn", "https://example.com/article")
        assert result1 != result2

    def test_different_url_produces_different_id(self) -> None:
        result1 = generate_article_id("bbc", "https://bbc.com/article1")
        result2 = generate_article_id("bbc", "https://bbc.com/article2")
        assert result1 != result2
