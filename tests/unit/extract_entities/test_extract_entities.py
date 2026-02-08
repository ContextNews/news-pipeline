"""Tests for extract_entities.extract_entities module."""

from unittest.mock import patch, MagicMock

from extract_entities.extract_entities import (
    _apply_word_limit,
    _normalize_entity_name,
    _contains_alias,
    _normalize_gpe_name,
    _normalize_country_name,
    _collect_article_texts,
    extract_entities,
)


class TestApplyWordLimit:
    def test_no_limit_returns_text(self) -> None:
        assert _apply_word_limit("one two three", None) == "one two three"

    def test_under_limit_returns_text(self) -> None:
        assert _apply_word_limit("one two", 5) == "one two"

    def test_over_limit_truncates(self) -> None:
        result = _apply_word_limit("one two three four five six", 3)
        assert result == "one two three"

    def test_empty_text_returns_empty(self) -> None:
        assert _apply_word_limit("", 5) == ""


class TestNormalizeEntityName:
    def test_uppercases(self) -> None:
        assert _normalize_entity_name("John Smith") == "JOHN SMITH"

    def test_removes_possessive_s(self) -> None:
        assert _normalize_entity_name("Biden's") == "BIDEN"

    def test_removes_curly_possessive(self) -> None:
        assert _normalize_entity_name("Trump\u2019s") == "TRUMP"

    def test_removes_trailing_punctuation(self) -> None:
        assert _normalize_entity_name("London,") == "LONDON"

    def test_removes_apos_s(self) -> None:
        assert _normalize_entity_name("China&apos;s") == "CHINA"


class TestContainsAlias:
    def test_substring_match(self) -> None:
        assert _contains_alias("BIDEN", "JOE BIDEN") is True

    def test_no_match(self) -> None:
        assert _contains_alias("TRUMP", "JOE BIDEN") is False

    def test_same_name_returns_false(self) -> None:
        assert _contains_alias("BIDEN", "BIDEN") is False

    def test_empty_short_name_returns_false(self) -> None:
        assert _contains_alias("", "JOE BIDEN") is False

    def test_empty_long_name_returns_false(self) -> None:
        assert _contains_alias("BIDEN", "") is False


class TestNormalizeGpeName:
    def test_removes_the_prefix(self) -> None:
        assert _normalize_gpe_name("THE UNITED STATES") == "UNITED STATES"

    def test_strips_punctuation(self) -> None:
        assert _normalize_gpe_name("NEW YORK,") == "NEW YORK"

    def test_empty_returns_empty(self) -> None:
        assert _normalize_gpe_name("") == ""


class TestNormalizeCountryName:
    def test_uk_maps_to_united_kingdom(self) -> None:
        assert _normalize_country_name("UK") == "UNITED KINGDOM"

    def test_britain_maps_to_united_kingdom(self) -> None:
        assert _normalize_country_name("BRITAIN") == "UNITED KINGDOM"

    def test_pycountry_lookup(self) -> None:
        result = _normalize_country_name("FRANCE")
        assert result == "FRANCE"

    def test_unknown_returns_none(self) -> None:
        assert _normalize_country_name("XYZLAND") is None

    def test_empty_returns_none(self) -> None:
        assert _normalize_country_name("") is None


class TestCollectArticleTexts:
    def test_combines_fields(self) -> None:
        articles = [{"id": "a1", "title": "T", "summary": "S", "text": "B"}]
        rows = _collect_article_texts(articles, None)
        assert len(rows) == 1
        assert "T" in rows[0]["combined"]
        assert "S" in rows[0]["combined"]
        assert "B" in rows[0]["combined"]

    def test_applies_word_limit(self) -> None:
        articles = [{"id": "a1", "title": "one two three four five", "summary": "", "text": ""}]
        rows = _collect_article_texts(articles, 3)
        assert len(rows[0]["combined"].split()) <= 3

    def test_skips_missing_id(self) -> None:
        articles = [{"title": "T", "summary": "S", "text": "B"}]
        assert _collect_article_texts(articles, None) == []


class TestExtractEntities:
    @patch("extract_entities.extract_entities.spacy")
    def test_full_pipeline(self, mock_spacy) -> None:
        mock_nlp = MagicMock()

        # Create a mock entity
        mock_ent = MagicMock()
        mock_ent.text = "London"
        mock_ent.label_ = "GPE"

        # Mock doc returned by nlp.pipe
        mock_doc = MagicMock()
        mock_doc.ents = [mock_ent]

        # Mock doc returned by nlp() for title
        mock_title_doc = MagicMock()
        mock_title_ent = MagicMock()
        mock_title_ent.text = "London"
        mock_title_ent.label_ = "GPE"
        mock_title_doc.ents = [mock_title_ent]

        mock_nlp.pipe.return_value = iter([mock_doc])
        mock_nlp.return_value = mock_title_doc
        mock_spacy.load.return_value = mock_nlp

        articles = [{"id": "a1", "title": "London news", "summary": "", "text": ""}]
        result = extract_entities(articles, model="en_core_web_sm")

        assert len(result) >= 1
        assert result[0].article_id == "a1"
        assert result[0].entity_type == "GPE"

    def test_empty_input_returns_empty(self) -> None:
        assert extract_entities([], model="en_core_web_sm") == []
