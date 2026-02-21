"""Tests for classify_articles.classify_articles module."""

from dataclasses import dataclass
from unittest.mock import patch, MagicMock

from classify_articles.classify_articles import _build_input_text, classify_articles


class TestBuildInputText:
    def test_combines_all_fields(self) -> None:
        article = {"title": "Title", "summary": "Summary", "text": "Body text"}
        result = _build_input_text(article)
        assert result == "Title Summary Body text"

    def test_skips_none_fields(self) -> None:
        article = {"title": "Title", "summary": None, "text": "Body"}
        result = _build_input_text(article)
        assert result == "Title Body"

    def test_skips_missing_fields(self) -> None:
        article = {"title": "Title"}
        result = _build_input_text(article)
        assert result == "Title"

    def test_word_limit(self) -> None:
        article = {"title": "one two three four five six seven eight nine ten"}
        result = _build_input_text(article, word_limit=5)
        assert result == "one two three four five"

    def test_word_limit_not_exceeded(self) -> None:
        article = {"title": "one two three"}
        result = _build_input_text(article, word_limit=10)
        assert result == "one two three"

    def test_empty_fields_returns_empty(self) -> None:
        article = {"title": None, "summary": None, "text": None}
        result = _build_input_text(article)
        assert result == ""

    def test_handles_objects(self) -> None:
        @dataclass
        class Article:
            title: str
            summary: str
            text: str

        article = Article(title="Title", summary="Summary", text="Body")
        result = _build_input_text(article)
        assert result == "Title Summary Body"


class TestClassifyArticles:
    @patch("classify_articles.classify_articles.AutoTokenizer")
    @patch("classify_articles.classify_articles.hf_pipeline")
    def test_happy_path(self, mock_pipeline, mock_tokenizer) -> None:
        mock_classifier = MagicMock()
        mock_classifier.return_value = [
            [
                {"label": "politics", "score": 0.9},
                {"label": "sports", "score": 0.1},
                {"label": "technology", "score": 0.7},
            ],
        ]
        mock_pipeline.return_value = mock_classifier

        articles = [{"id": "a1", "title": "Political tech news", "summary": "S", "text": "T"}]
        result = classify_articles(articles, model="test-model")

        assert len(result) == 1
        assert result[0].article_id == "a1"
        assert "politics" in result[0].topics
        assert "technology" in result[0].topics
        assert "sports" not in result[0].topics
        assert result[0].scores["politics"] == 0.9
        assert result[0].scores["sports"] == 0.1
        assert result[0].scores["technology"] == 0.7

    @patch("classify_articles.classify_articles.AutoTokenizer")
    @patch("classify_articles.classify_articles.hf_pipeline")
    def test_custom_threshold(self, mock_pipeline, mock_tokenizer) -> None:
        mock_classifier = MagicMock()
        mock_classifier.return_value = [
            [
                {"label": "politics", "score": 0.9},
                {"label": "sports", "score": 0.3},
                {"label": "technology", "score": 0.7},
            ],
        ]
        mock_pipeline.return_value = mock_classifier

        articles = [{"id": "a1", "title": "News", "summary": "S", "text": "T"}]
        result = classify_articles(articles, model="test-model", threshold=0.8)

        assert result[0].topics == ["politics"]

    @patch("classify_articles.classify_articles.AutoTokenizer")
    @patch("classify_articles.classify_articles.hf_pipeline")
    def test_no_labels_above_threshold(self, mock_pipeline, mock_tokenizer) -> None:
        mock_classifier = MagicMock()
        mock_classifier.return_value = [
            [
                {"label": "politics", "score": 0.1},
                {"label": "sports", "score": 0.2},
            ],
        ]
        mock_pipeline.return_value = mock_classifier

        articles = [{"id": "a1", "title": "News", "summary": "S", "text": "T"}]
        result = classify_articles(articles, model="test-model")

        assert result[0].topics == []
        assert result[0].scores == {"politics": 0.1, "sports": 0.2}

    @patch("classify_articles.classify_articles.AutoTokenizer")
    @patch("classify_articles.classify_articles.hf_pipeline")
    def test_filters_missing_id(self, mock_pipeline, mock_tokenizer) -> None:
        mock_classifier = MagicMock()
        mock_classifier.return_value = [
            [{"label": "politics", "score": 0.9}],
        ]
        mock_pipeline.return_value = mock_classifier

        articles = [
            {"id": None, "title": "No ID", "text": "Body"},
            {"id": "a1", "title": "Valid", "text": "Body"},
        ]
        result = classify_articles(articles, model="test-model")

        assert len(result) == 1
        assert result[0].article_id == "a1"

    @patch("classify_articles.classify_articles.AutoTokenizer")
    @patch("classify_articles.classify_articles.hf_pipeline")
    def test_filters_empty_text(self, mock_pipeline, mock_tokenizer) -> None:
        mock_classifier = MagicMock()
        mock_classifier.return_value = [
            [{"label": "politics", "score": 0.9}],
        ]
        mock_pipeline.return_value = mock_classifier

        articles = [
            {"id": "a1", "title": None, "summary": None, "text": None},
            {"id": "a2", "title": "Valid", "text": "Body"},
        ]
        result = classify_articles(articles, model="test-model")

        assert len(result) == 1
        assert result[0].article_id == "a2"

    def test_empty_input_returns_empty(self) -> None:
        result = classify_articles([], model="test-model")
        assert result == []

    @patch("classify_articles.classify_articles.AutoTokenizer")
    @patch("classify_articles.classify_articles.hf_pipeline")
    def test_all_invalid_returns_empty(self, mock_pipeline, mock_tokenizer) -> None:
        articles = [
            {"id": None, "title": "No ID"},
            {"id": "a1", "title": None, "summary": None, "text": None},
        ]
        result = classify_articles(articles, model="test-model")

        assert result == []
        mock_pipeline.assert_not_called()

    @patch("classify_articles.classify_articles.AutoTokenizer")
    @patch("classify_articles.classify_articles.hf_pipeline")
    def test_pipeline_called_with_correct_args(self, mock_pipeline, mock_tokenizer) -> None:
        mock_classifier = MagicMock()
        mock_classifier.return_value = [
            [{"label": "politics", "score": 0.9}],
        ]
        mock_pipeline.return_value = mock_classifier

        mock_tok_instance = MagicMock()
        mock_tokenizer.from_pretrained.return_value = mock_tok_instance

        articles = [{"id": "a1", "title": "News", "text": "Body"}]
        classify_articles(articles, model="my-model", batch_size=16)

        mock_pipeline.assert_called_once_with("text-classification", model="my-model", tokenizer=mock_tok_instance, top_k=None)
        mock_classifier.assert_called_once_with(["News Body"], batch_size=16, truncation=True)
