"""Tests for compute_embeddings.compute_embeddings module."""

from unittest.mock import patch, MagicMock

import numpy as np

from compute_embeddings.compute_embeddings import (
    _split_sentences,
    _build_text_to_embed,
    compute_embeddings,
)


class TestSplitSentences:
    def test_splits_on_punctuation(self) -> None:
        result = _split_sentences("First sentence. Second sentence! Third?")
        assert result == ["First sentence.", "Second sentence!", "Third?"]

    def test_empty_text_returns_empty(self) -> None:
        assert _split_sentences("") == []

    def test_none_returns_empty(self) -> None:
        assert _split_sentences(None) == []

    def test_no_punctuation(self) -> None:
        result = _split_sentences("No punctuation here")
        assert result == ["No punctuation here"]


class TestBuildTextToEmbed:
    def test_combines_all_fields(self) -> None:
        article = {"title": "Title", "summary": "Summary", "text": "Body text"}
        result = _build_text_to_embed(article, True, True, True, None)
        assert result == "Title Summary Body text"

    def test_title_only(self) -> None:
        article = {"title": "Title", "summary": "Summary", "text": "Body"}
        result = _build_text_to_embed(article, True, False, False, None)
        assert result == "Title"

    def test_word_limit_truncation(self) -> None:
        article = {"title": "One two three four five six seven eight nine ten.", "summary": "", "text": ""}
        result = _build_text_to_embed(article, True, True, True, 5)
        words = result.split()
        assert len(words) <= 5

    def test_empty_fields(self) -> None:
        article = {"title": None, "summary": None, "text": None}
        result = _build_text_to_embed(article, True, True, True, None)
        assert result == ""


class TestComputeEmbeddings:
    @patch("compute_embeddings.compute_embeddings.SentenceTransformer")
    def test_computes_embeddings(self, mock_st_cls) -> None:
        mock_model = MagicMock()
        mock_model.encode.return_value = np.array([[0.1, 0.2], [0.3, 0.4]])
        mock_st_cls.return_value = mock_model

        articles = [
            {"id": "a1", "url": "http://a", "title": "T1", "summary": "S1", "text": "B1",
             "source": "bbc", "published_at": None, "ingested_at": None},
            {"id": "a2", "url": "http://b", "title": "T2", "summary": "S2", "text": "B2",
             "source": "cnn", "published_at": None, "ingested_at": None},
        ]
        result = compute_embeddings(articles, model="test-model")

        assert len(result) == 2
        assert result[0].id == "a1"
        assert result[0].embedding == [0.1, 0.2]
        assert result[1].embedding == [0.3, 0.4]

    @patch("compute_embeddings.compute_embeddings.SentenceTransformer")
    def test_filters_invalid_articles(self, mock_st_cls) -> None:
        mock_model = MagicMock()
        mock_model.encode.return_value = np.array([[0.5, 0.6]])
        mock_st_cls.return_value = mock_model

        articles = [
            {"id": None, "url": "http://a", "title": "T"},  # missing id
            {"id": "a1", "url": None, "title": "T"},  # missing url
            {"id": "a2", "url": "http://b", "title": "T2", "summary": "", "text": "",
             "source": "bbc", "published_at": None, "ingested_at": None},
        ]
        result = compute_embeddings(articles, model="test-model")

        assert len(result) == 1
        assert result[0].id == "a2"

    def test_empty_input_returns_empty(self) -> None:
        assert compute_embeddings([], model="test-model") == []
