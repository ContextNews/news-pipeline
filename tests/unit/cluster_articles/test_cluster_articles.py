"""Tests for cluster_articles.cluster_articles module."""

from unittest.mock import patch, MagicMock

import numpy as np

from cluster_articles.cluster_articles import (
    _coerce_embedding,
    _prepare_embeddings,
    cluster_articles,
)


class TestCoerceEmbedding:
    def test_none_returns_none(self) -> None:
        assert _coerce_embedding(None) is None

    def test_json_string(self) -> None:
        result = _coerce_embedding("[1.0, 2.0, 3.0]")
        assert result == [1.0, 2.0, 3.0]

    def test_invalid_json_string_returns_none(self) -> None:
        assert _coerce_embedding("not json") is None

    def test_numpy_array(self) -> None:
        arr = np.array([1.0, 2.0, 3.0])
        result = _coerce_embedding(arr)
        assert result == [1.0, 2.0, 3.0]

    def test_list(self) -> None:
        result = _coerce_embedding([1.0, 2.0])
        assert result == [1.0, 2.0]

    def test_tuple(self) -> None:
        result = _coerce_embedding((1.0, 2.0))
        assert result == [1.0, 2.0]

    def test_invalid_type_returns_none(self) -> None:
        assert _coerce_embedding(42) is None


class TestPrepareEmbeddings:
    def test_filters_invalid_embeddings(self) -> None:
        articles = [
            {"embedding": None},
            {"embedding": [1.0, 2.0]},
            {"embedding": "invalid"},
        ]
        vectors, kept = _prepare_embeddings(articles)
        assert len(kept) == 1
        assert vectors.shape == (1, 2)

    def test_empty_input(self) -> None:
        vectors, kept = _prepare_embeddings([])
        assert vectors.size == 0
        assert kept == []


class TestClusterArticles:
    @patch("cluster_articles.cluster_articles.hdbscan")
    def test_assigns_labels(self, mock_hdbscan) -> None:
        mock_clusterer = MagicMock()
        mock_clusterer.fit_predict.return_value = np.array([0, 0, 1])
        mock_hdbscan.HDBSCAN.return_value = mock_clusterer

        articles = [
            {"id": "a1", "source": "bbc", "title": "T1", "summary": "S1",
             "url": "http://a", "published_at": None, "ingested_at": None,
             "text": "B1", "embedding": [0.1, 0.2], "embedding_model": "model"},
            {"id": "a2", "source": "cnn", "title": "T2", "summary": "S2",
             "url": "http://b", "published_at": None, "ingested_at": None,
             "text": "B2", "embedding": [0.3, 0.4], "embedding_model": "model"},
            {"id": "a3", "source": "fox", "title": "T3", "summary": "S3",
             "url": "http://c", "published_at": None, "ingested_at": None,
             "text": "B3", "embedding": [0.5, 0.6], "embedding_model": "model"},
        ]
        result = cluster_articles(articles, min_cluster_size=2)

        assert len(result) == 3
        assert result[0].cluster_id == 0
        assert result[2].cluster_id == 1

    def test_empty_input_returns_empty(self) -> None:
        assert cluster_articles([]) == []

    def test_no_valid_embeddings_returns_empty(self) -> None:
        articles = [{"id": "a1", "embedding": None}]
        assert cluster_articles(articles) == []
