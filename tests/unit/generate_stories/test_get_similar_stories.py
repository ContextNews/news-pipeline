"""Tests for generate_stories.get_similar_stories module."""

from __future__ import annotations

import pytest

from generate_stories.get_similar_stories import (
    _compute_mean_embedding,
    _cosine_similarity,
    _jaccard_similarity,
)


class TestComputeMeanEmbedding:
    def test_single_vector(self) -> None:
        result = _compute_mean_embedding([[1.0, 2.0, 3.0]])
        assert result == [1.0, 2.0, 3.0]

    def test_multiple_vectors(self) -> None:
        result = _compute_mean_embedding([[1.0, 0.0], [3.0, 4.0]])
        assert result == [2.0, 2.0]

    def test_empty_list(self) -> None:
        result = _compute_mean_embedding([])
        assert result is None


class TestCosineSimilarity:
    def test_identical_vectors(self) -> None:
        result = _cosine_similarity([1.0, 2.0, 3.0], [1.0, 2.0, 3.0])
        assert result == pytest.approx(1.0)

    def test_orthogonal_vectors(self) -> None:
        result = _cosine_similarity([1.0, 0.0], [0.0, 1.0])
        assert result == pytest.approx(0.0)

    def test_opposite_vectors(self) -> None:
        result = _cosine_similarity([1.0, 0.0], [-1.0, 0.0])
        assert result == pytest.approx(-1.0)

    def test_zero_vector(self) -> None:
        result = _cosine_similarity([0.0, 0.0], [1.0, 2.0])
        assert result == 0.0


class TestJaccardSimilarity:
    def test_full_overlap(self) -> None:
        result = _jaccard_similarity({"a", "b"}, {"a", "b"})
        assert result == pytest.approx(1.0)

    def test_no_overlap(self) -> None:
        result = _jaccard_similarity({"a"}, {"b"})
        assert result == pytest.approx(0.0)

    def test_partial_overlap(self) -> None:
        result = _jaccard_similarity({"a", "b", "c"}, {"b", "c", "d"})
        assert result == pytest.approx(0.5)

    def test_both_empty(self) -> None:
        result = _jaccard_similarity(set(), set())
        assert result == 0.0
