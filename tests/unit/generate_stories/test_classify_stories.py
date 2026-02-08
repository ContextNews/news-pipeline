"""Tests for generate_stories.classify_stories module."""

from __future__ import annotations

from unittest.mock import patch, MagicMock

from generate_stories.classify_stories import classify_stories


class TestClassifyStories:
    @patch("generate_stories.classify_stories.Cronkite")
    def test_classifies_stories(self, mock_cronkite_cls) -> None:
        mock_cronkite = MagicMock()
        mock_cronkite.classify_stories.return_value = [
            {"id": "s1", "topics": ["politics", "economy"]},
            {"id": "s2", "topics": ["technology"]},
        ]
        mock_cronkite_cls.return_value = mock_cronkite

        stories = [
            {"id": "s1", "title": "Title 1", "summary": "Summary 1"},
            {"id": "s2", "title": "Title 2", "summary": "Summary 2"},
        ]
        result = classify_stories(stories)

        assert len(result) == 2
        assert result[0].story_id == "s1"
        assert result[0].topics == ["politics", "economy"]
        assert result[1].story_id == "s2"

    def test_empty_input_returns_empty(self) -> None:
        assert classify_stories([]) == []

    @patch("generate_stories.classify_stories.Cronkite")
    def test_filters_missing_id_or_topics(self, mock_cronkite_cls) -> None:
        mock_cronkite = MagicMock()
        mock_cronkite.classify_stories.return_value = [
            {"id": None, "topics": ["politics"]},  # missing id
            {"id": "s1", "topics": []},  # empty topics
            {"id": "s2", "topics": ["tech"]},  # valid
        ]
        mock_cronkite_cls.return_value = mock_cronkite

        stories = [{"id": "s1", "title": "T", "summary": "S"}]
        result = classify_stories(stories)

        assert len(result) == 1
        assert result[0].story_id == "s2"
