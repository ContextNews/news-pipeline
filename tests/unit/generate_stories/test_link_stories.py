"""Tests for generate_stories.link_stories module."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

from generate_stories.link_stories import link_stories


class TestLinkStories:
    """Tests for the link_stories function."""

    @patch("generate_stories.link_stories.Cronkite")
    @patch("generate_stories.link_stories._load_stories_for_llm")
    @patch("generate_stories.link_stories.get_similar_stories")
    def test_links_matching_stories(
        self, mock_get_similar, mock_load_stories, mock_cronkite_cls
    ) -> None:
        """Stories matched by LLM are returned as (yesterday_id, today_id) pairs."""
        today_stories = [
            {"story_id": "today_1", "title": "T1", "summary": "S1", "key_points": ["k1"]},
            {"story_id": "today_2", "title": "T2", "summary": "S2", "key_points": ["k2"]},
        ]

        mock_get_similar.side_effect = [
            [{"story_id": "yest_a", "title": "A", "summary": "SA", "similarity_score": 0.9}],
            [{"story_id": "yest_b", "title": "B", "summary": "SB", "similarity_score": 0.8}],
        ]

        mock_load_stories.return_value = [
            {"story_id": "yest_a", "title": "A", "summary": "SA", "key_points": ["ka"]},
            {"story_id": "yest_b", "title": "B", "summary": "SB", "key_points": ["kb"]},
        ]

        mock_cronkite = MagicMock()
        mock_cronkite_cls.return_value = mock_cronkite
        mock_cronkite.group_stories.return_value = [
            {"group_a_index": 0, "group_b_index": 0},
        ]

        result = link_stories(today_stories, date(2024, 1, 1))

        assert result == [("yest_a", "today_1")]
        mock_cronkite.group_stories.assert_called_once()
        call_kwargs = mock_cronkite.group_stories.call_args
        assert len(call_kwargs.kwargs["group_a"]) == 2
        assert len(call_kwargs.kwargs["group_b"]) == 2

    @patch("generate_stories.link_stories.get_similar_stories")
    def test_no_candidates_returns_empty(self, mock_get_similar) -> None:
        """When no similar stories found, returns empty list."""
        today_stories = [
            {"story_id": "today_1", "title": "T1", "summary": "S1", "key_points": []},
        ]
        mock_get_similar.return_value = []

        result = link_stories(today_stories, date(2024, 1, 1))

        assert result == []

    @patch("generate_stories.link_stories.Cronkite")
    @patch("generate_stories.link_stories._load_stories_for_llm")
    @patch("generate_stories.link_stories.get_similar_stories")
    def test_no_llm_matches_returns_empty(
        self, mock_get_similar, mock_load_stories, mock_cronkite_cls
    ) -> None:
        """When LLM finds no matches, returns empty list."""
        today_stories = [
            {"story_id": "today_1", "title": "T1", "summary": "S1", "key_points": []},
        ]
        mock_get_similar.return_value = [
            {"story_id": "yest_a", "title": "A", "summary": "SA", "similarity_score": 0.5},
        ]

        mock_load_stories.return_value = [
            {"story_id": "yest_a", "title": "A", "summary": "SA", "key_points": ["ka"]},
        ]

        mock_cronkite = MagicMock()
        mock_cronkite_cls.return_value = mock_cronkite
        mock_cronkite.group_stories.return_value = []

        result = link_stories(today_stories, date(2024, 1, 1))

        assert result == []

    def test_empty_today_stories_returns_empty(self) -> None:
        """Empty input returns empty list without making any calls."""
        result = link_stories([], date(2024, 1, 1))
        assert result == []

    @patch("generate_stories.link_stories.Cronkite")
    @patch("generate_stories.link_stories._load_stories_for_llm")
    @patch("generate_stories.link_stories.get_similar_stories")
    def test_multiple_links_returned(
        self, mock_get_similar, mock_load_stories, mock_cronkite_cls
    ) -> None:
        """Multiple LLM matches produce multiple link pairs."""
        today_stories = [
            {"story_id": "today_1", "title": "T1", "summary": "S1", "key_points": []},
            {"story_id": "today_2", "title": "T2", "summary": "S2", "key_points": []},
        ]

        mock_get_similar.side_effect = [
            [{"story_id": "yest_a", "title": "A", "summary": "SA", "similarity_score": 0.9}],
            [{"story_id": "yest_b", "title": "B", "summary": "SB", "similarity_score": 0.8}],
        ]

        mock_load_stories.return_value = [
            {"story_id": "yest_a", "title": "A", "summary": "SA", "key_points": ["ka"]},
            {"story_id": "yest_b", "title": "B", "summary": "SB", "key_points": ["kb"]},
        ]

        mock_cronkite = MagicMock()
        mock_cronkite_cls.return_value = mock_cronkite
        mock_cronkite.group_stories.return_value = [
            {"group_a_index": 0, "group_b_index": 0},
            {"group_a_index": 1, "group_b_index": 1},
        ]

        result = link_stories(today_stories, date(2024, 1, 1))

        assert len(result) == 2
        assert ("yest_a", "today_1") in result
        assert ("yest_b", "today_2") in result

    @patch("generate_stories.link_stories.Cronkite")
    @patch("generate_stories.link_stories._load_stories_for_llm")
    @patch("generate_stories.link_stories.get_similar_stories")
    def test_deduplicates_candidate_ids(
        self, mock_get_similar, mock_load_stories, mock_cronkite_cls
    ) -> None:
        """Same candidate returned for multiple today stories is only loaded once."""
        today_stories = [
            {"story_id": "today_1", "title": "T1", "summary": "S1", "key_points": []},
            {"story_id": "today_2", "title": "T2", "summary": "S2", "key_points": []},
        ]

        # Both today stories have the same candidate
        mock_get_similar.side_effect = [
            [{"story_id": "yest_a", "title": "A", "summary": "SA", "similarity_score": 0.9}],
            [{"story_id": "yest_a", "title": "A", "summary": "SA", "similarity_score": 0.7}],
        ]

        mock_load_stories.return_value = [
            {"story_id": "yest_a", "title": "A", "summary": "SA", "key_points": ["ka"]},
        ]

        mock_cronkite = MagicMock()
        mock_cronkite_cls.return_value = mock_cronkite
        mock_cronkite.group_stories.return_value = [
            {"group_a_index": 0, "group_b_index": 1},
        ]

        result = link_stories(today_stories, date(2024, 1, 1))

        assert result == [("yest_a", "today_2")]
        # group_a should have only 1 story (deduplicated)
        call_kwargs = mock_cronkite.group_stories.call_args
        assert len(call_kwargs.kwargs["group_a"]) == 1
