"""Tests for generate_stories.resolve_story_location module."""

from __future__ import annotations

from generate_stories.resolve_story_location import (
    resolve_story_location,
    resolve_story_persons,
)


class TestResolveStoryLocation:
    def test_most_common_qid(self) -> None:
        article_locations = {
            "a1": ["Q84", "Q142"],
            "a2": ["Q84"],
        }
        result = resolve_story_location(["a1", "a2"], article_locations)
        assert result == "Q84"

    def test_tie_breaking_alphabetical(self) -> None:
        article_locations = {
            "a1": ["Q200"],
            "a2": ["Q100"],
        }
        result = resolve_story_location(["a1", "a2"], article_locations)
        assert result == "Q100"

    def test_empty_article_ids(self) -> None:
        assert resolve_story_location([], {"a1": ["Q1"]}) is None

    def test_no_locations_returns_none(self) -> None:
        assert resolve_story_location(["a1"], {}) is None


class TestResolveStoryPersons:
    def test_returns_all_unique_person_qids(self) -> None:
        article_persons = {
            "a1": ["Q1", "Q2"],
            "a2": ["Q3"],
        }
        result = resolve_story_persons(["a1", "a2"], article_persons)
        assert result == ["Q1", "Q2", "Q3"]

    def test_deduplicates_across_articles(self) -> None:
        article_persons = {
            "a1": ["Q1", "Q2"],
            "a2": ["Q2", "Q3"],
        }
        result = resolve_story_persons(["a1", "a2"], article_persons)
        assert result == ["Q1", "Q2", "Q3"]

    def test_returns_empty_for_no_persons(self) -> None:
        article_persons: dict[str, list[str]] = {"a1": []}
        result = resolve_story_persons(["a1"], article_persons)
        assert result == []

    def test_returns_empty_for_empty_article_ids(self) -> None:
        assert resolve_story_persons([], {"a1": ["Q1"]}) == []

    def test_ignores_articles_not_in_persons_map(self) -> None:
        article_persons = {"a1": ["Q1"]}
        result = resolve_story_persons(["a1", "a2"], article_persons)
        assert result == ["Q1"]

    def test_returns_sorted_qids(self) -> None:
        article_persons = {"a1": ["Q10", "Q2", "Q1"]}
        result = resolve_story_persons(["a1"], article_persons)
        assert result == ["Q1", "Q10", "Q2"]
