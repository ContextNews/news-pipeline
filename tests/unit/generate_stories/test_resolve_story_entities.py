"""Tests for generate_stories.resolve_story_entities module."""

from __future__ import annotations

from generate_stories.resolve_story_entities import (
    auto_attach_states,
    resolve_story_location,
    resolve_story_persons,
    resolve_story_states,
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


class TestResolveStoryLocationSpecificity:
    def test_city_beats_country_when_both_present(self) -> None:
        article_locations = {
            "a1": ["Q90", "Q142"],   # Paris (city) and France (country)
            "a2": ["Q142"],
        }
        location_types = {"Q90": "city", "Q142": "country"}
        result = resolve_story_location(
            ["a1", "a2"], article_locations, location_types=location_types
        )
        assert result == "Q90"

    def test_region_beats_country(self) -> None:
        article_locations = {"a1": ["Q1000", "Q142"]}
        location_types = {"Q1000": "region", "Q142": "country"}
        result = resolve_story_location(
            ["a1"], article_locations, location_types=location_types
        )
        assert result == "Q1000"

    def test_more_mentions_wins_within_same_specificity(self) -> None:
        article_locations = {
            "a1": ["Q90"],
            "a2": ["Q90"],
            "a3": ["Q64"],
        }
        location_types = {"Q90": "city", "Q64": "city"}
        result = resolve_story_location(
            ["a1", "a2", "a3"], article_locations, location_types=location_types
        )
        assert result == "Q90"

    def test_unknown_type_falls_to_back(self) -> None:
        """Typed city beats an untyped neighbour even with fewer mentions."""
        article_locations = {
            "a1": ["Q_unknown", "Q_unknown"],
            "a2": ["Q90"],   # city, just one mention
        }
        location_types = {"Q90": "city"}
        result = resolve_story_location(
            ["a1", "a2"], article_locations, location_types=location_types
        )
        assert result == "Q90"

    def test_no_location_types_falls_back_to_mention_count(self) -> None:
        """When location_types is omitted, mention count then alphabetical."""
        article_locations = {"a1": ["Q200", "Q200", "Q100"]}
        result = resolve_story_location(["a1"], article_locations)
        assert result == "Q200"


class TestResolveStoryStates:
    def test_returns_sorted_unique_state_qids(self) -> None:
        article_states = {
            "a1": ["Q142", "Q159"],
            "a2": ["Q159"],
        }
        assert resolve_story_states(["a1", "a2"], article_states) == ["Q142", "Q159"]

    def test_returns_empty_when_no_states(self) -> None:
        assert resolve_story_states(["a1"], {}) == []


class TestAutoAttachStates:
    def test_maps_city_to_containing_state(self) -> None:
        result = auto_attach_states(
            location_qids=["Q90"],   # Paris
            location_country_codes={"Q90": "FR"},
            country_to_state={"FR": "Q142"},
        )
        assert result == ["Q142"]

    def test_skips_locations_with_no_country_code(self) -> None:
        result = auto_attach_states(
            location_qids=["Q_arctic"],
            location_country_codes={"Q_arctic": None},
            country_to_state={"FR": "Q142"},
        )
        assert result == []

    def test_skips_when_country_has_no_seeded_state(self) -> None:
        result = auto_attach_states(
            location_qids=["Q_disputed"],
            location_country_codes={"Q_disputed": "XX"},
            country_to_state={"FR": "Q142"},
        )
        assert result == []

    def test_dedups_when_multiple_locations_share_country(self) -> None:
        result = auto_attach_states(
            location_qids=["Q90", "Q_lyon"],
            location_country_codes={"Q90": "FR", "Q_lyon": "FR"},
            country_to_state={"FR": "Q142"},
        )
        assert result == ["Q142"]
