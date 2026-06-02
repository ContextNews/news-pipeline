"""Tests for generate_stories.generate_stories module."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from generate_stories import generate_stories as stories_module
from generate_stories.generate_stories import (
    _normalize_articles_for_cronkite,
    _normalize_key_points,
    GeneratedStoryOverview,
)


class FakeCronkite:
    def __init__(self, model: str, config=None) -> None:
        self.model = model
        self.seen_articles: list[dict] | None = None

    def generate_story(self, articles: list[dict]) -> dict:
        self.seen_articles = articles
        return {
            "title": "Test Event",
            "summary": "Test summary.",
            "article_ids": [a.get("id") for a in articles],
            "noise_article_ids": ["noise-1"],
        }


class TestNormalizeKeyPoints:
    def test_list_of_strings_passthrough(self) -> None:
        assert _normalize_key_points(["point one", "point two"]) == ["point one", "point two"]

    def test_bare_string_wrapped_in_list(self) -> None:
        assert _normalize_key_points("only one point") == ["only one point"]

    def test_none_returns_empty_list(self) -> None:
        assert _normalize_key_points(None) == []

    def test_empty_list_returns_empty_list(self) -> None:
        assert _normalize_key_points([]) == []

    def test_empty_string_returns_empty_list(self) -> None:
        assert _normalize_key_points("") == []

    def test_list_items_coerced_to_str(self) -> None:
        assert _normalize_key_points([1, 2, 3]) == ["1", "2", "3"]


class TestNormalizeArticlesForCronkite:
    def test_datetime_to_iso_string(self) -> None:
        cluster = [
            {"id": "a1", "published_at": datetime(2024, 3, 15, 9, 30, tzinfo=timezone.utc)}
        ]
        result = _normalize_articles_for_cronkite(cluster)
        assert result[0]["published_at"].startswith("2024-03-15")

    def test_date_to_iso_string(self) -> None:
        cluster = [{"id": "a1", "published_at": date(2024, 3, 15)}]
        result = _normalize_articles_for_cronkite(cluster)
        assert result[0]["published_at"] == "2024-03-15"

    def test_string_passthrough(self) -> None:
        cluster = [{"id": "a1", "published_at": "2024-03-15T09:30:00"}]
        result = _normalize_articles_for_cronkite(cluster)
        assert result[0]["published_at"] == "2024-03-15T09:30:00"

    def test_none_passthrough(self) -> None:
        cluster = [{"id": "a1", "published_at": None}]
        result = _normalize_articles_for_cronkite(cluster)
        assert result[0]["published_at"] is None


class TestGenerateStoryOverview:
    def test_uses_cronkite(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = FakeCronkite("gpt-4o-mini")

        monkeypatch.setattr(stories_module, "Cronkite", lambda model, config: fake)

        cluster = [
            {
                "id": "a1",
                "title": "Headline",
                "summary": "Lede",
                "published_at": datetime(2024, 3, 15, 9, 30, tzinfo=timezone.utc),
                "source": "Reuters",
            }
        ]

        story = stories_module.generate_story_overview(cluster, model="gpt-4o-mini")

        assert story.title == "Test Event"
        assert story.summary == "Test summary."
        assert story.article_ids == ["a1"]
        assert story.noise_article_ids == ["noise-1"]
        assert fake.seen_articles is not None
        assert fake.seen_articles[0]["published_at"].startswith("2024-03-15")

    def test_key_points_as_list(self, monkeypatch: pytest.MonkeyPatch) -> None:
        class FakeWithKeyPoints(FakeCronkite):
            def generate_story(self, articles):
                return {**super().generate_story(articles), "key_points": ["point A", "point B"]}

        monkeypatch.setattr(stories_module, "Cronkite", lambda model, config: FakeWithKeyPoints("gpt-4o-mini"))
        cluster = [{"id": "a1", "published_at": None}]
        story = stories_module.generate_story_overview(cluster)
        assert story.key_points == ["point A", "point B"]

    def test_key_points_as_bare_string_wrapped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        class FakeBareString(FakeCronkite):
            def generate_story(self, articles):
                return {**super().generate_story(articles), "key_points": "single point text"}

        monkeypatch.setattr(stories_module, "Cronkite", lambda model, config: FakeBareString("gpt-4o-mini"))
        cluster = [{"id": "a1", "published_at": None}]
        story = stories_module.generate_story_overview(cluster)
        assert story.key_points == ["single point text"]

    def test_key_points_none_returns_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        class FakeNone(FakeCronkite):
            def generate_story(self, articles):
                return {**super().generate_story(articles), "key_points": None}

        monkeypatch.setattr(stories_module, "Cronkite", lambda model, config: FakeNone("gpt-4o-mini"))
        cluster = [{"id": "a1", "published_at": None}]
        story = stories_module.generate_story_overview(cluster)
        assert story.key_points == []

    def test_key_points_missing_returns_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(stories_module, "Cronkite", lambda model, config: FakeCronkite("gpt-4o-mini"))
        cluster = [{"id": "a1", "published_at": None}]
        story = stories_module.generate_story_overview(cluster)
        assert story.key_points == []


class TestGenerateStory:
    def test_integrates_location_and_person_resolution(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake = FakeCronkite("gpt-4o-mini")
        monkeypatch.setattr(stories_module, "Cronkite", lambda model, config: fake)

        cluster = [{"id": "a1", "published_at": None}]
        article_locations = {"a1": ["Q84"]}
        article_persons = {"a1": ["Q1", "Q2"]}

        story = stories_module.generate_story(
            cluster,
            model="gpt-4o-mini",
            article_locations=article_locations,
            article_persons=article_persons,
        )

        assert story.location_qid == "Q84"
        assert story.person_qids == ["Q1", "Q2"]


class TestGenerateStories:
    def test_batch_processing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = FakeCronkite("gpt-4o-mini")
        monkeypatch.setattr(stories_module, "Cronkite", lambda model, config: fake)

        clusters = [
            [{"id": "a1", "published_at": None}],
            [{"id": "a2", "published_at": None}],
        ]

        results = stories_module.generate_stories(clusters, model="gpt-4o-mini")

        assert len(results) == 2
        assert all(r.title == "Test Event" for r in results)
