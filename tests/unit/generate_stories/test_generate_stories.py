"""Tests for generate_stories.generate_stories module."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from generate_stories import generate_stories as stories_module
from generate_stories.generate_stories import (
    _normalize_articles_for_cronkite,
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
