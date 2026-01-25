"""Tests for story generation using Cronkite integration."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from generate_stories import generate_stories as stories_module


class FakeCronkite:
    def __init__(self, model: str) -> None:
        self.model = model
        self.seen_articles: list[dict] | None = None

    def generate_story(self, articles: list[dict]) -> dict:
        self.seen_articles = articles
        return {
            "title": "Test Event",
            "summary": "Test summary.",
            "article_ids": [article.get("id") for article in articles],
            "noise_article_ids": ["noise-1"],
        }


def test_generate_story_overview_uses_cronkite(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = FakeCronkite("gpt-4o-mini")

    def _factory(model: str) -> FakeCronkite:
        assert model == "gpt-4o-mini"
        return fake

    monkeypatch.setattr(stories_module, "Cronkite", _factory)

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
    assert story.key_points == []
    assert story.article_ids == ["a1"]
    assert story.noise_article_ids == ["noise-1"]
    assert fake.seen_articles is not None
    assert fake.seen_articles[0]["published_at"].startswith("2024-03-15")


def test_generate_story_overview_normalizes_date(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = FakeCronkite("gpt-4o-mini")

    monkeypatch.setattr(stories_module, "Cronkite", lambda model: fake)

    cluster = [
        {
            "id": "a2",
            "title": "Headline",
            "published_at": date(2024, 3, 15),
            "source": "Reuters",
        }
    ]

    story = stories_module.generate_story_overview(cluster)

    assert story.article_ids == ["a2"]
    assert fake.seen_articles is not None
    assert fake.seen_articles[0]["published_at"] == "2024-03-15"


def test_generate_stories_batch(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = FakeCronkite("gpt-4o-mini")

    monkeypatch.setattr(stories_module, "Cronkite", lambda model: fake)

    clusters = [
        [{"id": "a1", "published_at": None}],
        [{"id": "a2", "published_at": None}],
    ]

    results = stories_module.generate_stories(clusters, model="gpt-4o-mini")

    assert len(results) == 2
    assert all(result.title == "Test Event" for result in results)
