"""Tests for generate_stories.classify_stories module."""

from __future__ import annotations

from generate_stories.classify_stories import classify_stories


class TestClassifyStories:
    def test_empty_stories_returns_empty(self) -> None:
        assert classify_stories([], {}) == []

    def test_story_with_no_articles_returns_empty_topics(self) -> None:
        stories = [{"story_id": "s1", "article_ids": []}]
        result = classify_stories(stories, {"a1": ["politics"]})
        assert result[0].story_id == "s1"
        assert result[0].topics == []

    def test_top_two_topics_by_frequency(self) -> None:
        # politics: 3 articles, tech: 2 articles, sport: 1 article
        stories = [{"story_id": "s1", "article_ids": ["a1", "a2", "a3", "a4", "a5", "a6"]}]
        article_topics = {
            "a1": ["politics"],
            "a2": ["politics"],
            "a3": ["politics", "tech"],
            "a4": ["tech"],
            "a5": ["sport"],
            "a6": [],
        }
        result = classify_stories(stories, article_topics)
        assert result[0].topics == ["politics", "tech"]

    def test_topic_below_threshold_excluded(self) -> None:
        # 4 articles: topic appears in exactly 1 (25%), threshold is >25% so excluded
        stories = [{"story_id": "s1", "article_ids": ["a1", "a2", "a3", "a4"]}]
        article_topics = {
            "a1": ["politics"],
            "a2": [],
            "a3": [],
            "a4": [],
        }
        result = classify_stories(stories, article_topics)
        assert result[0].topics == []

    def test_topic_above_threshold_included(self) -> None:
        # 4 articles: topic appears in 2 (50% > 25%), included
        stories = [{"story_id": "s1", "article_ids": ["a1", "a2", "a3", "a4"]}]
        article_topics = {
            "a1": ["politics"],
            "a2": ["politics"],
            "a3": [],
            "a4": [],
        }
        result = classify_stories(stories, article_topics)
        assert result[0].topics == ["politics"]

    def test_max_two_topics_returned(self) -> None:
        # 3 topics all qualify, only top 2 returned
        stories = [{"story_id": "s1", "article_ids": ["a1", "a2", "a3"]}]
        article_topics = {
            "a1": ["politics", "tech", "sport"],
            "a2": ["politics", "tech", "sport"],
            "a3": ["politics", "tech", "sport"],
        }
        result = classify_stories(stories, article_topics)
        assert len(result[0].topics) == 2

    def test_articles_missing_from_topics_map_treated_as_no_topics(self) -> None:
        # 4 articles, only 1 in the topics map — 1/4 = 25%, not > 25%, so excluded
        stories = [{"story_id": "s1", "article_ids": ["a1", "a2", "a3", "a4"]}]
        article_topics = {"a1": ["politics"]}
        result = classify_stories(stories, article_topics)
        assert result[0].topics == []

    def test_multiple_stories_classified_independently(self) -> None:
        stories = [
            {"story_id": "s1", "article_ids": ["a1", "a2"]},
            {"story_id": "s2", "article_ids": ["a3", "a4"]},
        ]
        article_topics = {
            "a1": ["politics"],
            "a2": ["politics"],
            "a3": ["tech"],
            "a4": ["tech"],
        }
        result = classify_stories(stories, article_topics)
        assert result[0].story_id == "s1"
        assert result[0].topics == ["politics"]
        assert result[1].story_id == "s2"
        assert result[1].topics == ["tech"]
