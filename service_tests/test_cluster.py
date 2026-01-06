#!/usr/bin/env python3
"""Smoke test for clustering pipeline story building."""

import numpy as np

from news_cluster.cluster.stories import build_stories


def test_build_stories_with_new_location_schema():
    # Two articles in one cluster with embeddings and new-style locations
    articles = [
        {
            "article_id": "a1",
            "headline": "Paris summit on climate",
            "source": "reuters",
            "embedding_combined": [1.0, 0.0],
            "locations": [
                {
                    "name": "France",
                    "country_code": "FR",
                    "count": 3,
                    "in_headline": True,
                    "sub_entities": [
                        {"name": "Paris", "count": 2, "in_headline": True, "type": "city"}
                    ],
                }
            ],
        },
        {
            "article_id": "a2",
            "headline": "Talks continue in Paris",
            "source": "bbc",
            "embedding_combined": [0.8, 0.2],
            "locations": [
                {
                    "name": "France",
                    "country_code": "FR",
                    "count": 1,
                    "in_headline": True,
                    "sub_entities": [
                        {"name": "Paris", "count": 1, "in_headline": True, "type": "city"}
                    ],
                }
            ],
        },
    ]

    cluster_labels = np.array([0, 0])
    embeddings = np.array([a["embedding_combined"] for a in articles], dtype=np.float32)

    stories, article_maps, story_articles = build_stories(
        articles,
        cluster_labels,
        embeddings,
        location_min_confidence=0.1,  # low threshold to keep location
        location_max_locations=3,
        location_max_regions=3,
        location_max_cities=3,
    )

    # One story created
    assert len(stories) == 1
    story = stories[0]
    assert story.article_count == 2
    assert story.story_id
    assert story.locations, "Expected aggregated locations from new schema"

    # Location aggregation should combine counts and keep city sub-entity
    fr = story.locations[0]
    assert fr.country_code == "FR"
    assert fr.mention_count >= 4  # country + sub-entity counts
    assert fr.sub_entities and fr.sub_entities[0].name == "Paris"
    assert fr.sub_entities[0].mention_count >= 3  # aggregated sub-entity counts
    assert 0 <= fr.in_headline_ratio <= 1

    # Article mappings created for each article
    assert {m.article_id for m in article_maps} == {"a1", "a2"}
    assert all(m.story_id == story.story_id for m in article_maps)

    # Story articles view populated
    assert story_articles and story_articles[0].articles
