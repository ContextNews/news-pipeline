"""Tests for article location resolution and disambiguation heuristics."""

from __future__ import annotations

import pytest

from resolve_article_locations.models import ArticleLocation, LocationCandidate
from resolve_article_locations.resolve_article_locations import (
    _build_context,
    _disambiguate,
    resolve_article_locations,
)


class TestBuildContext:
    def test_extracts_country_code_from_unambiguous_entity(self) -> None:
        entity_candidates = {
            "LONDON": [
                LocationCandidate(
                    wikidata_qid="Q84",
                    name="London",
                    location_type="city",
                    country_code="GB",
                )
            ]
        }
        context = _build_context(entity_candidates)
        assert "GB" in context

    def test_extracts_country_name_from_unambiguous_country(self) -> None:
        entity_candidates = {
            "FRANCE": [
                LocationCandidate(
                    wikidata_qid="Q142",
                    name="France",
                    location_type="country",
                    country_code="FR",
                )
            ]
        }
        context = _build_context(entity_candidates)
        assert "FRANCE" in context
        assert "FR" in context

    def test_ignores_ambiguous_entities(self) -> None:
        entity_candidates = {
            "PARIS": [
                LocationCandidate(
                    wikidata_qid="Q90",
                    name="Paris",
                    location_type="city",
                    country_code="FR",
                ),
                LocationCandidate(
                    wikidata_qid="Q830149",
                    name="Paris",
                    location_type="city",
                    country_code="US",
                ),
            ]
        }
        context = _build_context(entity_candidates)
        assert len(context) == 0


class TestDisambiguate:
    def test_returns_single_candidate_unchanged(self) -> None:
        candidates = [
            LocationCandidate(
                wikidata_qid="Q84",
                name="London",
                location_type="city",
                country_code="GB",
            )
        ]
        result = _disambiguate(candidates, set())
        assert len(result) == 1
        assert result[0].wikidata_qid == "Q84"

    def test_heuristic1_prefers_candidate_matching_country_code_context(self) -> None:
        candidates = [
            LocationCandidate(
                wikidata_qid="Q90",
                name="Paris",
                location_type="city",
                country_code="FR",
            ),
            LocationCandidate(
                wikidata_qid="Q830149",
                name="Paris",
                location_type="city",
                country_code="US",
            ),
        ]
        context = {"FR"}
        result = _disambiguate(candidates, context)
        assert len(result) == 1
        assert result[0].wikidata_qid == "Q90"

    def test_heuristic1_prefers_candidate_matching_country_name_context(self) -> None:
        candidates = [
            LocationCandidate(
                wikidata_qid="Q90",
                name="Paris",
                location_type="city",
                country_code="FR",
            ),
            LocationCandidate(
                wikidata_qid="Q830149",
                name="Paris",
                location_type="city",
                country_code="US",
            ),
        ]
        # France mentioned elsewhere in article - context contains both country code and name
        context = {"FRANCE", "FR"}
        result = _disambiguate(candidates, context)
        assert len(result) == 1
        assert result[0].name == "Paris"
        assert result[0].country_code == "FR"

    def test_heuristic2_prefers_country_over_subdivision(self) -> None:
        candidates = [
            LocationCandidate(
                wikidata_qid="Q230",
                name="Georgia",
                location_type="country",
                country_code="GE",
            ),
            LocationCandidate(
                wikidata_qid="Q1428",
                name="Georgia",
                location_type="state",
                country_code="US",
            ),
        ]
        result = _disambiguate(candidates, set())
        assert len(result) == 1
        assert result[0].wikidata_qid == "Q230"
        assert result[0].location_type == "country"

    def test_heuristic3_type_hierarchy_prefers_countries(self) -> None:
        candidates = [
            LocationCandidate(
                wikidata_qid="Q1",
                name="Test",
                location_type="city",
                country_code="XX",
            ),
            LocationCandidate(
                wikidata_qid="Q2",
                name="Test",
                location_type="country",
                country_code="YY",
            ),
            LocationCandidate(
                wikidata_qid="Q3",
                name="Test",
                location_type="state",
                country_code="ZZ",
            ),
        ]
        result = _disambiguate(candidates, set())
        assert len(result) == 1
        assert result[0].location_type == "country"

    def test_returns_all_candidates_of_best_type_when_multiple(self) -> None:
        candidates = [
            LocationCandidate(
                wikidata_qid="Q1",
                name="Test1",
                location_type="city",
                country_code="XX",
            ),
            LocationCandidate(
                wikidata_qid="Q2",
                name="Test2",
                location_type="city",
                country_code="YY",
            ),
        ]
        result = _disambiguate(candidates, set())
        assert len(result) == 2


class TestResolveArticleLocations:
    def test_resolves_unambiguous_location(self) -> None:
        article_entities = {"article1": ["LONDON"]}
        alias_to_locations = {
            "LONDON": [
                LocationCandidate(
                    wikidata_qid="Q84",
                    name="London",
                    location_type="city",
                    country_code="GB",
                )
            ]
        }
        result = resolve_article_locations(article_entities, alias_to_locations)
        assert len(result) == 1
        assert result[0].article_id == "article1"
        assert result[0].wikidata_qid == "Q84"
        assert result[0].name == "LONDON"

    def test_uses_context_from_other_entities(self) -> None:
        article_entities = {"article1": ["PARIS", "FRANCE"]}
        alias_to_locations = {
            "PARIS": [
                LocationCandidate(
                    wikidata_qid="Q90",
                    name="Paris",
                    location_type="city",
                    country_code="FR",
                ),
                LocationCandidate(
                    wikidata_qid="Q830149",
                    name="Paris",
                    location_type="city",
                    country_code="US",
                ),
            ],
            "FRANCE": [
                LocationCandidate(
                    wikidata_qid="Q142",
                    name="France",
                    location_type="country",
                    country_code="FR",
                )
            ],
        }
        result = resolve_article_locations(article_entities, alias_to_locations)
        paris_results = [r for r in result if r.name == "PARIS"]
        assert len(paris_results) == 1
        assert paris_results[0].wikidata_qid == "Q90"  # Paris, France

    def test_skips_entities_without_alias_match(self) -> None:
        article_entities = {"article1": ["UNKNOWN_PLACE"]}
        alias_to_locations = {}
        result = resolve_article_locations(article_entities, alias_to_locations)
        assert len(result) == 0

    def test_resolves_multiple_articles(self) -> None:
        article_entities = {
            "article1": ["LONDON"],
            "article2": ["BERLIN"],
        }
        alias_to_locations = {
            "LONDON": [
                LocationCandidate(
                    wikidata_qid="Q84",
                    name="London",
                    location_type="city",
                    country_code="GB",
                )
            ],
            "BERLIN": [
                LocationCandidate(
                    wikidata_qid="Q64",
                    name="Berlin",
                    location_type="city",
                    country_code="DE",
                )
            ],
        }
        result = resolve_article_locations(article_entities, alias_to_locations)
        assert len(result) == 2
        article_ids = {r.article_id for r in result}
        assert article_ids == {"article1", "article2"}
