"""Tests for entity resolution and disambiguation heuristics."""

from __future__ import annotations

from resolve_entities.models import (
    ArticleLocation,
    ArticlePerson,
    LocationCandidate,
    PersonCandidate,
)
from resolve_entities.resolve_entities import (
    _build_location_context,
    _disambiguate_location,
    _disambiguate_person,
    _resolve_locations,
    _resolve_persons,
    resolve_entities,
)


# ---------------------------------------------------------------------------
# Location context
# ---------------------------------------------------------------------------


class TestBuildLocationContext:
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
        context = _build_location_context(entity_candidates)
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
        context = _build_location_context(entity_candidates)
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
        context = _build_location_context(entity_candidates)
        assert len(context) == 0


# ---------------------------------------------------------------------------
# Location disambiguation
# ---------------------------------------------------------------------------


class TestDisambiguateLocation:
    def test_returns_single_candidate_unchanged(self) -> None:
        candidates = [
            LocationCandidate(
                wikidata_qid="Q84",
                name="London",
                location_type="city",
                country_code="GB",
            )
        ]
        result = _disambiguate_location(candidates, set())
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
        result = _disambiguate_location(candidates, context)
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
        context = {"FRANCE", "FR"}
        result = _disambiguate_location(candidates, context)
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
        result = _disambiguate_location(candidates, set())
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
        result = _disambiguate_location(candidates, set())
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
        result = _disambiguate_location(candidates, set())
        assert len(result) == 2


# ---------------------------------------------------------------------------
# Location resolution integration
# ---------------------------------------------------------------------------


class TestResolveLocations:
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
        result = _resolve_locations(article_entities, alias_to_locations)
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
        result = _resolve_locations(article_entities, alias_to_locations)
        paris_results = [r for r in result if r.name == "PARIS"]
        assert len(paris_results) == 1
        assert paris_results[0].wikidata_qid == "Q90"

    def test_skips_entities_without_alias_match(self) -> None:
        article_entities = {"article1": ["UNKNOWN_PLACE"]}
        alias_to_locations = {}
        result = _resolve_locations(article_entities, alias_to_locations)
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
        result = _resolve_locations(article_entities, alias_to_locations)
        assert len(result) == 2
        article_ids = {r.article_id for r in result}
        assert article_ids == {"article1", "article2"}


# ---------------------------------------------------------------------------
# Person disambiguation
# ---------------------------------------------------------------------------


class TestDisambiguatePerson:
    def test_returns_single_candidate_unchanged(self) -> None:
        candidates = [
            PersonCandidate(
                wikidata_qid="Q1",
                name="John Smith",
                description="British politician",
                nationalities=["GB"],
            )
        ]
        result = _disambiguate_person(candidates, set())
        assert len(result) == 1
        assert result[0].wikidata_qid == "Q1"

    def test_nationality_match_disambiguates(self) -> None:
        candidates = [
            PersonCandidate(
                wikidata_qid="Q1",
                name="Jean Dupont",
                description="French journalist",
                nationalities=["FR"],
            ),
            PersonCandidate(
                wikidata_qid="Q2",
                name="Jean Dupont",
                description="Canadian politician",
                nationalities=["CA"],
            ),
        ]
        country_context = {"FR"}
        result = _disambiguate_person(candidates, country_context)
        assert len(result) == 1
        assert result[0].wikidata_qid == "Q1"

    def test_no_nationality_context_returns_all(self) -> None:
        candidates = [
            PersonCandidate(
                wikidata_qid="Q1",
                name="John Smith",
                description="British politician",
                nationalities=["GB"],
            ),
            PersonCandidate(
                wikidata_qid="Q2",
                name="John Smith",
                description="American actor",
                nationalities=["US"],
            ),
        ]
        result = _disambiguate_person(candidates, set())
        assert len(result) == 2

    def test_nationality_match_narrows_but_not_to_one(self) -> None:
        candidates = [
            PersonCandidate(
                wikidata_qid="Q1",
                name="John Smith",
                description="British politician",
                nationalities=["GB"],
            ),
            PersonCandidate(
                wikidata_qid="Q2",
                name="John Smith",
                description="British actor",
                nationalities=["GB"],
            ),
            PersonCandidate(
                wikidata_qid="Q3",
                name="John Smith",
                description="American athlete",
                nationalities=["US"],
            ),
        ]
        country_context = {"GB"}
        result = _disambiguate_person(candidates, country_context)
        assert len(result) == 2
        qids = {r.wikidata_qid for r in result}
        assert qids == {"Q1", "Q2"}

    def test_candidate_without_nationalities_excluded_when_context_matches_others(
        self,
    ) -> None:
        candidates = [
            PersonCandidate(
                wikidata_qid="Q1",
                name="Jean Dupont",
                description="French journalist",
                nationalities=["FR"],
            ),
            PersonCandidate(
                wikidata_qid="Q2",
                name="Jean Dupont",
                description="Unknown",
                nationalities=None,
            ),
        ]
        country_context = {"FR"}
        result = _disambiguate_person(candidates, country_context)
        assert len(result) == 1
        assert result[0].wikidata_qid == "Q1"


# ---------------------------------------------------------------------------
# Person resolution integration
# ---------------------------------------------------------------------------


class TestResolvePersons:
    def test_resolves_unambiguous_person(self) -> None:
        person_entities = {"article1": ["JOHN SMITH"]}
        alias_to_persons = {
            "JOHN SMITH": [
                PersonCandidate(
                    wikidata_qid="Q1",
                    name="John Smith",
                    description="British politician",
                    nationalities=["GB"],
                )
            ]
        }
        result = _resolve_persons(person_entities, alias_to_persons, {})
        assert len(result) == 1
        assert result[0].article_id == "article1"
        assert result[0].wikidata_qid == "Q1"
        assert result[0].name == "JOHN SMITH"

    def test_skips_persons_without_alias_match(self) -> None:
        person_entities = {"article1": ["UNKNOWN PERSON"]}
        alias_to_persons = {}
        result = _resolve_persons(person_entities, alias_to_persons, {})
        assert len(result) == 0

    def test_uses_country_context_for_disambiguation(self) -> None:
        person_entities = {"article1": ["JEAN DUPONT"]}
        alias_to_persons = {
            "JEAN DUPONT": [
                PersonCandidate(
                    wikidata_qid="Q1",
                    name="Jean Dupont",
                    description="French journalist",
                    nationalities=["FR"],
                ),
                PersonCandidate(
                    wikidata_qid="Q2",
                    name="Jean Dupont",
                    description="Canadian politician",
                    nationalities=["CA"],
                ),
            ]
        }
        article_country_codes = {"article1": {"FR"}}
        result = _resolve_persons(
            person_entities, alias_to_persons, article_country_codes
        )
        assert len(result) == 1
        assert result[0].wikidata_qid == "Q1"


# ---------------------------------------------------------------------------
# Full resolve_entities integration
# ---------------------------------------------------------------------------


class TestResolveEntities:
    def test_resolves_both_locations_and_persons(self) -> None:
        gpe_entities = {"article1": ["LONDON"]}
        person_entities = {"article1": ["JOHN SMITH"]}
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
        alias_to_persons = {
            "JOHN SMITH": [
                PersonCandidate(
                    wikidata_qid="Q1",
                    name="John Smith",
                    description="British politician",
                    nationalities=["GB"],
                )
            ]
        }
        locations, persons = resolve_entities(
            gpe_entities, person_entities, alias_to_locations, alias_to_persons
        )
        assert len(locations) == 1
        assert len(persons) == 1
        assert locations[0].wikidata_qid == "Q84"
        assert persons[0].wikidata_qid == "Q1"

    def test_location_context_informs_person_resolution(self) -> None:
        gpe_entities = {"article1": ["FRANCE"]}
        person_entities = {"article1": ["JEAN DUPONT"]}
        alias_to_locations = {
            "FRANCE": [
                LocationCandidate(
                    wikidata_qid="Q142",
                    name="France",
                    location_type="country",
                    country_code="FR",
                )
            ]
        }
        alias_to_persons = {
            "JEAN DUPONT": [
                PersonCandidate(
                    wikidata_qid="Q1",
                    name="Jean Dupont",
                    description="French journalist",
                    nationalities=["FR"],
                ),
                PersonCandidate(
                    wikidata_qid="Q2",
                    name="Jean Dupont",
                    description="Canadian politician",
                    nationalities=["CA"],
                ),
            ]
        }
        locations, persons = resolve_entities(
            gpe_entities, person_entities, alias_to_locations, alias_to_persons
        )
        assert len(persons) == 1
        assert persons[0].wikidata_qid == "Q1"

    def test_empty_input_returns_empty_result(self) -> None:
        locations, persons = resolve_entities({}, {}, {}, {})
        assert locations == []
        assert persons == []
