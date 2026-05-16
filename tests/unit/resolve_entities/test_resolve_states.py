"""Tests for state resolution and state-preferred-over-location behaviour."""

from __future__ import annotations

from resolve_entities.models import (
    LocationCandidate,
    PersonCandidate,
    StateCandidate,
)
from resolve_entities.resolve_entities import _resolve_gpe, resolve_entities


class TestStateResolution:
    def test_resolves_unambiguous_state(self) -> None:
        states, locations = _resolve_gpe(
            {"article1": ["FRANCE"]},
            {"FRANCE": [StateCandidate(wikidata_qid="Q142", name="France", iso_alpha_2="FR")]},
            {},
        )
        assert len(states) == 1
        assert states[0].wikidata_qid == "Q142"
        assert states[0].article_id == "article1"
        assert states[0].name == "FRANCE"
        assert locations == []

    def test_state_preferred_over_location(self) -> None:
        """An alias mapped to both a state and a location resolves only to the state."""
        states, locations = _resolve_gpe(
            {"article1": ["RUSSIA"]},
            {"RUSSIA": [StateCandidate(wikidata_qid="Q159", name="Russia", iso_alpha_2="RU")]},
            {
                "RUSSIA": [
                    LocationCandidate(
                        wikidata_qid="Q1000000",
                        name="Russia",
                        location_type="region",
                        country_code="RU",
                    )
                ]
            },
        )
        assert len(states) == 1
        assert states[0].wikidata_qid == "Q159"
        assert locations == []

    def test_ambiguous_state_alias_skipped(self) -> None:
        """An alias mapping to multiple state QIDs is dropped (no silent fallback)."""
        states, locations = _resolve_gpe(
            {"article1": ["CONGO"]},
            {
                "CONGO": [
                    StateCandidate(wikidata_qid="Q971", name="Republic of the Congo", iso_alpha_2="CG"),
                    StateCandidate(wikidata_qid="Q974", name="Democratic Republic of the Congo", iso_alpha_2="CD"),
                ]
            },
            {},
        )
        assert states == []
        assert locations == []

    def test_falls_back_to_location_when_no_state_alias(self) -> None:
        states, locations = _resolve_gpe(
            {"article1": ["LONDON"]},
            {},
            {
                "LONDON": [
                    LocationCandidate(
                        wikidata_qid="Q84",
                        name="London",
                        location_type="city",
                        country_code="GB",
                    )
                ]
            },
        )
        assert states == []
        assert len(locations) == 1
        assert locations[0].wikidata_qid == "Q84"

    def test_state_provides_country_context_for_city_disambiguation(self) -> None:
        """
        With "France" resolved as a state (no country-shaped kb_location row),
        the FR iso code must still flow into the location-disambiguation context
        so that ambiguous "Paris" resolves to Paris/FR not Paris/TX.
        """
        states, locations = _resolve_gpe(
            {"article1": ["FRANCE", "PARIS"]},
            {"FRANCE": [StateCandidate(wikidata_qid="Q142", name="France", iso_alpha_2="FR")]},
            {
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
            },
        )
        paris = [loc for loc in locations if loc.name == "PARIS"]
        assert len(paris) == 1
        assert paris[0].wikidata_qid == "Q90"

    def test_state_iso_code_feeds_person_disambiguation(self) -> None:
        """When only a state mentions the country (no country-location), the state
        ISO code still informs person nationality disambiguation."""
        _locations, states, persons, _orgs = resolve_entities(
            article_gpe_entities={"article1": ["FRANCE"]},
            article_person_entities={"article1": ["JEAN DUPONT"]},
            alias_to_locations={},
            alias_to_persons={
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
            },
            alias_to_states={
                "FRANCE": [StateCandidate(wikidata_qid="Q142", name="France", iso_alpha_2="FR")]
            },
        )
        assert len(states) == 1
        assert states[0].wikidata_qid == "Q142"
        assert len(persons) == 1
        assert persons[0].wikidata_qid == "Q1"

    def test_resolve_entities_returns_states_in_fourth_position(self) -> None:
        locations, states, persons, organizations = resolve_entities(
            article_gpe_entities={"article1": ["FRANCE"]},
            article_person_entities={},
            alias_to_locations={},
            alias_to_persons={},
            alias_to_states={
                "FRANCE": [StateCandidate(wikidata_qid="Q142", name="France", iso_alpha_2="FR")]
            },
        )
        assert locations == []
        assert persons == []
        assert organizations == []
        assert len(states) == 1
        assert states[0].wikidata_qid == "Q142"
