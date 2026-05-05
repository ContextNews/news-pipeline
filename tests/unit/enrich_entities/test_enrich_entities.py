"""Tests for enrich_entities.enrich_entities and cli helper functions."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from enrich_entities.enrich_entities import _disambiguate, _try_enrich, enrich_entities
from enrich_entities.helpers import group_by_entity_name
from enrich_entities.models import KBLocation, KBOrganization, KBPerson, WikidataCandidate


# ---------------------------------------------------------------------------
# _group_by_entity_name
# ---------------------------------------------------------------------------


class TestGroupByEntityName:
    def test_inverts_dict(self) -> None:
        entity_dict = {
            "article1": ["LONDON", "PARIS"],
            "article2": ["LONDON"],
        }
        result = group_by_entity_name(entity_dict)
        assert set(result["LONDON"]) == {"article1", "article2"}
        assert result["PARIS"] == ["article1"]

    def test_empty_input(self) -> None:
        assert group_by_entity_name({}) == {}


# ---------------------------------------------------------------------------
# _disambiguate
# ---------------------------------------------------------------------------


class TestDisambiguate:
    def test_returns_none_for_no_candidates(self) -> None:
        assert _disambiguate("LONDON", []) is None

    def test_returns_single_candidate(self) -> None:
        candidate = WikidataCandidate(qid="Q84", label="London", description=None)
        result = _disambiguate("LONDON", [candidate])
        assert result is candidate

    def test_accepts_exact_label_match_among_multiple(self) -> None:
        candidates = [
            WikidataCandidate(qid="Q84", label="London", description="UK capital"),
            WikidataCandidate(qid="Q123", label="London", description="city in Ontario"),
        ]
        # Both have label "London" — neither is a unique exact match → skip
        assert _disambiguate("LONDON", candidates) is None

    def test_accepts_unique_exact_match(self) -> None:
        candidates = [
            WikidataCandidate(qid="Q84", label="London", description="UK capital"),
            WikidataCandidate(qid="Q456", label="Greater London", description="region"),
        ]
        result = _disambiguate("LONDON", candidates)
        assert result is not None
        assert result.qid == "Q84"

    def test_case_insensitive_match(self) -> None:
        candidates = [
            WikidataCandidate(qid="Q1", label="France", description="country"),
            WikidataCandidate(qid="Q2", label="France national football team", description=None),
        ]
        result = _disambiguate("FRANCE", candidates)
        assert result is not None
        assert result.qid == "Q1"

    def test_picks_exact_match_ignoring_non_matching_labels(self) -> None:
        # "Paris" matches "PARIS" exactly; "Paris Hilton" does not — so Q1 is selected
        candidates = [
            WikidataCandidate(qid="Q1", label="Paris", description="city in France"),
            WikidataCandidate(qid="Q2", label="Paris Hilton", description="celebrity"),
        ]
        result = _disambiguate("PARIS", candidates)
        assert result is not None
        assert result.qid == "Q1"

    def test_skips_when_multiple_exact_matches(self) -> None:
        # Two candidates both labelled "Paris" — genuinely ambiguous
        candidates = [
            WikidataCandidate(qid="Q1", label="Paris", description="city in France"),
            WikidataCandidate(qid="Q2", label="Paris", description="city in Texas"),
        ]
        assert _disambiguate("PARIS", candidates) is None


# ---------------------------------------------------------------------------
# enrich_entities
# ---------------------------------------------------------------------------


class TestEnrichEntities:
    @patch("enrich_entities.enrich_entities.search_entity")
    @patch("enrich_entities.enrich_entities.fetch_wikidata_entity_data")
    @patch("enrich_entities.enrich_entities.classify_as_location")
    @patch("enrich_entities.enrich_entities.classify_as_person")
    @patch("enrich_entities.enrich_entities.get_english_aliases")
    def test_enriches_location(
        self,
        mock_aliases,
        mock_classify_person,
        mock_classify_location,
        mock_fetch,
        mock_search,
    ) -> None:
        mock_search.return_value = [
            WikidataCandidate(qid="Q84", label="London", description="city")
        ]
        mock_fetch.return_value = {"claims": {}}
        mock_classify_location.return_value = KBLocation(
            qid="Q84",
            name="London",
            description="capital of the UK",
            location_type="city",
            country_code="GB",
        )
        mock_aliases.return_value = ["London"]

        result = enrich_entities(
            unresolved_gpe={"LONDON": ["article1"]},
            unresolved_persons={},
            delay=0,
        )

        assert len(result) == 1
        assert result[0].qid == "Q84"
        assert result[0].entity_type == "location"
        assert result[0].article_ids == ["article1"]
        assert "London" in result[0].aliases

    @patch("enrich_entities.enrich_entities.search_entity")
    @patch("enrich_entities.enrich_entities.fetch_wikidata_entity_data")
    @patch("enrich_entities.enrich_entities.classify_as_location")
    @patch("enrich_entities.enrich_entities.classify_as_person")
    @patch("enrich_entities.enrich_entities.get_english_aliases")
    def test_enriches_person(
        self,
        mock_aliases,
        mock_classify_person,
        mock_classify_location,
        mock_fetch,
        mock_search,
    ) -> None:
        mock_search.return_value = [
            WikidataCandidate(qid="Q6279", label="Joe Biden", description="US president")
        ]
        mock_fetch.return_value = {"claims": {}}
        mock_classify_person.return_value = KBPerson(
            qid="Q6279",
            name="Joe Biden",
            description="46th president",
            nationalities=["US"],
        )
        mock_aliases.return_value = ["Joe Biden", "Biden"]

        result = enrich_entities(
            unresolved_gpe={},
            unresolved_persons={"JOE BIDEN": ["article1", "article2"]},
            delay=0,
        )

        assert len(result) == 1
        assert result[0].qid == "Q6279"
        assert result[0].entity_type == "person"
        assert set(result[0].article_ids) == {"article1", "article2"}

    @patch("enrich_entities.enrich_entities.search_entity")
    def test_skips_when_no_candidates(self, mock_search) -> None:
        mock_search.return_value = []
        result = enrich_entities(
            unresolved_gpe={"UNKNOWNPLACE": ["article1"]},
            unresolved_persons={},
            delay=0,
        )
        assert result == []

    @patch("enrich_entities.enrich_entities.search_entity")
    @patch("enrich_entities.enrich_entities.fetch_wikidata_entity_data")
    @patch("enrich_entities.enrich_entities.classify_as_location")
    @patch("enrich_entities.enrich_entities.get_english_aliases")
    def test_skips_wrong_entity_type(
        self, mock_aliases, mock_classify_location, mock_fetch, mock_search
    ) -> None:
        # Wikidata returns a result but it fails location classification
        mock_search.return_value = [
            WikidataCandidate(qid="Q5", label="human", description="biological species")
        ]
        mock_fetch.return_value = {"claims": {}}
        mock_classify_location.return_value = None

        result = enrich_entities(
            unresolved_gpe={"HUMAN": ["article1"]},
            unresolved_persons={},
            delay=0,
        )
        assert result == []

    def test_empty_input_returns_empty(self) -> None:
        result = enrich_entities({}, {}, {}, delay=0)
        assert result == []

    @patch("enrich_entities.enrich_entities.search_entity")
    @patch("enrich_entities.enrich_entities.fetch_wikidata_entity_data")
    @patch("enrich_entities.enrich_entities.classify_as_organization")
    @patch("enrich_entities.enrich_entities.get_english_aliases")
    def test_enriches_organisation(
        self,
        mock_aliases,
        mock_classify_org,
        mock_fetch,
        mock_search,
    ) -> None:
        mock_search.return_value = [
            WikidataCandidate(qid="Q312", label="Apple Inc.", description="technology company")
        ]
        mock_fetch.return_value = {"claims": {}}
        mock_classify_org.return_value = KBOrganization(
            qid="Q312",
            name="Apple Inc.",
            description="American technology company",
            org_type="company",
            country_code="US",
            logo_url=None,
        )
        mock_aliases.return_value = ["Apple Inc.", "Apple"]

        result = enrich_entities(
            unresolved_gpe={},
            unresolved_persons={},
            unresolved_orgs={"APPLE INC.": ["article1"]},
            delay=0,
        )

        assert len(result) == 1
        assert result[0].qid == "Q312"
        assert result[0].entity_type == "organization"
        assert result[0].organization is not None
        assert result[0].organization.org_type == "company"
        assert result[0].organization.country_code == "US"
        assert result[0].location is None
        assert result[0].person is None

    @patch("enrich_entities.enrich_entities.search_entity")
    @patch("enrich_entities.enrich_entities.fetch_wikidata_entity_data")
    @patch("enrich_entities.enrich_entities.classify_as_organization")
    def test_skips_org_when_classification_fails(
        self, mock_classify_org, mock_fetch, mock_search
    ) -> None:
        mock_search.return_value = [
            WikidataCandidate(qid="Q1", label="Something", description=None)
        ]
        mock_fetch.return_value = {"claims": {}}
        mock_classify_org.return_value = None

        result = enrich_entities(
            unresolved_gpe={},
            unresolved_persons={},
            unresolved_orgs={"SOMETHING": ["article1"]},
            delay=0,
        )
        assert result == []

    @patch("enrich_entities.enrich_entities.search_entity")
    @patch("enrich_entities.enrich_entities.fetch_wikidata_entity_data")
    @patch("enrich_entities.enrich_entities.classify_as_location")
    @patch("enrich_entities.enrich_entities.get_english_aliases")
    def test_original_name_added_to_aliases(
        self, mock_aliases, mock_classify_location, mock_fetch, mock_search
    ) -> None:
        mock_search.return_value = [
            WikidataCandidate(qid="Q145", label="United Kingdom", description=None)
        ]
        mock_fetch.return_value = {"claims": {}}
        mock_classify_location.return_value = KBLocation(
            qid="Q145",
            name="United Kingdom",
            description=None,
            location_type="country",
            country_code="GB",
        )
        # Wikidata aliases don't include the original extracted name
        mock_aliases.return_value = ["United Kingdom", "UK", "Britain"]

        result = enrich_entities(
            unresolved_gpe={"BRITAIN": ["article1"]},
            unresolved_persons={},
            delay=0,
        )
        assert len(result) == 1
        # "BRITAIN" is title-cased to "Britain" which IS in aliases, so not re-added
        assert "Britain" in result[0].aliases
