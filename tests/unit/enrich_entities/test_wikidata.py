"""Tests for enrich_entities.wikidata module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from enrich_entities.wikidata import (
    _get_claim_qids,
    _get_claim_string,
    _resolve_country_codes,
    classify_as_location,
    classify_as_person,
    get_english_aliases,
    search_entity,
    fetch_wikidata_entity_data,
)


# ---------------------------------------------------------------------------
# Pure helper functions
# ---------------------------------------------------------------------------


class TestGetClaimQids:
    def test_extracts_qids(self) -> None:
        claims = {
            "P31": [
                {
                    "mainsnak": {
                        "snaktype": "value",
                        "datavalue": {
                            "type": "wikibase-entityid",
                            "value": {"id": "Q515"},
                        },
                    }
                }
            ]
        }
        assert _get_claim_qids(claims, "P31") == ["Q515"]

    def test_missing_property_returns_empty(self) -> None:
        assert _get_claim_qids({}, "P31") == []

    def test_ignores_somevalue_snaks(self) -> None:
        claims = {
            "P31": [{"mainsnak": {"snaktype": "somevalue"}}]
        }
        assert _get_claim_qids(claims, "P31") == []


class TestGetClaimString:
    def test_extracts_string(self) -> None:
        claims = {
            "P297": [
                {
                    "mainsnak": {
                        "snaktype": "value",
                        "datavalue": {"type": "string", "value": "GB"},
                    }
                }
            ]
        }
        assert _get_claim_string(claims, "P297") == "GB"

    def test_missing_property_returns_none(self) -> None:
        assert _get_claim_string({}, "P297") is None


class TestGetEnglishAliases:
    def test_returns_label_and_aliases(self) -> None:
        entity_data = {
            "labels": {"en": {"value": "London"}},
            "aliases": {"en": [{"value": "Greater London"}, {"value": "Inner London"}]},
        }
        result = get_english_aliases(entity_data)
        assert "London" in result
        assert "Greater London" in result
        assert "Inner London" in result

    def test_no_duplicates(self) -> None:
        entity_data = {
            "labels": {"en": {"value": "London"}},
            "aliases": {"en": [{"value": "London"}]},
        }
        result = get_english_aliases(entity_data)
        assert result.count("London") == 1

    def test_no_english_data_returns_empty(self) -> None:
        assert get_english_aliases({}) == []


# ---------------------------------------------------------------------------
# search_entity
# ---------------------------------------------------------------------------


class TestSearchEntity:
    @patch("enrich_entities.wikidata.requests.get")
    @patch("enrich_entities.wikidata.time.sleep")
    def test_returns_candidates(self, mock_sleep, mock_get) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "search": [
                {"id": "Q84", "label": "London", "description": "capital of the UK"},
                {"id": "Q123", "label": "London", "description": "city in Ontario"},
            ]
        }
        mock_get.return_value = mock_resp

        result = search_entity("London", delay=0)
        assert len(result) == 2
        assert result[0].qid == "Q84"
        assert result[0].label == "London"
        assert result[0].description == "capital of the UK"

    @patch("enrich_entities.wikidata.requests.get")
    @patch("enrich_entities.wikidata.time.sleep")
    def test_returns_empty_on_api_error(self, mock_sleep, mock_get) -> None:
        mock_get.side_effect = Exception("network error")
        result = search_entity("London", delay=0)
        assert result == []

    @patch("enrich_entities.wikidata.requests.get")
    @patch("enrich_entities.wikidata.time.sleep")
    def test_returns_empty_when_no_results(self, mock_sleep, mock_get) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"search": []}
        mock_get.return_value = mock_resp
        assert search_entity("xyzunknown", delay=0) == []


# ---------------------------------------------------------------------------
# fetch_wikidata_entity_data
# ---------------------------------------------------------------------------


class TestFetchWikidataEntityData:
    @patch("enrich_entities.wikidata.requests.get")
    @patch("enrich_entities.wikidata.time.sleep")
    def test_returns_entity_data(self, mock_sleep, mock_get) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "entities": {"Q84": {"id": "Q84", "claims": {}}}
        }
        mock_get.return_value = mock_resp
        result = fetch_wikidata_entity_data("Q84", delay=0)
        assert result is not None
        assert result["id"] == "Q84"

    @patch("enrich_entities.wikidata.requests.get")
    @patch("enrich_entities.wikidata.time.sleep")
    def test_returns_none_for_missing_entity(self, mock_sleep, mock_get) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "entities": {"Q99999": {"missing": ""}}
        }
        mock_get.return_value = mock_resp
        assert fetch_wikidata_entity_data("Q99999", delay=0) is None

    @patch("enrich_entities.wikidata.requests.get")
    @patch("enrich_entities.wikidata.time.sleep")
    def test_returns_none_on_api_error(self, mock_sleep, mock_get) -> None:
        mock_get.side_effect = Exception("timeout")
        assert fetch_wikidata_entity_data("Q84", delay=0) is None


# ---------------------------------------------------------------------------
# classify_as_location
# ---------------------------------------------------------------------------


def _make_entity_data(instance_of_qids: list[str], p17_qid: str | None = None, p297: str | None = None, label: str = "Test") -> dict:
    """Build a minimal entity_data dict for testing."""
    claims: dict = {}
    if instance_of_qids:
        claims["P31"] = [
            {
                "mainsnak": {
                    "snaktype": "value",
                    "datavalue": {"type": "wikibase-entityid", "value": {"id": qid}},
                }
            }
            for qid in instance_of_qids
        ]
    if p17_qid:
        claims["P17"] = [
            {
                "mainsnak": {
                    "snaktype": "value",
                    "datavalue": {"type": "wikibase-entityid", "value": {"id": p17_qid}},
                }
            }
        ]
    if p297:
        claims["P297"] = [
            {
                "mainsnak": {
                    "snaktype": "value",
                    "datavalue": {"type": "string", "value": p297},
                }
            }
        ]
    return {
        "claims": claims,
        "labels": {"en": {"value": label}},
        "descriptions": {"en": {"value": "a place"}},
    }


class TestClassifyAsLocation:
    def test_classifies_country(self) -> None:
        entity_data = _make_entity_data(["Q6256"], p297="GB", label="United Kingdom")
        result = classify_as_location("Q145", entity_data, delay=0)
        assert result is not None
        assert result.location_type == "country"
        assert result.country_code == "GB"
        assert result.name == "United Kingdom"

    def test_classifies_city(self) -> None:
        with patch("enrich_entities.wikidata._fetch_country_code", return_value="GB"):
            entity_data = _make_entity_data(["Q515"], p17_qid="Q145", label="London")
            result = classify_as_location("Q84", entity_data, delay=0)
        assert result is not None
        assert result.location_type == "city"
        assert result.country_code == "GB"

    def test_falls_back_to_region_when_has_p17(self) -> None:
        with patch("enrich_entities.wikidata._fetch_country_code", return_value="US"):
            entity_data = _make_entity_data([], p17_qid="Q30", label="Some Region")
            result = classify_as_location("Q999", entity_data, delay=0)
        assert result is not None
        assert result.location_type == "region"

    def test_returns_none_for_non_location(self) -> None:
        entity_data = _make_entity_data(["Q5"])  # human
        result = classify_as_location("Q1", entity_data, delay=0)
        assert result is None


# ---------------------------------------------------------------------------
# classify_as_person
# ---------------------------------------------------------------------------


class TestClassifyAsPerson:
    def test_classifies_person(self) -> None:
        with patch("enrich_entities.wikidata._resolve_country_codes", return_value=["US"]):
            entity_data = _make_entity_data(["Q5"], label="Joe Biden")
            entity_data["claims"]["P27"] = [
                {
                    "mainsnak": {
                        "snaktype": "value",
                        "datavalue": {"type": "wikibase-entityid", "value": {"id": "Q30"}},
                    }
                }
            ]
            result = classify_as_person("Q6279", entity_data, delay=0)
        assert result is not None
        assert result.name == "Joe Biden"
        assert result.nationalities == ["US"]

    def test_returns_none_for_non_person(self) -> None:
        entity_data = _make_entity_data(["Q515"])  # city
        result = classify_as_person("Q84", entity_data, delay=0)
        assert result is None

    def test_none_nationalities_when_no_p27(self) -> None:
        entity_data = _make_entity_data(["Q5"], label="Unknown Person")
        result = classify_as_person("Q1", entity_data, delay=0)
        assert result is not None
        assert result.nationalities is None


# ---------------------------------------------------------------------------
# _resolve_country_codes
# ---------------------------------------------------------------------------


class TestResolveCountryCodes:
    @patch("enrich_entities.wikidata.requests.get")
    @patch("enrich_entities.wikidata.time.sleep")
    def test_resolves_codes(self, mock_sleep, mock_get) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "entities": {
                "Q145": {
                    "claims": {
                        "P297": [
                            {
                                "mainsnak": {
                                    "snaktype": "value",
                                    "datavalue": {"type": "string", "value": "GB"},
                                }
                            }
                        ]
                    }
                }
            }
        }
        mock_get.return_value = mock_resp
        result = _resolve_country_codes(["Q145"], delay=0)
        assert result == ["GB"]

    def test_empty_input_returns_empty(self) -> None:
        assert _resolve_country_codes([], delay=0) == []

    @patch("enrich_entities.wikidata.requests.get")
    @patch("enrich_entities.wikidata.time.sleep")
    def test_returns_empty_on_error(self, mock_sleep, mock_get) -> None:
        mock_get.side_effect = Exception("timeout")
        assert _resolve_country_codes(["Q145"], delay=0) == []
