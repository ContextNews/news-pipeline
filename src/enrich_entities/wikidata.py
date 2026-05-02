"""Wikidata API client for entity search and detail fetching."""

from __future__ import annotations

import logging
import time
from urllib.parse import quote

import requests

from enrich_entities.models import KBLocation, KBPerson, WikidataCandidate

logger = logging.getLogger(__name__)

WIKIDATA_API = "https://www.wikidata.org/w/api.php"
USER_AGENT = "news-pipeline/1.0 (https://github.com/ContextNews/news-pipeline)"

# Maps Wikidata P31 (instance of) QIDs to our KB location_type values.
# More specific types are listed first so the first match wins.
LOCATION_TYPE_MAP: dict[str, str] = {
    "Q6256": "country",
    "Q3024240": "country",    # historical country
    "Q5119": "city",          # capital city
    "Q515": "city",
    "Q1093829": "city",       # city in the United States
    "Q486972": "city",        # human settlement
    "Q3957": "city",          # town
    "Q532": "city",           # village
    "Q15284": "city",         # municipality
    "Q35657": "state",        # state of the United States
    "Q10864048": "state",     # first-level administrative country subdivision
    "Q13220204": "state",     # second-level administrative country subdivision
    "Q24746": "region",       # metropolitan area
    "Q2221906": "region",     # geographic location (broad)
    "Q82794": "region",       # geographic region
}

HUMAN_QID = "Q5"


def search_entity(name: str, delay: float = 0.5) -> list[WikidataCandidate]:
    """Search Wikidata for candidates matching the given name."""
    time.sleep(delay)
    params = {
        "action": "wbsearchentities",
        "search": name,
        "language": "en",
        "type": "item",
        "limit": 5,
        "format": "json",
    }
    try:
        resp = requests.get(
            WIKIDATA_API,
            params=params,
            headers={"User-Agent": USER_AGENT},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("Wikidata search failed for '%s': %s", name, exc)
        return []

    return [
        WikidataCandidate(
            qid=item["id"],
            label=item.get("label", ""),
            description=item.get("description"),
        )
        for item in data.get("search", [])
    ]


def fetch_wikidata_entity_data(qid: str, delay: float = 0.5) -> dict | None:
    """Fetch entity claims, labels, aliases and descriptions from Wikidata."""
    time.sleep(delay)
    params = {
        "action": "wbgetentities",
        "ids": qid,
        "props": "claims|labels|aliases|descriptions",
        "languages": "en",
        "format": "json",
    }
    try:
        resp = requests.get(
            WIKIDATA_API,
            params=params,
            headers={"User-Agent": USER_AGENT},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("Wikidata entity fetch failed for QID %s: %s", qid, exc)
        return None

    entity = data.get("entities", {}).get(qid)
    if not entity or entity.get("missing") == "":
        return None
    return entity


def classify_as_location(
    qid: str,
    entity_data: dict,
    delay: float = 0.5,
) -> KBLocation | None:
    """
    Return a KBLocation if the entity is a geographic location, else None.

    May make additional API calls to resolve the country code of sub-national locations.
    """
    claims = entity_data.get("claims", {})
    instance_of_qids = _get_claim_qids(claims, "P31")

    location_type: str | None = None
    for instance_qid in instance_of_qids:
        if instance_qid in LOCATION_TYPE_MAP:
            location_type = LOCATION_TYPE_MAP[instance_qid]
            break

    # Fallback: if the entity has a P17 (country) claim it's likely a location
    if not location_type:
        if _get_claim_qids(claims, "P17"):
            location_type = "region"
        else:
            return None

    country_code: str | None = None
    if location_type == "country":
        # P297 holds the ISO 3166-1 alpha-2 code directly on country entities
        country_code = _get_claim_string(claims, "P297")
    else:
        country_qids = _get_claim_qids(claims, "P17")
        if country_qids:
            country_code = _fetch_country_code(country_qids[0], delay)

    return KBLocation(
        qid=qid,
        name=_get_english_label(entity_data) or qid,
        description=entity_data.get("descriptions", {}).get("en", {}).get("value"),
        location_type=location_type,
        country_code=country_code,
    )


def classify_as_person(
    qid: str,
    entity_data: dict,
    delay: float = 0.5,
) -> KBPerson | None:
    """
    Return a KBPerson if the entity is a human, else None.

    May make additional API calls to resolve nationality country codes.
    """
    claims = entity_data.get("claims", {})
    if HUMAN_QID not in _get_claim_qids(claims, "P31"):
        return None

    # P27: country of citizenship → resolve to ISO alpha-2 codes
    nationality_qids = _get_claim_qids(claims, "P27")
    nationalities = _resolve_country_codes(nationality_qids, delay) or None

    # P18: image → build Wikimedia Commons Special:FilePath URL
    image_filename = _get_claim_string(claims, "P18")
    image_url = (
        f"https://commons.wikimedia.org/wiki/Special:FilePath/{quote(image_filename, safe='')}"
        if image_filename else None
    )

    return KBPerson(
        qid=qid,
        name=_get_english_label(entity_data) or qid,
        description=entity_data.get("descriptions", {}).get("en", {}).get("value"),
        nationalities=nationalities,
        image_url=image_url,
    )


def get_english_aliases(entity_data: dict) -> list[str]:
    """Extract the English label and all English aliases from entity data."""
    results: list[str] = []
    label = entity_data.get("labels", {}).get("en", {}).get("value")
    if label:
        results.append(label)
    for entry in entity_data.get("aliases", {}).get("en", []):
        val = entry.get("value")
        if val and val not in results:
            results.append(val)
    return results


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _get_claim_qids(claims: dict, prop: str) -> list[str]:
    """Extract item QID values for a given property from a claims dict."""
    result = []
    for statement in claims.get(prop, []):
        snak = statement.get("mainsnak", {})
        if snak.get("snaktype") == "value":
            dv = snak.get("datavalue", {})
            if dv.get("type") == "wikibase-entityid":
                result.append(dv["value"]["id"])
    return result


def _get_claim_string(claims: dict, prop: str) -> str | None:
    """Extract the first string value for a given property from a claims dict."""
    for statement in claims.get(prop, []):
        snak = statement.get("mainsnak", {})
        if snak.get("snaktype") == "value":
            dv = snak.get("datavalue", {})
            if dv.get("type") == "string":
                return dv["value"]
    return None


def _get_english_label(entity_data: dict) -> str | None:
    return entity_data.get("labels", {}).get("en", {}).get("value")


def _fetch_country_code(country_qid: str, delay: float) -> str | None:
    """Fetch the ISO 3166-1 alpha-2 code for a single country QID."""
    codes = _resolve_country_codes([country_qid], delay)
    return codes[0] if codes else None


def _resolve_country_codes(country_qids: list[str], delay: float) -> list[str]:
    """
    Fetch ISO 3166-1 alpha-2 codes for a list of country QIDs in one batched API call.
    """
    if not country_qids:
        return []
    time.sleep(delay)
    params = {
        "action": "wbgetentities",
        "ids": "|".join(country_qids[:50]),
        "props": "claims",
        "format": "json",
    }
    try:
        resp = requests.get(
            WIKIDATA_API,
            params=params,
            headers={"User-Agent": USER_AGENT},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("Country code resolution failed for %s: %s", country_qids, exc)
        return []

    codes = []
    for qid in country_qids:
        entity = data.get("entities", {}).get(qid, {})
        if entity.get("missing") == "":
            continue
        code = _get_claim_string(entity.get("claims", {}), "P297")
        if code:
            codes.append(code)
    return codes
