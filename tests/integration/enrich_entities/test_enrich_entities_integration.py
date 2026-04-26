"""Integration test for the enrich_entities pipeline stage.

Loads real entity data from the database, runs enrichment against the live
Wikidata API, and validates the results. Does not write anything to the database.

Run with:
    poetry run pytest -m integration -s --log-cli-level=INFO
"""

import dataclasses
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from common.aws import load_entities_for_resolution, load_location_aliases, load_person_aliases
from enrich_entities.enrich_entities import enrich_entities
from enrich_entities.helpers import group_by_entity_name

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.integration

# Maximum number of unresolved entities of each type to send to Wikidata.
# Keeps the test fast and avoids hammering the API.
SAMPLE_SIZE = 5

DATA_DIR = Path(__file__).parents[2] / "data"


@pytest.fixture(scope="module")
def unresolved_sample() -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """
    Load a small sample of unresolved GPE and PERSON entities from the real DB.

    Tries the past 7 days in reverse order, using the first date that has data.
    Skips if the KB already covers everything found.
    """
    gpe_entities: dict = {}
    person_entities: dict = {}
    found_date = None

    for days_ago in range(7):
        date = (datetime.now(timezone.utc) - timedelta(days=days_ago)).date()
        gpe_entities, person_entities = load_entities_for_resolution(date, overwrite=False)
        if gpe_entities or person_entities:
            found_date = date
            break

    if not found_date:
        pytest.skip("No entity data found in DB for the past 7 days")

    logger.info(
        "Using date %s — found %d articles with GPE entities, %d with PERSON entities",
        found_date,
        len(gpe_entities),
        len(person_entities),
    )

    gpe_by_name = group_by_entity_name(gpe_entities)
    person_by_name = group_by_entity_name(person_entities)

    logger.info(
        "%d distinct GPE names, %d distinct PERSON names",
        len(gpe_by_name),
        len(person_by_name),
    )

    existing_location_aliases = load_location_aliases(set(gpe_by_name.keys()))
    existing_person_aliases = load_person_aliases(set(person_by_name.keys()))

    logger.info(
        "%d GPE names already in KB, %d PERSON names already in KB",
        len(existing_location_aliases),
        len(existing_person_aliases),
    )

    unresolved_gpe = {
        name: ids
        for name, ids in gpe_by_name.items()
        if name not in existing_location_aliases
    }
    unresolved_persons = {
        name: ids
        for name, ids in person_by_name.items()
        if name not in existing_person_aliases
    }

    logger.info(
        "%d unresolved GPE names, %d unresolved PERSON names",
        len(unresolved_gpe),
        len(unresolved_persons),
    )

    if not unresolved_gpe and not unresolved_persons:
        pytest.skip("No unresolved entities found — KB already covers all entities for this date")

    sample_gpe = dict(list(unresolved_gpe.items())[:SAMPLE_SIZE])
    sample_persons = dict(list(unresolved_persons.items())[:SAMPLE_SIZE])

    logger.info(
        "Sampled %d GPE and %d PERSON names for enrichment: GPE=%s PERSON=%s",
        len(sample_gpe),
        len(sample_persons),
        list(sample_gpe.keys()),
        list(sample_persons.keys()),
    )

    return sample_gpe, sample_persons


@pytest.fixture(scope="module")
def enriched_results(unresolved_sample):
    """Run enrichment once and save results to tests/data/."""
    sample_gpe, sample_persons = unresolved_sample
    results = enrich_entities(sample_gpe, sample_persons, delay=0.5)
    logger.info("Wikidata returned %d enriched entities", len(results))

    DATA_DIR.mkdir(exist_ok=True)
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_path = DATA_DIR / f"enrich_entities_{date_str}.json"
    with open(output_path, "w") as f:
        json.dump([dataclasses.asdict(e) for e in results], f, indent=2)
    logger.info("Saved enriched entities to %s", output_path)

    return results


def test_enrich_entities_runs_without_error(enriched_results) -> None:
    """enrich_entities completes without raising against real Wikidata."""
    pass


def test_enriched_entities_have_valid_structure(enriched_results) -> None:
    """Every returned EnrichedEntity has the required fields correctly populated."""
    for entity in enriched_results:
        logger.info(
            "  [%s] '%s' → %s '%s' | aliases=%s | articles=%d",
            entity.entity_type,
            entity.entity_name,
            entity.qid,
            entity.name,
            entity.aliases,
            len(entity.article_ids),
        )
        assert entity.qid, f"QID must be non-empty for entity '{entity.entity_name}'"
        assert entity.entity_type in ("location", "person"), (
            f"Unexpected entity_type '{entity.entity_type}' for '{entity.entity_name}'"
        )
        assert entity.name, f"Canonical name must be non-empty for '{entity.entity_name}'"
        assert entity.aliases, f"Aliases must be non-empty for '{entity.entity_name}'"
        assert entity.article_ids, f"article_ids must be non-empty for '{entity.entity_name}'"

        if entity.entity_type == "location":
            assert entity.location is not None, f"location must be set for '{entity.entity_name}'"
            assert entity.location.location_type in ("country", "state", "city", "region"), (
                f"Unexpected location_type '{entity.location.location_type}'"
            )
            assert entity.person is None
        else:
            assert entity.person is not None, f"person must be set for '{entity.entity_name}'"
            assert entity.location is None


def test_original_entity_name_included_in_aliases(enriched_results) -> None:
    """The original extracted entity name should always be traceable via aliases."""
    for entity in enriched_results:
        aliases_upper = [a.upper() for a in entity.aliases]
        assert entity.entity_name.upper() in aliases_upper or entity.name.upper() in aliases_upper, (
            f"Neither entity_name '{entity.entity_name}' nor canonical name '{entity.name}' "
            f"found in aliases {entity.aliases}"
        )


def test_article_ids_are_subset_of_input(unresolved_sample, enriched_results) -> None:
    """Article IDs on enriched entities must come from the input data."""
    sample_gpe, sample_persons = unresolved_sample
    all_input_article_ids = {
        article_id
        for ids in list(sample_gpe.values()) + list(sample_persons.values())
        for article_id in ids
    }

    for entity in enriched_results:
        for article_id in entity.article_ids:
            assert article_id in all_input_article_ids, (
                f"Unexpected article_id '{article_id}' on entity '{entity.entity_name}'"
            )
