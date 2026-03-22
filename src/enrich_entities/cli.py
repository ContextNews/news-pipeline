"""CLI for enriching the knowledge base with Wikidata lookups."""

from __future__ import annotations

import logging

from dotenv import load_dotenv
from context_db.connection import get_session

from enrich_entities.enrich_entities import enrich_entities
from enrich_entities.helpers import parse_enrich_entities_args, group_by_entity_name
from common.aws import (
    load_entities_for_resolution,
    load_location_aliases,
    load_person_aliases,
    upload_enriched_entities,
)
from common.cli_helpers import setup_logging
from common.local_io import save_jsonl_records_local

load_dotenv()

setup_logging()
logger = logging.getLogger(__name__)


def main() -> None:
    args = parse_enrich_entities_args()

    gpe_entities, person_entities = load_entities_for_resolution(
        args.published_date, overwrite=False
    )

    if not gpe_entities and not person_entities:
        logger.warning("No entities found for date %s", args.published_date)
        return

    gpe_by_name = group_by_entity_name(gpe_entities)
    person_by_name = group_by_entity_name(person_entities)

    existing_location_aliases = load_location_aliases(set(gpe_by_name.keys()))
    existing_person_aliases = load_person_aliases(set(person_by_name.keys()))

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
        "Found %d unresolved GPE and %d unresolved PERSON entities",
        len(unresolved_gpe),
        len(unresolved_persons),
    )

    if not unresolved_gpe and not unresolved_persons:
        logger.info("All entities already in KB")
        return

    enriched = enrich_entities(unresolved_gpe, unresolved_persons, delay=args.delay)

    if not enriched:
        logger.warning("No entities enriched from Wikidata")
        return

    if args.load_rds:
        with get_session() as session:
            upload_enriched_entities(enriched, session, args.overwrite)

    if args.load_local:
        save_jsonl_records_local(enriched, "enriched_entities")


if __name__ == "__main__":
    main()
