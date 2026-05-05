"""CLI for enriching the knowledge base with Wikidata lookups."""

from __future__ import annotations

import logging

from context_db.connection import get_session

from enrich_entities.enrich_entities import enrich_entities
from enrich_entities.helpers import parse_enrich_entities_args, group_by_entity_name
from common.aws import (
    load_entities_for_resolution,
    load_location_aliases,
    load_organization_aliases,
    load_person_aliases,
    upload_enriched_entities,
)
from common.cli_helpers import setup_logging
from common.local_io import save_jsonl_records_local

setup_logging()
logger = logging.getLogger(__name__)


def main() -> None:
    args = parse_enrich_entities_args()

    gpe_entities, person_entities, org_entities = load_entities_for_resolution(
        args.published_date, overwrite=False
    )

    if not gpe_entities and not person_entities and not org_entities:
        logger.warning("No entities found for date %s", args.published_date)
        return

    gpe_by_name = group_by_entity_name(gpe_entities)
    person_by_name = group_by_entity_name(person_entities)
    org_by_name = group_by_entity_name(org_entities)

    existing_location_aliases = load_location_aliases(set(gpe_by_name.keys()))
    existing_person_aliases = load_person_aliases(set(person_by_name.keys()))
    existing_org_aliases = load_organization_aliases(set(org_by_name.keys()))

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
    unresolved_orgs = {
        name: ids
        for name, ids in org_by_name.items()
        if name not in existing_org_aliases
    }

    logger.info(
        "Found %d unresolved GPE, %d unresolved PERSON, and %d unresolved ORG entities",
        len(unresolved_gpe),
        len(unresolved_persons),
        len(unresolved_orgs),
    )

    if not unresolved_gpe and not unresolved_persons and not unresolved_orgs:
        logger.info("All entities already in KB")
        return

    enriched = enrich_entities(unresolved_gpe, unresolved_persons, unresolved_orgs, delay=args.delay)

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
