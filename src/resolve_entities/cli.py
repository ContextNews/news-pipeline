"""CLI for resolving entities (GPE -> locations, PERSON -> persons)."""

from __future__ import annotations

import logging

from dotenv import load_dotenv
from context_db.connection import get_session

from resolve_entities.resolve_entities import resolve_entities
from resolve_entities.helpers import parse_resolve_entities_args
from common.db_io import (
    load_entities_for_resolution,
    load_location_aliases,
    load_organization_aliases,
    load_person_aliases,
    load_state_aliases,
    upload_resolved_locations,
    upload_resolved_organizations,
    upload_resolved_persons,
    upload_resolved_states,
)
from common.object_storage import upload_jsonl_records_to_object_store
from common.cli_helpers import setup_logging
from common.local_io import save_jsonl_records_local

load_dotenv()

setup_logging()
logger = logging.getLogger(__name__)


def main() -> None:
    args = parse_resolve_entities_args()

    gpe_entities, person_entities, org_entities = load_entities_for_resolution(
        args.published_date, args.overwrite
    )

    if not gpe_entities and not person_entities and not org_entities:
        logger.warning("No entities to resolve")
        return

    gpe_names = {name for names in gpe_entities.values() for name in names}
    person_names = {name for names in person_entities.values() for name in names}
    org_names = {name for names in org_entities.values() for name in names}

    alias_to_states = load_state_aliases(gpe_names)
    alias_to_locations = load_location_aliases(gpe_names)
    alias_to_persons = load_person_aliases(person_names)
    alias_to_organizations = load_organization_aliases(org_names)

    if (
        not alias_to_states
        and not alias_to_locations
        and not alias_to_persons
        and not alias_to_organizations
    ):
        logger.warning("No alias reference data found")
        return

    locations, states, persons, organizations = resolve_entities(
        gpe_entities,
        person_entities,
        alias_to_locations,
        alias_to_persons,
        article_org_entities=org_entities,
        alias_to_organizations=alias_to_organizations,
        alias_to_states=alias_to_states,
    )

    if not locations and not states and not persons and not organizations:
        logger.warning("No entities resolved")
        return

    if locations:
        if args.load_object_store:
            upload_jsonl_records_to_object_store(locations, "article_locations")

        if args.load_local:
            save_jsonl_records_local(locations, "article_locations")

    if states:
        if args.load_object_store:
            upload_jsonl_records_to_object_store(states, "article_states")

        if args.load_local:
            save_jsonl_records_local(states, "article_states")

    if persons:
        if args.load_object_store:
            upload_jsonl_records_to_object_store(persons, "article_persons")

        if args.load_local:
            save_jsonl_records_local(persons, "article_persons")

    if organizations:
        if args.load_object_store:
            upload_jsonl_records_to_object_store(organizations, "article_organizations")

        if args.load_local:
            save_jsonl_records_local(organizations, "article_organizations")

    if args.load_db:
        with get_session() as session:
            if locations:
                upload_resolved_locations(locations, session, args.overwrite)
            if states:
                upload_resolved_states(states, session, args.overwrite)
            if persons:
                upload_resolved_persons(persons, session, args.overwrite)
            if organizations:
                upload_resolved_organizations(organizations, session, args.overwrite)


if __name__ == "__main__":
    main()
