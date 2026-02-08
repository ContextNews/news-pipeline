"""CLI for resolving entities (GPE -> locations, PERSON -> persons)."""

from __future__ import annotations

import logging

from dotenv import load_dotenv
from rds_postgres.connection import get_session

from resolve_entities.resolve_entities import resolve_entities
from resolve_entities.helpers import parse_resolve_entities_args
from common.aws import (
    load_entities_for_resolution,
    load_location_aliases,
    load_person_aliases,
    upload_resolved_locations,
    upload_resolved_persons,
    upload_jsonl_records_to_s3,
)
from common.cli_helpers import setup_logging
from common.local_io import save_jsonl_records_local

load_dotenv()

setup_logging()
logger = logging.getLogger(__name__)


def main() -> None:
    args = parse_resolve_entities_args()

    gpe_entities, person_entities = load_entities_for_resolution(
        args.published_date, args.overwrite
    )

    if not gpe_entities and not person_entities:
        logger.warning("No entities to resolve")
        return

    alias_to_locations = load_location_aliases()
    alias_to_persons = load_person_aliases()

    if not alias_to_locations and not alias_to_persons:
        logger.warning("No alias reference data found")
        return

    locations, persons = resolve_entities(
        gpe_entities, person_entities, alias_to_locations, alias_to_persons
    )

    if not locations and not persons:
        logger.warning("No entities resolved")
        return

    if locations:
        if args.load_s3:
            upload_jsonl_records_to_s3(locations, "article_locations")

        if args.load_local:
            save_jsonl_records_local(locations, "article_locations")

    if persons:
        if args.load_s3:
            upload_jsonl_records_to_s3(persons, "article_persons")

        if args.load_local:
            save_jsonl_records_local(persons, "article_persons")

    if args.load_rds:
        with get_session() as session:
            if locations:
                upload_resolved_locations(locations, session, args.overwrite)
            if persons:
                upload_resolved_persons(persons, session, args.overwrite)


if __name__ == "__main__":
    main()
