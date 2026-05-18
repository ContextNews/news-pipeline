"""CLI for resolving entities (GPE -> locations, PERSON -> persons)."""

from __future__ import annotations

import logging

from dotenv import load_dotenv

from resolve_entities.resolve_entities import resolve_entities
from resolve_entities.helpers import parse_resolve_entities_args, persist_resolved_entities
from common.db_io import load_resolution_inputs
from common.cli_helpers import setup_logging

load_dotenv()

setup_logging()
logger = logging.getLogger(__name__)


def main() -> None:
    args = parse_resolve_entities_args()

    (
        gpe_entities,
        person_entities,
        org_entities,
        alias_to_states,
        alias_to_locations,
        alias_to_persons,
        alias_to_organizations,
    ) = load_resolution_inputs(args.published_date, args.overwrite)

    if not gpe_entities and not person_entities and not org_entities:
        logger.warning("No entities to resolve")
        return

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

    persist_resolved_entities(args, locations, states, persons, organizations)


if __name__ == "__main__":
    main()
