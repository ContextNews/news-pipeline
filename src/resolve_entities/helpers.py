"""Helper functions for resolve_entities CLI."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Any

from common.cli_helpers import parse_date


def persist_resolved_entities(
    args: argparse.Namespace,
    locations: list[Any],
    states: list[Any],
    persons: list[Any],
    organizations: list[Any],
) -> None:
    """Write resolved entity results to all requested outputs."""
    from common.object_storage import upload_jsonl_records_to_object_store
    from common.local_io import save_jsonl_records_local
    from common.db_io import (
        upload_resolved_locations,
        upload_resolved_organizations,
        upload_resolved_persons,
        upload_resolved_states,
    )
    from context_db.connection import get_session

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


def parse_resolve_entities_args() -> argparse.Namespace:
    """Parse CLI arguments for resolve_entities."""

    parser = argparse.ArgumentParser()

    # Input options
    parser.add_argument(
        "--published-date",
        type=lambda v: parse_date(v, "published-date"),
        default=datetime.now(timezone.utc).date(),
        help="Resolve entities for articles published on this date (UTC, YYYY-MM-DD)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-resolve entities for articles that already have resolved entities",
    )

    # Output options
    parser.add_argument("--load-object-store", action="store_true", help="Upload results to object store")
    parser.add_argument(
        "--load-db", action="store_true", help="Load resolved entities into DB"
    )
    parser.add_argument(
        "--load-local", action="store_true", help="Save results to local file"
    )

    return parser.parse_args()
