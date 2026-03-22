"""Helper functions for enrich_entities CLI."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from common.cli_helpers import parse_date


def parse_enrich_entities_args() -> argparse.Namespace:
    """Parse CLI arguments for enrich_entities."""

    parser = argparse.ArgumentParser()

    # Input options
    parser.add_argument(
        "--published-date",
        type=lambda v: parse_date(v, "published-date"),
        default=datetime.now(timezone.utc).date(),
        help="Enrich entities from articles published on this date (UTC, YYYY-MM-DD)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-enrich entities that are already in the KB",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Seconds between Wikidata API calls (default: 0.5)",
    )

    # Output options
    parser.add_argument(
        "--load-rds",
        action="store_true",
        help="Write new entities to KB and link to articles in RDS",
    )
    parser.add_argument(
        "--load-local",
        action="store_true",
        help="Save results to local file",
    )

    return parser.parse_args()


def group_by_entity_name(
    entity_dict: dict[str, list[str]],
) -> dict[str, list[str]]:
    """Convert {article_id: [entity_name, ...]} to {entity_name: [article_id, ...]}."""
    result: dict[str, list[str]] = {}
    for article_id, names in entity_dict.items():
        for name in names:
            result.setdefault(name, []).append(article_id)
    return result
