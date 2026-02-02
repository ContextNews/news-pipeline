"""CLI for resolving article locations from GPE entities."""

from __future__ import annotations

import argparse
import logging
import os
from collections import defaultdict
from datetime import date, datetime, timezone

from dotenv import load_dotenv

from common.aws import build_s3_key, upload_jsonl_to_s3
from common.cli_helpers import date_to_range, parse_date, save_jsonl_local, setup_logging
from common.serialization import serialize_dataclass
from resolve_article_locations.models import LocationCandidate
from resolve_article_locations.resolve_article_locations import resolve_article_locations

setup_logging()
logger = logging.getLogger(__name__)


def _load_gpe_entities_from_rds(
    published_date: date, overwrite: bool
) -> dict[str, list[str]]:
    """
    Load GPE entities grouped by article.

    Returns: {article_id: [entity_name1, entity_name2, ...]}
    """
    from sqlalchemy import text

    from rds_postgres.connection import get_session

    start, end = date_to_range(published_date)

    logger.info(
        "Loading GPE entities for articles published from %s to %s",
        start.isoformat(),
        end.isoformat(),
    )
    with get_session() as session:
        stmt = text(
            """
            SELECT
                ae.article_id,
                ae.entity_name
            FROM article_entities ae
            JOIN articles a ON a.id = ae.article_id
            WHERE a.published_at >= :start
              AND a.published_at < :end
              AND ae.entity_type = 'GPE'
              AND (:overwrite OR NOT EXISTS (
                    SELECT 1 FROM article_locations al
                    WHERE al.article_id = ae.article_id
              ))
            """
        )
        results = session.execute(
            stmt,
            {"start": start, "end": end, "overwrite": overwrite},
        ).all()

    article_entities: dict[str, list[str]] = defaultdict(list)
    for row in results:
        article_entities[row.article_id].append(row.entity_name.upper())

    logger.info(
        "Loaded %d GPE entities from %d articles",
        sum(len(v) for v in article_entities.values()),
        len(article_entities),
    )
    return dict(article_entities)


def _load_alias_to_locations_from_rds() -> dict[str, list[LocationCandidate]]:
    """
    Load all aliases with their candidate locations.

    Returns: {alias: [LocationCandidate, ...]}
    """
    from sqlalchemy import text

    from rds_postgres.connection import get_session

    logger.info("Loading location aliases from RDS")
    with get_session() as session:
        stmt = text(
            """
            SELECT
                UPPER(la.alias) as alias,
                la.wikidata_qid,
                l.name,
                l.location_type,
                l.country_code
            FROM location_aliases la
            JOIN locations l ON l.wikidata_qid = la.wikidata_qid
            """
        )
        results = session.execute(stmt).all()

    alias_to_locations: dict[str, list[LocationCandidate]] = defaultdict(list)
    for row in results:
        alias_to_locations[row.alias].append(
            LocationCandidate(
                wikidata_qid=row.wikidata_qid,
                name=row.name,
                location_type=row.location_type,
                country_code=row.country_code,
            )
        )

    logger.info(
        "Loaded %d aliases mapping to %d location candidates",
        len(alias_to_locations),
        sum(len(v) for v in alias_to_locations.values()),
    )
    return dict(alias_to_locations)


def _delete_article_locations(session: object, article_ids: list[str]) -> None:
    if not article_ids:
        return
    from sqlalchemy import text

    session.execute(
        text(
            """
            DELETE FROM article_locations
            WHERE article_id = ANY(:article_ids)
            """
        ),
        {"article_ids": article_ids},
    )


def _insert_article_locations(session: object, records: list[dict[str, str]]) -> None:
    if not records:
        return
    from sqlalchemy import text

    session.execute(
        text(
            """
            INSERT INTO article_locations (article_id, wikidata_qid, name)
            VALUES (:article_id, :wikidata_qid, :name)
            ON CONFLICT DO NOTHING
            """
        ),
        records,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--published-date",
        type=lambda v: parse_date(v, "published-date"),
        default=datetime.now(timezone.utc).date(),
        help="Resolve locations for articles published on this date (UTC, YYYY-MM-DD)",
    )
    parser.add_argument(
        "--overwrite",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Re-resolve locations for articles that already have locations",
    )
    parser.add_argument("--load-s3", action="store_true", help="Upload results to S3")
    parser.add_argument(
        "--load-rds", action="store_true", help="Load locations into RDS"
    )
    parser.add_argument(
        "--load-local", action="store_true", help="Save results to local file"
    )
    args = parser.parse_args()

    load_dotenv()

    article_entities = _load_gpe_entities_from_rds(args.published_date, args.overwrite)
    if not article_entities:
        logger.warning("No articles to process")
        return

    alias_to_locations = _load_alias_to_locations_from_rds()
    if not alias_to_locations:
        logger.warning("No location aliases found")
        return

    locations = resolve_article_locations(article_entities, alias_to_locations)
    if not locations:
        logger.warning("No locations resolved")
        return

    logger.info("Resolved %d article locations", len(locations))

    now = datetime.now(timezone.utc)
    records = [serialize_dataclass(loc) for loc in locations]

    if args.load_s3:
        bucket = os.environ["S3_BUCKET_NAME"]
        key = build_s3_key(
            "article_locations",
            now,
            f"article_locations_{now.strftime('%Y_%m_%d_%H_%M')}.jsonl",
        )
        upload_jsonl_to_s3(records, bucket, key)
        logger.info("Uploaded %d locations to s3://%s/%s", len(records), bucket, key)

    if args.load_rds:
        from rds_postgres.connection import get_session

        article_ids = sorted({loc["article_id"] for loc in records})

        with get_session() as session:
            if args.overwrite:
                _delete_article_locations(session, article_ids)
            _insert_article_locations(session, records)
            session.commit()

        logger.info("Upserted %d article locations into RDS", len(records))

    if args.load_local:
        filepath = save_jsonl_local(records, "article_locations", now)
        logger.info("Saved %d locations to %s", len(records), filepath)


if __name__ == "__main__":
    main()
