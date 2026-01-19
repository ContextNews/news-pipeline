"""CLI for extracting named entities from articles."""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

from common.aws import build_s3_key, upload_jsonl_to_s3
from common.serialization import serialize_dataclass
from extract_entities.extract_entities import extract_entities

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "en_core_web_sm"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def _parse_ingested_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("ingested-date must be YYYY-MM-DD") from exc


def _load_articles_from_rds(ingested_date: date, overwrite: bool) -> list[dict[str, object]]:
    """Load articles from RDS for a specific ingested date (UTC)."""
    from sqlalchemy import text

    from rds_postgres.connection import get_session

    start = datetime.combine(ingested_date, datetime.min.time())
    end = start + timedelta(days=1)

    logger.info("Loading articles ingested from %s to %s", start.isoformat(), end.isoformat())
    with get_session() as session:
        stmt = text(
            """
            SELECT
                a.id,
                a.title,
                a.summary,
                a.text
            FROM articles a
            WHERE a.ingested_at >= :start
              AND a.ingested_at < :end
              AND a.text IS NOT NULL
              AND (:overwrite OR NOT EXISTS (
                    SELECT 1
                    FROM article_entities ae
                    WHERE ae.article_id = a.id
                ))
            """
        )
        results = session.execute(
            stmt,
            {"start": start, "end": end, "overwrite": overwrite},
        ).mappings().all()
        articles = [dict(row) for row in results]

    logger.info("Loaded %d articles from RDS", len(articles))
    return articles


def _delete_article_entities(session: object, article_ids: list[str]) -> None:
    if not article_ids:
        return
    from sqlalchemy import text

    session.execute(
        text(
            """
            DELETE FROM article_entities
            WHERE article_id = ANY(:article_ids)
            """
        ),
        {"article_ids": article_ids},
    )


def _insert_entities(session: object, entities: list[dict[str, str]]) -> None:
    if not entities:
        return
    from sqlalchemy import text

    session.execute(
        text(
            """
            INSERT INTO entities (type, name)
            VALUES (:entity_type, :entity_name)
            ON CONFLICT DO NOTHING
            """
        ),
        entities,
    )


def _insert_article_entities(session: object, records: list[dict[str, str]]) -> None:
    if not records:
        return
    from sqlalchemy import text

    session.execute(
        text(
            """
            INSERT INTO article_entities (
                article_id,
                entity_type,
                entity_name,
                entity_count,
                entity_in_article_title
            )
            VALUES (
                :article_id,
                :entity_type,
                :entity_name,
                :entity_count,
                :entity_in_article_title
            )
            ON CONFLICT DO NOTHING
            """
        ),
        records,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ingested-date",
        type=_parse_ingested_date,
        default=datetime.now(timezone.utc).date(),
        help="UTC date (YYYY-MM-DD)",
    )
    parser.add_argument("--model", default=DEFAULT_MODEL, help=f"spaCy model to use (default: {DEFAULT_MODEL})")
    parser.add_argument("--batch-size", type=int, default=32, help="Batch size for spaCy NER (default: 32)")
    parser.add_argument(
        "--word-limit",
        type=int,
        default=300,
        help="Maximum number of words to extract entities from (default: 300)",
    )
    parser.add_argument(
        "--overwrite",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Re-extract entities for articles that already have entities",
    )
    parser.add_argument("--load-s3", action="store_true", help="Upload results to S3")
    parser.add_argument("--load-rds", action="store_true", help="Load entities into RDS")
    parser.add_argument("--load-local", action="store_true", help="Save results to local file")
    args = parser.parse_args()

    load_dotenv()

    articles = _load_articles_from_rds(args.ingested_date, args.overwrite)
    if not articles:
        logger.warning("No articles to process")
        return

    entities = extract_entities(
        articles=articles,
        model=args.model,
        batch_size=args.batch_size,
        word_limit=args.word_limit,
    )
    if not entities:
        logger.warning("No entities extracted")
        return

    now = datetime.now(timezone.utc)
    records = []
    for entity in entities:
        record = serialize_dataclass(entity)
        if not record.get("aliases"):
            record.pop("aliases", None)
        records.append(record)

    if args.load_s3:
        bucket = os.environ["S3_BUCKET_NAME"]
        key = build_s3_key(
            "article_entities",
            now,
            f"article_entities_{now.strftime('%Y_%m_%d_%H_%M')}.jsonl",
        )
        upload_jsonl_to_s3(records, bucket, key)
        logger.info("Uploaded %d entities to s3://%s/%s", len(records), bucket, key)

    if args.load_rds:
        from rds_postgres.connection import get_session

        article_ids = sorted({entity["article_id"] for entity in records})
        unique_entities = {
            (entity["entity_type"], entity["entity_name"])
            for entity in records
        }
        entity_rows = [
            {"entity_type": entity_type, "entity_name": entity_name}
            for entity_type, entity_name in unique_entities
        ]
        article_entity_rows = [
            {
                "article_id": record["article_id"],
                "entity_type": record["entity_type"],
                "entity_name": record["entity_name"],
                "entity_count": record["count"],
                "entity_in_article_title": record["in_title"],
            }
            for record in records
        ]

        with get_session() as session:
            if args.overwrite:
                _delete_article_entities(session, article_ids)
            _insert_entities(session, entity_rows)
            _insert_article_entities(session, article_entity_rows)
            session.commit()

        logger.info(
            "Upserted %d entity definitions and %d article entities into RDS",
            len(entity_rows),
            len(records),
        )

    if args.load_local:
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        filename = f"article_entities_{now.strftime('%Y_%m_%d_%H_%M')}.jsonl"
        filepath = output_dir / filename
        with filepath.open("w") as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        logger.info("Saved %d entities to %s", len(records), filepath)


if __name__ == "__main__":
    main()
