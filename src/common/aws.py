import gzip
import json
import logging
import os
from datetime import date, datetime
from typing import Iterable, Iterator, Mapping, Any

import boto3
from dotenv import load_dotenv

from common.cli_helpers import date_to_range

load_dotenv()

logger = logging.getLogger(__name__)


def get_s3_client():
    """Create S3 client."""
    return boto3.client("s3")


def build_s3_key(prefix: str, timestamp: datetime, filename: str) -> str:
    """Build a partitioned S3 key path."""
    return (
        f"{prefix}/"
        f"year={timestamp.year:04d}/"
        f"month={timestamp.month:02d}/"
        f"day={timestamp.day:02d}/"
        f"{filename}"
    )


def upload_jsonl_to_s3(
    records: Iterable[Mapping[str, Any]],
    bucket: str,
    key: str,
) -> None:
    """Upload in-memory records to S3 as JSONL."""
    body = "\n".join(json.dumps(record, ensure_ascii=False) for record in records) + "\n"

    s3 = get_s3_client()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/jsonl",
    )


def upload_jsonl_records_to_s3(records: list[Any], prefix: str) -> None:
    """
    Upload a list of dataclass records to S3 as JSONL.

    Handles serialization, builds the S3 key, and logs the result.

    Args:
        records: List of dataclass objects to upload
        prefix: S3 prefix (e.g., "ingested_articles", "embedded_articles")
    """
    import logging
    from datetime import timezone
    from common.serialization import serialize_dataclass

    logger = logging.getLogger(__name__)

    bucket = os.environ["S3_BUCKET_NAME"]
    now = datetime.now(timezone.utc)
    filename = f"{prefix}_{now.strftime('%Y_%m_%d_%H_%M')}.jsonl"
    key = build_s3_key(prefix, now, filename)

    serialized = [serialize_dataclass(record) for record in records]
    upload_jsonl_to_s3(serialized, bucket, key)

    logger.info("Uploaded %d records to s3://%s/%s", len(records), bucket, key)


def upload_csv_to_s3(csv_content: str, bucket: str, key: str) -> None:
    """Upload CSV string to S3."""
    s3 = get_s3_client()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_content.encode("utf-8"),
        ContentType="text/csv",
    )


def list_s3_jsonl_files(bucket: str, prefix: str) -> list[str]:
    """List all .jsonl files under an S3 prefix."""
    s3 = get_s3_client()
    files = []
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".jsonl") or key.endswith(".jsonl.gz"):
                files.append(key)

    return files


def read_jsonl_from_s3(bucket: str, key: str) -> Iterator[dict]:
    """Read JSONL file from S3, handling gzip if needed."""
    s3 = get_s3_client()
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read()

    if key.endswith(".gz"):
        content = gzip.decompress(content)

    for line in content.decode("utf-8").splitlines():
        line = line.strip()
        if line:
            yield json.loads(line)


def upload_articles(articles: list[Any], session: Any) -> None:
    """
    Upload articles to RDS PostgreSQL.

    Handles insertion with duplicate detection and logs the result.

    Args:
        articles: List of article objects or dicts with fields:
            id, source, title, summary, url, published_at, ingested_at, text
        session: SQLAlchemy session
    """
    import logging
    from sqlalchemy.dialects.postgresql import insert
    from rds_postgres.models import Article

    logger = logging.getLogger(__name__)
    inserted = 0
    skipped = 0

    for article in articles:
        if hasattr(article, "id"):
            article_id = article.id
            url = article.url
            data = {
                "id": article_id,
                "source": article.source,
                "title": article.title,
                "summary": article.summary,
                "url": url,
                "published_at": article.published_at,
                "ingested_at": article.ingested_at,
                "text": article.text,
            }
        else:
            article_id = article["id"]
            url = article["url"]
            data = {
                "id": article_id,
                "source": article["source"],
                "title": article["title"],
                "summary": article["summary"],
                "url": url,
                "published_at": article["published_at"],
                "ingested_at": article["ingested_at"],
                "text": article.get("text"),
            }

        stmt = insert(Article).values(**data).on_conflict_do_nothing()
        result = session.execute(stmt)

        if result.rowcount > 0:
            inserted += 1
        else:
            logger.warning("Skipped duplicate article: id=%s url=%s", article_id, url)
            skipped += 1

    session.commit()
    logger.info("Loaded %d articles to RDS (%d skipped as duplicates)", inserted, skipped)


def load_ingested_articles(
    published_date: date,
    model: str,
    overwrite: bool,
) -> list[dict]:
    """
    Load articles from RDS for a specific published date (UTC).

    Args:
        published_date: Date to load articles for
        model: Embedding model name (used to filter already-embedded articles)
        overwrite: If True, include articles that already have embeddings

    Returns:
        List of article dicts with fields: id, source, title, summary, url,
        published_at, ingested_at, text
    """
    from sqlalchemy import text
    from rds_postgres.connection import get_session

    start, end = date_to_range(published_date)

    logger.info("Loading articles published from %s to %s", start.isoformat(), end.isoformat())
    with get_session() as session:
        stmt = text(
            """
            SELECT
                a.id,
                a.source,
                a.title,
                a.summary,
                a.url,
                a.published_at,
                a.ingested_at,
                a.text
            FROM articles a
            WHERE a.published_at >= :start
              AND a.published_at < :end
              AND (:overwrite OR NOT EXISTS (
                    SELECT 1
                    FROM article_embeddings e
                    WHERE e.article_id = a.id
                      AND e.embedding_model = :model
                ))
            """
        )
        results = session.execute(
            stmt,
            {"start": start, "end": end, "overwrite": overwrite, "model": model},
        ).mappings().all()
        articles = [dict(row) for row in results]

    logger.info("Loaded %d articles from RDS", len(articles))
    return articles


def upload_embeddings(embeddings: list[Any], session: Any) -> None:
    """
    Upload article embeddings to RDS PostgreSQL.

    Handles upsert (update existing or insert new) and logs the result.

    Args:
        embeddings: List of embedding objects with fields:
            id, embedded_text, embedding, embedding_model
        session: SQLAlchemy session
    """
    from sqlalchemy import text

    updated = 0
    inserted = 0

    for embedding in embeddings:
        params = {
            "article_id": embedding.id,
            "embedded_text": embedding.embedded_text,
            "embedding": embedding.embedding,
            "embedding_model": embedding.embedding_model,
        }
        update_stmt = text(
            """
            UPDATE article_embeddings
            SET embedded_text = :embedded_text,
                embedding = :embedding
            WHERE article_id = :article_id
              AND embedding_model = :embedding_model
            """
        )
        result = session.execute(update_stmt, params)
        if result.rowcount and result.rowcount > 0:
            updated += 1
            continue

        insert_stmt = text(
            """
            INSERT INTO article_embeddings
                (article_id, embedded_text, embedding, embedding_model)
            VALUES
                (:article_id, :embedded_text, :embedding, :embedding_model)
            """
        )
        session.execute(insert_stmt, params)
        inserted += 1

    session.commit()
    logger.info("Upserted %d embeddings to RDS (%d updated, %d inserted)", updated + inserted, updated, inserted)


def load_articles_for_entities(published_date: date, overwrite: bool) -> list[dict]:
    """
    Load articles from RDS for entity extraction.

    Args:
        published_date: Date to load articles for
        overwrite: If True, include articles that already have entities

    Returns:
        List of article dicts with fields: id, title, summary, text
    """
    from sqlalchemy import text
    from rds_postgres.connection import get_session

    start, end = date_to_range(published_date)

    logger.info("Loading articles published from %s to %s", start.isoformat(), end.isoformat())
    with get_session() as session:
        stmt = text(
            """
            SELECT
                a.id,
                a.title,
                a.summary,
                a.text
            FROM articles a
            WHERE a.published_at >= :start
              AND a.published_at < :end
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


def upload_entities(entities: list[Any], session: Any, overwrite: bool = False) -> None:
    """
    Upload article entities to RDS PostgreSQL.

    Handles deletion of existing entities (if overwrite), insertion of new entity
    definitions, and insertion of article-entity relationships.

    Args:
        entities: List of entity objects with fields:
            article_id, entity_type, entity_name, count, in_title
        session: SQLAlchemy session
        overwrite: If True, delete existing entities for these articles first
    """
    from sqlalchemy import text

    if not entities:
        logger.warning("No entities to upload")
        return

    # Get unique article IDs
    article_ids = sorted({entity.article_id for entity in entities})

    # Delete existing entities if overwrite
    if overwrite and article_ids:
        session.execute(
            text("DELETE FROM article_entities WHERE article_id = ANY(:article_ids)"),
            {"article_ids": article_ids},
        )

    # Insert unique entity definitions
    unique_entities = {(entity.entity_type, entity.entity_name) for entity in entities}
    entity_rows = [
        {"entity_type": entity_type, "entity_name": entity_name}
        for entity_type, entity_name in unique_entities
    ]
    if entity_rows:
        session.execute(
            text(
                """
                INSERT INTO entities (type, name)
                VALUES (:entity_type, :entity_name)
                ON CONFLICT DO NOTHING
                """
            ),
            entity_rows,
        )

    # Insert article-entity relationships
    article_entity_rows = [
        {
            "article_id": entity.article_id,
            "entity_type": entity.entity_type,
            "entity_name": entity.entity_name,
            "entity_count": entity.count,
            "entity_in_article_title": entity.in_title,
        }
        for entity in entities
    ]
    if article_entity_rows:
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
            article_entity_rows,
        )

    session.commit()
    logger.info(
        "Upserted %d entity definitions and %d article entities into RDS",
        len(entity_rows),
        len(article_entity_rows),
    )
