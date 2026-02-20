import gzip
import json
import logging
import os
from datetime import date, datetime, timezone
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


def load_articles_with_embeddings(
    ingested_date: date,
    embedding_model: str,
) -> list[dict]:
    """
    Load articles with embeddings from RDS for a specific ingested date (UTC).

    Args:
        ingested_date: Date to load articles for
        embedding_model: Embedding model to filter by

    Returns:
        List of article dicts with fields: id, source, title, summary, url,
        published_at, ingested_at, text, embedding, embedding_model
    """
    from sqlalchemy import text
    from rds_postgres.connection import get_session

    start, end = date_to_range(ingested_date)

    logger.info("Loading articles ingested from %s to %s", start.isoformat(), end.isoformat())
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
                a.text,
                e.embedding,
                e.embedding_model
            FROM articles a
            JOIN article_embeddings e ON e.article_id = a.id
            WHERE a.ingested_at >= :start
              AND a.ingested_at < :end
              AND e.embedding_model = :model
            """
        )
        results = session.execute(
            stmt,
            {"start": start, "end": end, "model": embedding_model},
        ).mappings().all()
        articles = [dict(row) for row in results]

    logger.info("Loaded %d articles with embeddings", len(articles))
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


def load_clusters(cluster_period: date) -> list[dict[str, Any]]:
    """Load article clusters and their articles from RDS for a specific cluster period (UTC)."""
    from sqlalchemy import text
    from rds_postgres.connection import get_session

    start, end = date_to_range(cluster_period)

    logger.info("Loading clusters from %s to %s", start.isoformat(), end.isoformat())

    with get_session() as session:
        clusters_stmt = text(
            """
            SELECT article_cluster_id, cluster_period
            FROM article_clusters
            WHERE cluster_period >= :start
              AND cluster_period < :end
            """
        )
        cluster_results = session.execute(
            clusters_stmt,
            {"start": start, "end": end},
        ).mappings().all()

        if not cluster_results:
            return []

        cluster_ids = [row["article_cluster_id"] for row in cluster_results]

        articles_stmt = text(
            """
            SELECT
                aca.article_cluster_id,
                a.id,
                a.source,
                a.title,
                a.summary,
                a.url,
                a.published_at,
                a.text
            FROM article_cluster_articles aca
            JOIN articles a ON a.id = aca.article_id
            WHERE aca.article_cluster_id = ANY(:cluster_ids)
            """
        )
        article_results = session.execute(
            articles_stmt,
            {"cluster_ids": cluster_ids},
        ).mappings().all()

    # Group articles by cluster
    clusters_map: dict[str, list[dict[str, Any]]] = {}
    for row in article_results:
        cluster_id = row["article_cluster_id"]
        article = {
            "id": row["id"],
            "source": row["source"],
            "title": row["title"],
            "summary": row["summary"],
            "url": row["url"],
            "published_at": row["published_at"],
            "text": row["text"],
        }
        clusters_map.setdefault(cluster_id, []).append(article)

    clusters = []
    for cluster_row in cluster_results:
        cluster_id = cluster_row["article_cluster_id"]
        if cluster_id in clusters_map:
            clusters.append({
                "cluster_id": cluster_id,
                "cluster_period": cluster_row["cluster_period"],
                "articles": clusters_map[cluster_id],
            })

    logger.info("Loaded %d clusters with %d total articles", len(clusters), len(article_results))
    return clusters


def load_article_locations(article_ids: list[str]) -> dict[str, list[str]]:
    """Load locations for articles. Returns {article_id: [wikidata_qid, ...]}."""
    from sqlalchemy import text
    from rds_postgres.connection import get_session

    if not article_ids:
        return {}

    with get_session() as session:
        stmt = text(
            """
            SELECT article_id, wikidata_qid
            FROM article_locations
            WHERE article_id = ANY(:article_ids)
            """
        )
        results = session.execute(stmt, {"article_ids": article_ids}).mappings().all()

    article_locations: dict[str, list[str]] = {}
    for row in results:
        article_locations.setdefault(row["article_id"], []).append(row["wikidata_qid"])

    logger.info("Loaded locations for %d articles", len(article_locations))
    return article_locations


def load_article_persons(article_ids: list[str]) -> dict[str, list[str]]:
    """Load persons for articles. Returns {article_id: [wikidata_qid, ...]}."""
    from sqlalchemy import text
    from rds_postgres.connection import get_session

    if not article_ids:
        return {}

    with get_session() as session:
        stmt = text(
            """
            SELECT article_id, wikidata_qid
            FROM article_persons
            WHERE article_id = ANY(:article_ids)
            """
        )
        results = session.execute(stmt, {"article_ids": article_ids}).mappings().all()

    article_persons: dict[str, list[str]] = {}
    for row in results:
        article_persons.setdefault(row["article_id"], []).append(row["wikidata_qid"])

    logger.info("Loaded persons for %d articles", len(article_persons))
    return article_persons


def upload_stories(
    stories: list[dict[str, Any]],
    session: Any,
    cluster_period: date,
    overwrite: bool = True,
) -> None:
    """
    Upload generated stories to RDS PostgreSQL.

    Handles deletion of existing stories (if overwrite) and insertion of new stories
    with their article links and location references.

    Args:
        stories: List of story dicts with fields: story_id, title, summary,
            key_points, story_period, article_ids, location_qid
        session: SQLAlchemy session
        cluster_period: Date used to determine story_period for deletion
        overwrite: If True, delete existing stories for this period first
    """
    from sqlalchemy import text

    now = datetime.now(timezone.utc)

    if overwrite:
        start, end = date_to_range(cluster_period)
        # Delete from junction tables first (foreign key constraints)
        session.execute(
            text(
                """
                DELETE FROM story_stories
                WHERE story_id_1 IN (
                    SELECT id FROM stories
                    WHERE story_period >= :start
                      AND story_period < :end
                )
                OR story_id_2 IN (
                    SELECT id FROM stories
                    WHERE story_period >= :start
                      AND story_period < :end
                )
                """
            ),
            {"start": start, "end": end},
        )
        for table in ("story_locations", "story_persons", "article_stories", "story_topics"):
            session.execute(
                text(
                    f"""
                    DELETE FROM {table}
                    WHERE story_id IN (
                        SELECT id FROM stories
                        WHERE story_period >= :start
                          AND story_period < :end
                    )
                    """
                ),
                {"start": start, "end": end},
            )
        session.execute(
            text(
                """
                DELETE FROM stories
                WHERE story_period >= :start
                  AND story_period < :end
                """
            ),
            {"start": start, "end": end},
        )
        logger.info("Deleted existing stories for %s", cluster_period.isoformat())

    # Insert stories
    session.execute(
        text(
            """
            INSERT INTO stories (
                id, title, summary, key_points,
                story_period, generated_at, updated_at
            )
            VALUES (
                :id, :title, :summary, :key_points,
                :story_period, :generated_at, :updated_at
            )
            """
        ),
        [
            {
                "id": story["story_id"],
                "title": story["title"],
                "summary": story["summary"],
                "key_points": story["key_points"],
                "story_period": story["story_period"],
                "generated_at": now,
                "updated_at": now,
            }
            for story in stories
        ],
    )

    # Insert article_stories links
    article_story_rows = []
    for story in stories:
        for article_id in story["article_ids"]:
            article_story_rows.append({
                "article_id": article_id,
                "story_id": story["story_id"],
            })

    if article_story_rows:
        session.execute(
            text(
                """
                INSERT INTO article_stories (article_id, story_id)
                VALUES (:article_id, :story_id)
                """
            ),
            article_story_rows,
        )

    # Insert story_locations
    story_location_rows = [
        {"story_id": story["story_id"], "wikidata_qid": story["location_qid"]}
        for story in stories
        if story["location_qid"]
    ]

    if story_location_rows:
        session.execute(
            text(
                """
                INSERT INTO story_locations (story_id, wikidata_qid)
                VALUES (:story_id, :wikidata_qid)
                """
            ),
            story_location_rows,
        )
        logger.info(
            "Resolved locations for %d of %d stories",
            len(story_location_rows),
            len(stories),
        )

    # Insert story_persons
    story_person_rows = [
        {"story_id": story["story_id"], "wikidata_qid": qid}
        for story in stories
        for qid in story.get("person_qids", [])
    ]

    if story_person_rows:
        session.execute(
            text(
                """
                INSERT INTO story_persons (story_id, wikidata_qid)
                VALUES (:story_id, :wikidata_qid)
                ON CONFLICT DO NOTHING
                """
            ),
            story_person_rows,
        )
        logger.info(
            "Resolved persons for %d of %d stories",
            len({r["story_id"] for r in story_person_rows}),
            len(stories),
        )

    # Insert story_topics (if stories have been classified)
    topic_rows = []
    for story in stories:
        for topic in story.get("topics", []):
            topic_rows.append({
                "story_id": story["story_id"],
                "topic": topic,
            })

    if topic_rows:
        session.execute(
            text(
                """
                INSERT INTO story_topics (story_id, topic)
                VALUES (:story_id, :topic)
                ON CONFLICT DO NOTHING
                """
            ),
            topic_rows,
        )
        logger.info("Saved %d topic classifications to RDS", len(topic_rows))

    session.commit()
    logger.info("Saved %d stories to RDS", len(stories))


def load_entities_for_resolution(
    published_date: date,
    overwrite: bool,
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """
    Load GPE and PERSON entities from RDS for entity resolution.

    Args:
        published_date: Date to load articles for
        overwrite: If True, include articles that already have resolved entities

    Returns:
        Tuple of (gpe_entities, person_entities) where each is
        {article_id: [ENTITY_NAME, ...]} with uppercase names
    """
    from collections import defaultdict

    from sqlalchemy import text
    from rds_postgres.connection import get_session

    start, end = date_to_range(published_date)

    logger.info(
        "Loading GPE and PERSON entities for articles published from %s to %s",
        start.isoformat(),
        end.isoformat(),
    )
    with get_session() as session:
        stmt = text(
            """
            SELECT
                ae.article_id,
                ae.entity_type,
                ae.entity_name
            FROM article_entities ae
            JOIN articles a ON a.id = ae.article_id
            WHERE a.published_at >= :start
              AND a.published_at < :end
              AND ae.entity_type IN ('GPE', 'PERSON')
              AND (:overwrite OR (
                NOT EXISTS (
                    SELECT 1 FROM article_locations al
                    WHERE al.article_id = ae.article_id
                )
                OR NOT EXISTS (
                    SELECT 1 FROM article_persons ap
                    WHERE ap.article_id = ae.article_id
                )
              ))
            """
        )
        results = session.execute(
            stmt,
            {"start": start, "end": end, "overwrite": overwrite},
        ).all()

    gpe_entities: dict[str, list[str]] = defaultdict(list)
    person_entities: dict[str, list[str]] = defaultdict(list)

    for row in results:
        if row.entity_type == "GPE":
            gpe_entities[row.article_id].append(row.entity_name.upper())
        elif row.entity_type == "PERSON":
            person_entities[row.article_id].append(row.entity_name.upper())

    logger.info(
        "Loaded %d GPE entities from %d articles and %d PERSON entities from %d articles",
        sum(len(v) for v in gpe_entities.values()),
        len(gpe_entities),
        sum(len(v) for v in person_entities.values()),
        len(person_entities),
    )
    return dict(gpe_entities), dict(person_entities)


def load_location_aliases() -> dict[str, list]:
    """
    Load all location aliases with their candidate locations from RDS.

    Returns: {ALIAS_UPPER: [LocationCandidate, ...]}
    """
    from collections import defaultdict

    from sqlalchemy import text
    from rds_postgres.connection import get_session
    from resolve_entities.models import LocationCandidate

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

    logger.info("Loaded %d location aliases", len(alias_to_locations))
    return dict(alias_to_locations)


def load_person_aliases() -> dict[str, list]:
    """
    Load all person aliases with their candidate persons from RDS.

    Returns: {ALIAS_UPPER: [PersonCandidate, ...]}
    """
    from collections import defaultdict

    from sqlalchemy import text
    from rds_postgres.connection import get_session
    from resolve_entities.models import PersonCandidate

    logger.info("Loading person aliases from RDS")
    with get_session() as session:
        stmt = text(
            """
            SELECT
                UPPER(pa.alias) as alias,
                pa.wikidata_qid,
                p.name,
                p.description,
                p.nationalities
            FROM person_aliases pa
            JOIN persons p ON p.wikidata_qid = pa.wikidata_qid
            """
        )
        results = session.execute(stmt).all()

    alias_to_persons: dict[str, list[PersonCandidate]] = defaultdict(list)
    for row in results:
        alias_to_persons[row.alias].append(
            PersonCandidate(
                wikidata_qid=row.wikidata_qid,
                name=row.name,
                description=row.description,
                nationalities=row.nationalities,
            )
        )

    logger.info("Loaded %d person aliases", len(alias_to_persons))
    return dict(alias_to_persons)


def upload_resolved_locations(
    locations: list,
    session: Any,
    overwrite: bool = False,
) -> None:
    """
    Upload resolved article locations to RDS.

    Args:
        locations: List of ArticleLocation dataclass instances
        session: SQLAlchemy session
        overwrite: If True, delete existing locations for these articles first
    """
    from sqlalchemy import text

    if not locations:
        return

    article_ids = sorted({loc.article_id for loc in locations})

    if overwrite:
        session.execute(
            text("DELETE FROM article_locations WHERE article_id = ANY(:article_ids)"),
            {"article_ids": article_ids},
        )

    records = [
        {"article_id": loc.article_id, "wikidata_qid": loc.wikidata_qid, "name": loc.name}
        for loc in locations
    ]
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
    session.commit()
    logger.info("Upserted %d article locations into RDS", len(records))


def upload_resolved_persons(
    persons: list,
    session: Any,
    overwrite: bool = False,
) -> None:
    """
    Upload resolved article persons to RDS.

    Args:
        persons: List of ArticlePerson dataclass instances
        session: SQLAlchemy session
        overwrite: If True, delete existing persons for these articles first
    """
    from sqlalchemy import text

    if not persons:
        return

    article_ids = sorted({p.article_id for p in persons})

    if overwrite:
        session.execute(
            text("DELETE FROM article_persons WHERE article_id = ANY(:article_ids)"),
            {"article_ids": article_ids},
        )

    records = [
        {"article_id": p.article_id, "wikidata_qid": p.wikidata_qid, "name": p.name}
        for p in persons
    ]
    session.execute(
        text(
            """
            INSERT INTO article_persons (article_id, wikidata_qid, name)
            VALUES (:article_id, :wikidata_qid, :name)
            ON CONFLICT DO NOTHING
            """
        ),
        records,
    )
    session.commit()
    logger.info("Upserted %d article persons into RDS", len(records))


def load_articles_for_classification(
    published_date: date,
    overwrite: bool,
) -> list[dict]:
    """
    Load articles from RDS for a specific published date (UTC).

    Args:
        published_date: Date to load articles for
        overwrite: If True, include articles that already have topic assignments

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
            SELECT a.id, a.title, a.summary, a.text
            FROM articles a
            WHERE a.published_at >= :start
              AND a.published_at < :end
              AND (:overwrite OR NOT EXISTS (
                    SELECT 1
                    FROM article_topics t
                    WHERE t.article_id = a.id
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


def upload_article_topics(
    classified_articles: list[Any],
    session: Any,
    overwrite: bool = False,
) -> None:
    """
    Upload article topic classifications to RDS PostgreSQL.

    Ensures topic labels exist in the topics table, then inserts article_topics
    records. Deletes existing records first if overwrite=True.

    Args:
        classified_articles: List of ClassifiedArticle objects with article_id and topics fields
        session: SQLAlchemy session
        overwrite: If True, delete existing topic assignments for these articles first
    """
    from sqlalchemy import text

    if not classified_articles:
        logger.warning("No classified articles to upload")
        return

    # Collect all unique topics and ensure they exist
    all_topics = sorted({topic for ca in classified_articles for topic in ca.topics})
    if all_topics:
        session.execute(
            text("INSERT INTO topics (topic) VALUES (:topic) ON CONFLICT DO NOTHING"),
            [{"topic": t} for t in all_topics],
        )

    # Delete existing topic assignments if overwrite
    article_ids = [ca.article_id for ca in classified_articles]
    if overwrite and article_ids:
        session.execute(
            text("DELETE FROM article_topics WHERE article_id = ANY(:article_ids)"),
            {"article_ids": article_ids},
        )

    # Insert article-topic relationships
    rows = [
        {"article_id": ca.article_id, "topic": topic}
        for ca in classified_articles
        for topic in ca.topics
    ]
    if rows:
        session.execute(
            text(
                """
                INSERT INTO article_topics (article_id, topic)
                VALUES (:article_id, :topic)
                ON CONFLICT DO NOTHING
                """
            ),
            rows,
        )

    session.commit()
    logger.info(
        "Uploaded topics for %d articles (%d topic assignments) to RDS",
        len(article_ids),
        len(rows),
    )


def upload_clusters(
    clustered_articles: list[Any],
    session: Any,
    ingested_date: date,
    overwrite: bool = True,
) -> None:
    """
    Upload article clusters to RDS PostgreSQL.

    Groups articles by cluster_id, creates cluster records, and links articles
    to clusters. Noise articles (cluster_id == -1) are ignored.

    Args:
        clustered_articles: List of article objects/dicts with cluster_id field
        session: SQLAlchemy session
        ingested_date: Date used to determine cluster_period
        overwrite: If True, delete existing clusters for this date first
    """
    from uuid import uuid4
    from sqlalchemy import text

    # Group articles by cluster
    clusters: dict[int, list[str]] = {}
    for article in clustered_articles:
        if hasattr(article, "cluster_id"):
            cluster_id = article.cluster_id
            article_id = article.id
        else:
            cluster_id = article.get("cluster_id")
            article_id = article.get("id")

        if cluster_id is None or cluster_id == -1:
            continue
        clusters.setdefault(cluster_id, []).append(article_id)

    if not clusters:
        logger.warning("No non-noise clusters to save")
        return

    cluster_period = datetime.combine(ingested_date, datetime.min.time())
    start, end = date_to_range(ingested_date)

    # Delete existing clusters if overwrite
    if overwrite:
        session.execute(
            text(
                """
                DELETE FROM article_cluster_articles aca
                USING article_clusters ac
                WHERE aca.article_cluster_id = ac.article_cluster_id
                  AND ac.cluster_period >= :start
                  AND ac.cluster_period < :end
                """
            ),
            {"start": start, "end": end},
        )
        session.execute(
            text(
                """
                DELETE FROM article_clusters
                WHERE cluster_period >= :start
                  AND cluster_period < :end
                """
            ),
            {"start": start, "end": end},
        )

    # Insert clusters and their articles
    for _, article_ids in clusters.items():
        cluster_uuid = uuid4().hex
        session.execute(
            text(
                """
                INSERT INTO article_clusters (article_cluster_id, cluster_period)
                VALUES (:cluster_id, :cluster_period)
                """
            ),
            {"cluster_id": cluster_uuid, "cluster_period": cluster_period},
        )
        session.execute(
            text(
                """
                INSERT INTO article_cluster_articles (article_cluster_id, article_id)
                VALUES (:cluster_id, :article_id)
                """
            ),
            [
                {"cluster_id": cluster_uuid, "article_id": article_id}
                for article_id in article_ids
            ],
        )

    session.commit()
    logger.info("Saved %d clusters to RDS", len(clusters))
