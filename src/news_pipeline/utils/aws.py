import gzip
import json
import os
import boto3
from datetime import datetime
from typing import Iterable, Iterator, Mapping, Any

from dotenv import load_dotenv

load_dotenv()


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


def upload_articles(articles: list[Any], session: Any) -> tuple[int, int]:
    """
    Upload articles to RDS PostgreSQL.

    Args:
        articles: List of article objects or dicts with fields:
            id, source, title, summary, url, published_at, ingested_at, text
        session: SQLAlchemy session

    Returns:
        Tuple of (articles inserted, articles skipped)
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
    return inserted, skipped