"""Storage backends for article output."""

import csv
import gzip
import json
import os
from datetime import datetime
from io import BytesIO, StringIO
from pathlib import Path

import boto3

from news_ingest.config import Config


def get_s3_client():
    """Create an S3 client configured from environment variables."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT"),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def build_s3_key(timestamp: datetime, extension: str) -> str:
    """Build the S3 key path for a given timestamp."""
    return (
        f"news-raw/"
        f"year={timestamp.year:04d}/"
        f"month={timestamp.month:02d}/"
        f"day={timestamp.day:02d}/"
        f"raw_articles_{timestamp.strftime('%Y_%m_%d_%H_%M')}.{extension}"
    )


def build_local_path(base_path: str, timestamp: datetime, extension: str) -> Path:
    """Build local file path for a given timestamp."""
    path = Path(base_path)
    path.mkdir(parents=True, exist_ok=True)
    filename = f"raw_articles_{timestamp.strftime('%Y_%m_%d_%H_%M')}.{extension}"
    return path / filename


def _serialize_jsonl(articles: list[dict], compress: bool) -> tuple[bytes, str]:
    """Serialize articles to JSONL format."""
    jsonl_content = "\n".join(json.dumps(article, ensure_ascii=False) for article in articles)
    jsonl_bytes = jsonl_content.encode("utf-8")

    if compress:
        buffer = BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
            gz.write(jsonl_bytes)
        return buffer.getvalue(), "jsonl.gz"
    else:
        return jsonl_bytes, "jsonl"


def _serialize_csv(articles: list[dict]) -> tuple[bytes, str]:
    """Serialize articles to CSV format."""
    if not articles:
        return b"", "csv"

    fieldnames = ["article_id", "source", "headline", "body", "url", "published_at", "fetched_at"]

    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(articles)

    return buffer.getvalue().encode("utf-8"), "csv"


def upload_articles(articles: list[dict], timestamp: datetime, config: Config) -> str:
    """Upload articles using the configured storage backend.

    Args:
        articles: List of article dictionaries to upload
        timestamp: Timestamp for partitioning
        config: Configuration object

    Returns:
        The path/key where the file was uploaded

    Raises:
        ValueError: If articles list is empty
        Exception: If upload fails
    """
    if not articles:
        raise ValueError("Cannot upload empty articles list")

    # Serialize based on output format
    if config.output_format == "csv":
        content, extension = _serialize_csv(articles)
    else:
        content, extension = _serialize_jsonl(articles, config.output_compress)

    # Upload based on storage backend
    if config.storage_backend == "local":
        return _upload_local(content, timestamp, extension, config.storage_local_path)
    else:
        return _upload_s3(content, timestamp, extension)


def _upload_s3(content: bytes, timestamp: datetime, extension: str) -> str:
    """Upload to S3."""
    bucket = os.environ["S3_BUCKET"]
    key = build_s3_key(timestamp, extension)

    client = get_s3_client()
    client.upload_fileobj(BytesIO(content), bucket, key)

    return key


def _upload_local(content: bytes, timestamp: datetime, extension: str, base_path: str) -> str:
    """Write to local filesystem."""
    filepath = build_local_path(base_path, timestamp, extension)

    with open(filepath, "wb") as f:
        f.write(content)

    return str(filepath)
