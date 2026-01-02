"""Storage backends for article output."""

import csv
import gzip
import json
import os
from datetime import datetime
from io import BytesIO, StringIO
from pathlib import Path

import boto3

from news_ingest.config import get_config


def get_s3_client():
    """Create an S3 client configured from environment variables."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT"),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def build_s3_key(source: str, timestamp: datetime, extension: str) -> str:
    """Build the S3 key path for a given source and timestamp."""
    return (
        f"news-raw/"
        f"year={timestamp.year:04d}/"
        f"month={timestamp.month:02d}/"
        f"day={timestamp.day:02d}/"
        f"source={source}/"
        f"articles_{timestamp.strftime('%Y%m%d_%H%M%S')}.{extension}"
    )


def build_local_path(base_path: str, source: str, timestamp: datetime, extension: str) -> Path:
    """Build local file path for a given source and timestamp."""
    path = Path(base_path)
    path.mkdir(parents=True, exist_ok=True)
    filename = f"{source}_{timestamp.strftime('%Y%m%d_%H%M%S')}.{extension}"
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


def upload_articles(articles: list[dict], source: str, timestamp: datetime) -> str:
    """Upload articles using the configured storage backend.

    Args:
        articles: List of article dictionaries to upload
        source: Source identifier (e.g., "bbc", "reuters")
        timestamp: Timestamp for partitioning

    Returns:
        The path/key where the file was uploaded

    Raises:
        ValueError: If articles list is empty
        Exception: If upload fails
    """
    if not articles:
        raise ValueError("Cannot upload empty articles list")

    config = get_config()

    # Serialize based on output format
    if config.output.format == "csv":
        content, extension = _serialize_csv(articles)
    else:
        content, extension = _serialize_jsonl(articles, config.output.compress)

    # Upload based on storage backend
    if config.storage.backend == "local":
        return _upload_local(content, source, timestamp, extension, config.storage.local_path)
    else:
        return _upload_s3(content, source, timestamp, extension)


def _upload_s3(content: bytes, source: str, timestamp: datetime, extension: str) -> str:
    """Upload to S3."""
    bucket = os.environ["S3_BUCKET"]
    key = build_s3_key(source, timestamp, extension)

    client = get_s3_client()
    client.upload_fileobj(BytesIO(content), bucket, key)

    return key


def _upload_local(content: bytes, source: str, timestamp: datetime, extension: str, base_path: str) -> str:
    """Write to local filesystem."""
    filepath = build_local_path(base_path, source, timestamp, extension)

    with open(filepath, "wb") as f:
        f.write(content)

    return str(filepath)
