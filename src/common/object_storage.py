"""Object-storage helpers (currently S3-backed via boto3).

Public API uses storage-neutral names; the implementation is AWS S3 today.
To swap backends, replace the bodies here without touching callers.
"""

import gzip
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Iterable, Iterator, Mapping

import boto3
from dotenv import load_dotenv

from common.serialization import serialize_dataclass

load_dotenv()

logger = logging.getLogger(__name__)


def _get_client():
    return boto3.client("s3")


def build_object_key(prefix: str, timestamp: datetime, filename: str) -> str:
    """Build a Hive-partitioned key path under the given prefix."""
    return (
        f"{prefix}/"
        f"year={timestamp.year:04d}/"
        f"month={timestamp.month:02d}/"
        f"day={timestamp.day:02d}/"
        f"{filename}"
    )


def upload_jsonl_to_object_store(
    records: Iterable[Mapping[str, Any]],
    bucket: str,
    key: str,
) -> None:
    """Upload in-memory records to the object store as JSONL."""
    body = "\n".join(json.dumps(record, ensure_ascii=False) for record in records) + "\n"

    client = _get_client()
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/jsonl",
    )


def upload_jsonl_records_to_object_store(records: list[Any], prefix: str) -> None:
    """Serialize a list of dataclass records and upload as JSONL.

    Reads bucket name from the S3_BUCKET_NAME env var.

    Args:
        records: List of dataclass objects to upload
        prefix: Path prefix (e.g., "ingested_articles", "embedded_articles")
    """
    bucket = os.environ["S3_BUCKET_NAME"]
    now = datetime.now(timezone.utc)
    filename = f"{prefix}_{now.strftime('%Y_%m_%d_%H_%M')}.jsonl"
    key = build_object_key(prefix, now, filename)

    serialized = [serialize_dataclass(record) for record in records]
    upload_jsonl_to_object_store(serialized, bucket, key)

    logger.info("Uploaded %d records to s3://%s/%s", len(records), bucket, key)


def upload_csv_to_object_store(csv_content: str, bucket: str, key: str) -> None:
    """Upload a CSV string to the object store."""
    client = _get_client()
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_content.encode("utf-8"),
        ContentType="text/csv",
    )


def list_jsonl_files(bucket: str, prefix: str) -> list[str]:
    """List all .jsonl (or .jsonl.gz) keys under a prefix."""
    client = _get_client()
    files = []
    paginator = client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".jsonl") or key.endswith(".jsonl.gz"):
                files.append(key)

    return files


def read_jsonl_from_object_store(bucket: str, key: str) -> Iterator[dict]:
    """Stream JSONL records from the object store, handling gzip if needed."""
    client = _get_client()
    response = client.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read()

    if key.endswith(".gz"):
        content = gzip.decompress(content)

    for line in content.decode("utf-8").splitlines():
        line = line.strip()
        if line:
            yield json.loads(line)
