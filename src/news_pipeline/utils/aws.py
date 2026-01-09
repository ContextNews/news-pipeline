import gzip
import json
import os
import boto3
from datetime import datetime
from typing import Iterable, Iterator, Mapping, Any

from dotenv import load_dotenv

load_dotenv()


def get_s3_client():
    """Create S3 client, using S3_ENDPOINT if set."""
    return boto3.client("s3", endpoint_url=os.environ.get("S3_ENDPOINT"))


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