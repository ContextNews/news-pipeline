import io
from typing import Iterator

import boto3


def is_s3_path(path: str) -> bool:
    return path.startswith("s3://")


def parse_s3_path(path: str) -> tuple[str, str]:
    """Parse s3://bucket/key into (bucket, key)."""
    path = path.removeprefix("s3://")
    parts = path.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key


def read_s3_bytes(path: str) -> bytes:
    """Read entire S3 object as bytes."""
    bucket, key = parse_s3_path(path)
    s3 = boto3.client("s3")

    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def read_s3_lines(path: str) -> Iterator[str]:
    """Stream lines from an S3 object."""
    bucket, key = parse_s3_path(path)
    s3 = boto3.client("s3")

    response = s3.get_object(Bucket=bucket, Key=key)
    for line in response["Body"].iter_lines():
        yield line.decode("utf-8")


def list_s3_objects(path: str) -> list[str]:
    """List objects under an S3 prefix."""
    bucket, prefix = parse_s3_path(path)
    s3 = boto3.client("s3")

    objects = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            objects.append(f"s3://{bucket}/{obj['Key']}")
    return objects


def list_jsonl_files(bucket: str, prefix: str) -> list[str]:
    """List all .jsonl and .jsonl.gz files under an S3 prefix.

    Args:
        bucket: S3 bucket name
        prefix: S3 key prefix (e.g., 'news-raw/year=2024/month=12/day=31/')

    Returns:
        List of full S3 paths (s3://bucket/key)
    """
    s3 = boto3.client("s3")
    files = []
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".jsonl.gz") or key.endswith(".jsonl"):
                files.append(f"s3://{bucket}/{key}")

    return files


def write_s3_bytes(path: str, data: bytes) -> None:
    """Write bytes to an S3 object."""
    bucket, key = parse_s3_path(path)
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=data)
