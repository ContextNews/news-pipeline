"""S3 utilities for news-cluster."""

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


def list_s3_objects(path: str, suffix: str = "") -> list[str]:
    """List objects under an S3 prefix, optionally filtering by suffix."""
    bucket, prefix = parse_s3_path(path)
    s3 = boto3.client("s3")

    objects = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not suffix or key.endswith(suffix):
                objects.append(f"s3://{bucket}/{key}")
    return objects


def read_s3_bytes(path: str) -> bytes:
    """Read entire S3 object as bytes."""
    bucket, key = parse_s3_path(path)
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def write_s3_bytes(path: str, data: bytes) -> None:
    """Write bytes to an S3 object."""
    bucket, key = parse_s3_path(path)
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=data)
