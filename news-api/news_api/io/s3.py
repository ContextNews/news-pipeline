"""S3 client utilities."""

import os
from functools import lru_cache

import boto3


@lru_cache(maxsize=1)
def get_s3_client():
    """Get a cached S3 client."""
    endpoint = os.getenv("S3_ENDPOINT", "https://s3.amazonaws.com")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


def list_s3_objects(bucket: str, prefix: str) -> list[str]:
    """List all object keys under a prefix.

    Args:
        bucket: S3 bucket name
        prefix: Key prefix to list

    Returns:
        List of object keys
    """
    client = get_s3_client()
    keys = []

    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    return keys


def read_s3_bytes(bucket: str, key: str) -> bytes:
    """Read an object from S3 as bytes.

    Args:
        bucket: S3 bucket name
        key: Object key

    Returns:
        Object contents as bytes
    """
    client = get_s3_client()
    response = client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()
