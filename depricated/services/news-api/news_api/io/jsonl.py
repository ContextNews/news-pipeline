"""JSONL reading utilities."""

import gzip
import json
from datetime import date
from pathlib import Path
from typing import Iterator

from news_api.config import APIConfig
from news_api.io.s3 import list_s3_objects, read_s3_bytes


def _date_to_prefix(prefix: str, dt: date) -> str:
    """Build S3 prefix for a date partition."""
    return f"{prefix}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"


def list_jsonl_files(config: APIConfig, prefix: str, dt: date) -> list[str]:
    """List JSONL files for a date.

    Args:
        config: API config
        prefix: Base prefix (e.g., "news-raw")
        dt: Date to query

    Returns:
        List of file paths/keys
    """
    if config.is_s3:
        full_prefix = _date_to_prefix(prefix, dt)
        keys = list_s3_objects(config.s3.bucket, full_prefix)
        return [k for k in keys if k.endswith(".jsonl") or k.endswith(".jsonl.gz")]
    else:
        base_path = Path(config.local.raw_path)
        date_path = base_path / f"year={dt.year}" / f"month={dt.month:02d}" / f"day={dt.day:02d}"
        if not date_path.exists():
            return []
        jsonl_files = list(date_path.glob("*.jsonl")) + list(date_path.glob("*.jsonl.gz"))
        return [str(p) for p in jsonl_files]


def read_jsonl(config: APIConfig, path: str) -> Iterator[dict]:
    """Read a JSONL file and yield records.

    Supports both plain and gzip-compressed JSONL files.

    Args:
        config: API config
        path: File path (local) or S3 key

    Yields:
        Records as dicts
    """
    if config.is_s3:
        data = read_s3_bytes(config.s3.bucket, path)
        if path.endswith(".gz"):
            data = gzip.decompress(data)
        for line in data.decode("utf-8").splitlines():
            line = line.strip()
            if line:
                yield json.loads(line)
    else:
        opener = gzip.open if path.endswith(".gz") else open
        with opener(path, "rt", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    yield json.loads(line)


def load_raw_articles(config: APIConfig, dt: date) -> list[dict]:
    """Load all raw articles for a date.

    Args:
        config: API config
        dt: Date to query

    Returns:
        List of raw article dicts
    """
    prefix = config.s3.raw_prefix if config.s3 else "news-raw"
    files = list_jsonl_files(config, prefix, dt)

    all_articles = []
    for f in files:
        for article in read_jsonl(config, f):
            all_articles.append(article)

    return all_articles
