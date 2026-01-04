"""Parquet reading utilities."""

import io
import json
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq

from news_api.config import APIConfig
from news_api.io.s3 import list_s3_objects, read_s3_bytes


def _date_to_prefix(prefix: str, dt: date) -> str:
    """Build S3 prefix for a date partition."""
    return f"{prefix}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"


def list_parquet_files(config: APIConfig, prefix: str, dt: date) -> list[str]:
    """List parquet files for a date.

    Args:
        config: API config
        prefix: Base prefix (e.g., "news-normalized")
        dt: Date to query

    Returns:
        List of file paths/keys
    """
    if config.is_s3:
        full_prefix = _date_to_prefix(prefix, dt)
        keys = list_s3_objects(config.s3.bucket, full_prefix)
        return [k for k in keys if k.endswith(".parquet")]
    else:
        # Local storage
        if prefix == config.s3.normalized_prefix if config.s3 else "news-normalized":
            base_path = Path(config.local.normalized_path)
        else:
            base_path = Path(config.local.clustered_path)

        date_path = base_path / f"year={dt.year}" / f"month={dt.month:02d}" / f"day={dt.day:02d}"
        if not date_path.exists():
            return []
        return [str(p) for p in date_path.glob("*.parquet")]


def list_json_files(config: APIConfig, prefix: str, dt: date) -> list[str]:
    """List JSON files for a date.

    Args:
        config: API config
        prefix: Base prefix (e.g., "news-clustered")
        dt: Date to query

    Returns:
        List of file paths/keys
    """
    if config.is_s3:
        full_prefix = _date_to_prefix(prefix, dt)
        keys = list_s3_objects(config.s3.bucket, full_prefix)
        return [k for k in keys if k.endswith(".json")]
    else:
        base_path = Path(config.local.clustered_path)
        date_path = base_path / f"year={dt.year}" / f"month={dt.month:02d}" / f"day={dt.day:02d}"
        if not date_path.exists():
            return []
        return [str(p) for p in date_path.glob("*.json")]


def read_parquet(config: APIConfig, path: str) -> list[dict]:
    """Read a parquet file and return as list of dicts.

    Args:
        config: API config
        path: File path (local) or S3 key

    Returns:
        List of records as dicts
    """
    if config.is_s3:
        data = read_s3_bytes(config.s3.bucket, path)
        table = pq.read_table(io.BytesIO(data))
    else:
        table = pq.read_table(path)

    return table.to_pylist()


def read_json(config: APIConfig, path: str) -> list[dict]:
    """Read a JSON file and return as list of dicts.

    Args:
        config: API config
        path: File path (local) or S3 key

    Returns:
        List of records (assumes JSON array at top level)
    """
    if config.is_s3:
        data = read_s3_bytes(config.s3.bucket, path)
        return json.loads(data.decode("utf-8"))
    else:
        with open(path) as f:
            return json.load(f)


def load_normalized_articles(config: APIConfig, dt: date) -> list[dict]:
    """Load all normalized articles for a date.

    Args:
        config: API config
        dt: Date to query

    Returns:
        List of article dicts
    """
    prefix = config.s3.normalized_prefix if config.s3 else "news-normalized"
    files = list_parquet_files(config, prefix, dt)

    all_articles = []
    for f in files:
        articles = read_parquet(config, f)
        all_articles.extend(articles)

    return all_articles


def load_stories(config: APIConfig, dt: date) -> list[dict]:
    """Load all stories for a date.

    Args:
        config: API config
        dt: Date to query

    Returns:
        List of story dicts
    """
    prefix = config.s3.clustered_prefix if config.s3 else "news-clustered"
    files = list_parquet_files(config, prefix, dt)

    # Filter to stories files only
    story_files = [f for f in files if "stories_" in f]

    all_stories = []
    for f in story_files:
        stories = read_parquet(config, f)
        all_stories.extend(stories)

    return all_stories


def load_story_articles(config: APIConfig, dt: date) -> list[dict]:
    """Load story_articles JSON for a date.

    Args:
        config: API config
        dt: Date to query

    Returns:
        List of story_articles dicts (denormalized view)
    """
    prefix = config.s3.clustered_prefix if config.s3 else "news-clustered"
    files = list_json_files(config, prefix, dt)

    # Filter to story_articles files only
    story_articles_files = [f for f in files if "story_articles_" in f]

    all_story_articles = []
    for f in story_articles_files:
        data = read_json(config, f)
        if isinstance(data, list):
            all_story_articles.extend(data)
        else:
            all_story_articles.append(data)

    return all_story_articles


def load_article_story_map(config: APIConfig, dt: date) -> list[dict]:
    """Load article-to-story mapping for a date.

    Args:
        config: API config
        dt: Date to query

    Returns:
        List of article-story map dicts
    """
    prefix = config.s3.clustered_prefix if config.s3 else "news-clustered"
    files = list_parquet_files(config, prefix, dt)

    # Filter to article_story_map files only
    map_files = [f for f in files if "article_story_map_" in f]

    all_maps = []
    for f in map_files:
        maps = read_parquet(config, f)
        all_maps.extend(maps)

    return all_maps
