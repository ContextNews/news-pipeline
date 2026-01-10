"""Write clustering output to storage."""

import io
import json
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from news_cluster.config_loader import ClusterConfig
from news_cluster.io.s3 import write_s3_bytes
from news_cluster.schema import ArticleStoryMap, Story, StoryArticles


def get_timestamp() -> str:
    """Get current timestamp for filenames."""
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def stories_to_table(stories: list[Story]) -> pa.Table:
    """Convert Story objects to PyArrow Table."""
    records = [s.to_dict() for s in stories]
    return pa.Table.from_pylist(records)


def article_maps_to_table(maps: list[ArticleStoryMap]) -> pa.Table:
    """Convert ArticleStoryMap objects to PyArrow Table."""
    records = [m.to_dict() for m in maps]
    return pa.Table.from_pylist(records)


def write_parquet_local(table: pa.Table, path: str) -> None:
    """Write PyArrow Table to local Parquet file."""
    pq.write_table(table, path)


def write_parquet_s3(table: pa.Table, s3_path: str) -> None:
    """Write PyArrow Table to S3 as Parquet."""
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    write_s3_bytes(s3_path, buffer.getvalue())


def write_json_local(data: list[dict], path: str) -> None:
    """Write data to local JSON file."""
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)


def write_output(
    config: ClusterConfig,
    stories: list[Story],
    article_maps: list[ArticleStoryMap],
    story_articles: list[StoryArticles],
) -> tuple[str, str, str]:
    """Write clustering output to storage.

    Args:
        config: Cluster configuration
        stories: List of Story objects
        article_maps: List of ArticleStoryMap objects
        story_articles: List of StoryArticles objects (denormalized view)

    Returns:
        Tuple of (stories_path, article_maps_path, story_articles_path)
    """
    timestamp = get_timestamp()

    if config.storage == "local" or config.output_dir:
        # Local output
        output_dir = Path(config.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        if config.output_format == "parquet":
            stories_path = str(output_dir / f"stories_{timestamp}.parquet")
            maps_path = str(output_dir / f"article_story_map_{timestamp}.parquet")
            story_articles_path = str(output_dir / f"story_articles_{timestamp}.json")

            stories_table = stories_to_table(stories)
            maps_table = article_maps_to_table(article_maps)

            write_parquet_local(stories_table, stories_path)
            write_parquet_local(maps_table, maps_path)
            # Story articles always written as JSON (nested structure)
            write_json_local([sa.to_dict() for sa in story_articles], story_articles_path)
        else:
            # JSON output
            stories_path = str(output_dir / f"stories_{timestamp}.json")
            maps_path = str(output_dir / f"article_story_map_{timestamp}.json")
            story_articles_path = str(output_dir / f"story_articles_{timestamp}.json")

            write_json_local([s.to_dict() for s in stories], stories_path)
            write_json_local([m.to_dict() for m in article_maps], maps_path)
            write_json_local([sa.to_dict() for sa in story_articles], story_articles_path)

        return stories_path, maps_path, story_articles_path
    else:
        # S3 output
        stories_key = f"{config.output_prefix}stories_{timestamp}.parquet"
        maps_key = f"{config.output_prefix}article_story_map_{timestamp}.parquet"
        story_articles_key = f"{config.output_prefix}story_articles_{timestamp}.json"

        stories_path = f"s3://{config.bucket}/{stories_key}"
        maps_path = f"s3://{config.bucket}/{maps_key}"
        story_articles_path = f"s3://{config.bucket}/{story_articles_key}"

        stories_table = stories_to_table(stories)
        maps_table = article_maps_to_table(article_maps)

        write_parquet_s3(stories_table, stories_path)
        write_parquet_s3(maps_table, maps_path)
        # Story articles always written as JSON (nested structure)
        write_s3_bytes(story_articles_path, json.dumps(
            [sa.to_dict() for sa in story_articles], indent=2, default=str
        ).encode())

        return stories_path, maps_path, story_articles_path
