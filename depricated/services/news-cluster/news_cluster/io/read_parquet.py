"""Read normalized Parquet files."""

import io
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from news_cluster.config_loader import ClusterConfig
from news_cluster.io.s3 import list_s3_objects, read_s3_bytes


def read_parquet_file(path: str) -> list[dict[str, Any]]:
    """Read a single Parquet file and return list of records."""
    if path.startswith("s3://"):
        data = read_s3_bytes(path)
        table = pq.read_table(io.BytesIO(data))
    else:
        table = pq.read_table(path)

    return table.to_pylist()


def read_json_file(path: str) -> list[dict[str, Any]]:
    """Read a JSON file (array of objects) and return list of records."""
    with open(path) as f:
        return json.load(f)


def load_articles_for_date(config: ClusterConfig, date: datetime) -> list[dict[str, Any]]:
    """Load all normalized articles for a specific date."""
    articles = []

    if config.storage == "s3":
        prefix = config.input_prefix_for_date(date)
        s3_path = f"s3://{config.bucket}/{prefix}"
        parquet_files = list_s3_objects(s3_path, suffix=".parquet")

        for pq_file in parquet_files:
            articles.extend(read_parquet_file(pq_file))
    else:
        # Local mode - look for parquet or json files
        input_path = Path(config.input_dir)

        # Try parquet files first
        pq_files = list(input_path.glob("*.parquet"))
        if pq_files:
            for pq_file in pq_files:
                articles.extend(read_parquet_file(str(pq_file)))
        else:
            # Fall back to JSON files
            json_files = list(input_path.glob("*.json"))
            for json_file in json_files:
                articles.extend(read_json_file(str(json_file)))

    return articles


def load_all_articles(config: ClusterConfig) -> list[dict[str, Any]]:
    """Load all normalized articles for the configured date range."""
    all_articles = []

    for date in config.date_range:
        articles = load_articles_for_date(config, date)
        all_articles.extend(articles)

    return all_articles
