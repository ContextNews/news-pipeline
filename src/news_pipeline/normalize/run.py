"""CLI entry point for news normalize."""

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import yaml

from news_pipeline.normalize.models import NormalizeConfig
from news_pipeline.normalize.normalize import normalize
from news_pipeline.utils.aws import (
    build_s3_key,
    list_s3_jsonl_files,
    read_jsonl_from_s3,
    upload_jsonl_to_s3,
)
from news_pipeline.utils.serialization import serialize_dataclass

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).parent / "configs"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("config", nargs="?", default="prod", choices=["prod", "test"])
    parser.add_argument("--period", help="Date to process (YYYY-MM-DD), defaults to today")
    args = parser.parse_args()

    with open(CONFIG_DIR / f"{args.config}.yaml") as f:
        config = NormalizeConfig(**yaml.safe_load(f))

    # Override period if provided
    period = args.period or config.period or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    period_dt = datetime.strptime(period, "%Y-%m-%d")

    bucket = os.environ["S3_BUCKET"]
    now = datetime.now(timezone.utc)

    # Read input articles from S3
    input_prefix = build_s3_key("news-raw", period_dt, "")[:-1]  # Remove trailing slash
    input_files = list_s3_jsonl_files(bucket, input_prefix)

    if not input_files:
        logger.warning(f"No input files found for {period}")
        return

    logger.info(f"Found {len(input_files)} input files for {period}")

    raw_articles = []
    for key in input_files:
        logger.info(f"Reading {key}")
        raw_articles.extend(read_jsonl_from_s3(bucket, key))

    if not raw_articles:
        logger.warning("No articles found in input files")
        return

    logger.info(f"Loaded {len(raw_articles)} articles")

    # Run normalization
    normalized = normalize(
        raw_articles,
        spacy_model=config.spacy_model,
        embedding_enabled=config.embedding_enabled,
        embedding_model=config.embedding_model,
    )

    if not normalized:
        logger.warning("No articles normalized")
        return

    records = [serialize_dataclass(a) for a in normalized]

    if config.output == "local":
        path = Path("output")
        path.mkdir(exist_ok=True)
        filepath = path / f"normalized_{now.strftime('%Y_%m_%d_%H_%M')}.jsonl"
        with open(filepath, "w") as f:
            for r in records:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        logger.info(f"Saved {len(normalized)} articles to {filepath}")
    else:
        key = build_s3_key("news-normalized", period_dt, f"normalized_{now.strftime('%Y_%m_%d_%H_%M')}.jsonl")
        upload_jsonl_to_s3(records, bucket, key)
        logger.info(f"Uploaded {len(normalized)} articles to s3://{bucket}/{key}")


if __name__ == "__main__":
    main()
