"""CLI entry point for news ingest."""

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import yaml

from news_pipeline.stage1_ingest.ingest import ingest
from news_pipeline.stage1_ingest.models import IngestConfig
from news_pipeline.utils.aws import build_s3_key, upload_jsonl_to_s3
from news_pipeline.utils.serialization import serialize_dataclass

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).parent / "configs"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("config", nargs="?", default="prod", choices=["prod", "test"])
    args = parser.parse_args()

    with open(CONFIG_DIR / f"{args.config}.yaml") as f:
        config = IngestConfig(**yaml.safe_load(f))

    now = datetime.now(timezone.utc)
    articles = ingest(config.sources, config.lookback_hours, config.fetch_article_text)

    if not articles:
        logger.warning("No articles collected")
        return

    records = [serialize_dataclass(a) for a in articles]

    if config.output == "local":
        path = Path("output")
        path.mkdir(exist_ok=True)
        filepath = path / f"raw_articles_{now.strftime('%Y_%m_%d_%H_%M')}.jsonl"
        with open(filepath, "w") as f:
            for r in records:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        logger.info(f"Saved {len(articles)} articles to {filepath}")
    else:
        bucket = os.environ["S3_BUCKET_NAME"]
        key = build_s3_key("ingested_articles", now, f"raw_articles_{now.strftime('%Y_%m_%d_%H_%M')}.jsonl")
        upload_jsonl_to_s3(records, bucket, key)
        logger.info(f"Uploaded {len(articles)} articles to s3://{bucket}/{key}")


if __name__ == "__main__":
    main()
