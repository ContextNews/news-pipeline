"""CLI for ingesting and cleaning articles."""

from __future__ import annotations

import argparse
import logging
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from ingest_articles.ingest_articles import ingest_articles
from ingest_articles.fetch_articles.sources import RSS_FEEDS
from news_pipeline.utils.aws import build_s3_key, upload_jsonl_to_s3, upload_articles
from news_pipeline.utils.serialization import serialize_dataclass

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def _parse_sources(value: str | None) -> list[str]:
    if not value:
        return list(RSS_FEEDS.keys())
    sources = []
    for part in value.split(","):
        source = part.strip()
        if source:
            sources.append(source)
    return sources or list(RSS_FEEDS.keys())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lookback-hours", type=int, default=12)
    parser.add_argument(
        "--sources",
        default=None,
        help="Comma-separated list of sources (default: all).",
    )
    parser.add_argument("--load-s3", action="store_true")
    parser.add_argument("--load-rds", action="store_true")
    parser.add_argument("--load-local", action="store_true")
    args = parser.parse_args()

    sources = _parse_sources(args.sources)
    ingested_articles = ingest_articles(
        sources=sources,
        lookback_hours=args.lookback_hours,
    )

    if not ingested_articles:
        logger.warning("No articles cleaned")
        return

    now = datetime.now(timezone.utc)

    if args.load_s3:
        bucket = os.environ["S3_BUCKET_NAME"]
        key = build_s3_key(
            "ingested_articles",
            now,
            f"ingested_articles_{now.strftime('%Y_%m_%d_%H_%M')}.jsonl",
        )
        records = [serialize_dataclass(article) for article in ingested_articles]
        upload_jsonl_to_s3(records, bucket, key)
        logger.info("Uploaded %d cleaned articles to s3://%s/%s", len(ingested_articles), bucket, key)

    if args.load_local:
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        filename = f"ingested_articles_{now.strftime('%Y_%m_%d_%H_%M')}.jsonl"
        filepath = output_dir / filename
        records = [serialize_dataclass(article) for article in ingested_articles]
        with filepath.open("w") as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        logger.info("Saved %d cleaned articles to %s", len(ingested_articles), filepath)

    if args.load_rds:
        from rds_postgres.connection import get_session

        with get_session() as session:
            inserted, skipped = upload_articles(ingested_articles, session)
        logger.info("Loaded %d articles to RDS (%d skipped as duplicates)", inserted, skipped)


if __name__ == "__main__":
    main()
