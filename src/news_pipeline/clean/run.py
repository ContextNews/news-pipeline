"""CLI entry point for news clean stage."""

import argparse
import logging
import os
from datetime import datetime, timezone

from news_pipeline.clean.clean import clean_text
from news_pipeline.clean.models import CleanedArticle
from news_pipeline.utils.aws import (
    build_s3_key,
    list_s3_jsonl_files,
    read_jsonl_from_s3,
    upload_jsonl_to_s3,
)
from news_pipeline.utils.serialization import serialize_dataclass

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def clean_articles(raw_articles: list[dict]) -> list[CleanedArticle]:
    """Clean raw articles: title, summary, and text."""
    if not raw_articles:
        logger.warning("No articles to clean")
        return []

    logger.info(f"Cleaning {len(raw_articles)} articles")

    results = []
    for raw in raw_articles:
        results.append(CleanedArticle(
            id=raw["id"],
            source=raw["source"],
            title=clean_text(raw.get("title")) or "",
            summary=clean_text(raw.get("summary")) or "",
            url=raw["url"],
            published_at=_parse_datetime(raw.get("published_at")),
            ingested_at=_parse_datetime(raw.get("ingested_at")),
            text=clean_text(raw.get("text")),
        ))

    logger.info(f"Cleaned {len(results)} articles")
    return results


def _parse_datetime(value) -> datetime:
    """Parse datetime from ISO string or return as-is if already datetime."""
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--period", help="Date to process (YYYY-MM-DD), defaults to today")
    args = parser.parse_args()

    period = args.period or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    period_dt = datetime.strptime(period, "%Y-%m-%d")

    bucket = os.environ["S3_BUCKET_NAME"]
    now = datetime.now(timezone.utc)

    # Read input articles from S3 (news-raw)
    input_prefix = build_s3_key("news-raw", period_dt, "")[:-1]
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

    # Clean articles
    cleaned = clean_articles(raw_articles)

    if not cleaned:
        logger.warning("No articles cleaned")
        return

    # Upload as JSONL to S3
    timestamp = now.strftime('%Y_%m_%d_%H_%M')
    output_key = build_s3_key("cleaned_articles", period_dt, f"cleaned_articles_{timestamp}.jsonl")

    records = [serialize_dataclass(article) for article in cleaned]
    upload_jsonl_to_s3(records, bucket, output_key)

    logger.info(f"Uploaded {len(cleaned)} cleaned articles to s3://{bucket}/{output_key}")


if __name__ == "__main__":
    main()
