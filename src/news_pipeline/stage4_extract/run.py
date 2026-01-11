"""CLI entry point for news extract stage."""

import argparse
import logging
import os
from datetime import datetime, timezone

from news_pipeline.stage4_extract.extract import extract, DEFAULT_MODEL, DEFAULT_BATCH_SIZE, SPACY_MODELS
from news_pipeline.utils.aws import (
    build_s3_key,
    list_s3_jsonl_files,
    read_jsonl_from_s3,
    upload_jsonl_to_s3,
)
from news_pipeline.utils.serialization import serialize_dataclass

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--period", help="Date to process (YYYY-MM-DD), defaults to today")
    parser.add_argument("--model", default=DEFAULT_MODEL, choices=list(SPACY_MODELS.keys()))
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    args = parser.parse_args()

    period = args.period or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    period_dt = datetime.strptime(period, "%Y-%m-%d")

    bucket = os.environ["S3_BUCKET_NAME"]
    now = datetime.now(timezone.utc)

    # Read input articles from S3 (embedded_articles)
    input_prefix = build_s3_key("embedded_articles", period_dt, "")[:-1]
    input_files = list_s3_jsonl_files(bucket, input_prefix)

    if not input_files:
        logger.warning(f"No input files found for {period}")
        return

    logger.info(f"Found {len(input_files)} input files for {period}")

    embedded_articles = []
    for key in input_files:
        logger.info(f"Reading {key}")
        embedded_articles.extend(read_jsonl_from_s3(bucket, key))

    if not embedded_articles:
        logger.warning("No articles found in input files")
        return

    logger.info(f"Loaded {len(embedded_articles)} articles")

    # Extract entities
    extracted = extract(
        embedded_articles,
        model_key=args.model,
        batch_size=args.batch_size,
    )

    if not extracted:
        logger.warning("No entities extracted")
        return

    # Upload as JSONL to S3
    timestamp = now.strftime('%Y_%m_%d_%H_%M')
    output_key = build_s3_key("extracted_articles", period_dt, f"extracted_articles_{timestamp}.jsonl")

    records = [serialize_dataclass(article) for article in extracted]
    upload_jsonl_to_s3(records, bucket, output_key)

    logger.info(f"Uploaded {len(extracted)} extracted articles to s3://{bucket}/{output_key}")


if __name__ == "__main__":
    main()
