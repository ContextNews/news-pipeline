"""CLI entry point for news load stage."""

import argparse
import logging
from datetime import datetime, timezone
import os

from news_pipeline.stage5_load.load import load_articles
from news_pipeline.utils.aws import (
    build_s3_key,
    list_s3_jsonl_files,
    read_jsonl_from_s3,
)
from rds_postgres.connection import get_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--period", help="Date to process (YYYY-MM-DD), defaults to today")
    args = parser.parse_args()

    period = args.period or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    period_dt = datetime.strptime(period, "%Y-%m-%d")

    bucket = os.environ["S3_BUCKET_NAME"]

    # Read input articles from S3 (extracted_articles)
    input_prefix = build_s3_key("extracted_articles", period_dt, "")[:-1]
    input_files = list_s3_jsonl_files(bucket, input_prefix)

    if not input_files:
        logger.warning(f"No input files found for {period}")
        return

    logger.info(f"Found {len(input_files)} input files for {period}")

    extracted_articles = []
    for key in input_files:
        logger.info(f"Reading {key}")
        extracted_articles.extend(read_jsonl_from_s3(bucket, key))

    if not extracted_articles:
        logger.warning("No articles found in input files")
        return

    logger.info(f"Loaded {len(extracted_articles)} articles from S3")

    # Load into database
    with get_session() as session:
        articles_loaded, entities_loaded, article_entities_loaded = load_articles(
            extracted_articles, session
        )

    logger.info(
        f"Loaded {articles_loaded} articles, {entities_loaded} new entities, "
        f"{article_entities_loaded} new article-entity relationships"
    )


if __name__ == "__main__":
    main()
