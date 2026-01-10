"""CLI entry point for news normalize."""

import argparse
import csv
import io
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import yaml

from news_pipeline.normalize.models import NormalizeConfig, NormalizedArticle
from news_pipeline.normalize.normalize import normalize
from news_pipeline.utils.aws import (
    build_s3_key,
    list_s3_jsonl_files,
    read_jsonl_from_s3,
    upload_csv_to_s3,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).parent / "configs"


def build_csvs(articles: list[NormalizedArticle]) -> tuple[str, str, str]:
    """Build 3 CSV files from normalized articles.

    Returns:
        Tuple of (processed_articles_csv, entities_csv, article_entity_map_csv)
    """
    # Build unique entities set
    unique_entities: set[tuple[str, str]] = set()
    for article in articles:
        for entity in article.entities:
            unique_entities.add((entity.name, entity.type))

    # Build processed_articles CSV
    articles_buffer = io.StringIO()
    articles_writer = csv.writer(articles_buffer)
    articles_writer.writerow([
        "id", "source", "title", "summary", "url", "published_at", "ingested_at",
        "article_text", "article_text_clean", "article_text_processed",
        "ner_model", "normalized_at", "embedding", "embedding_model"
    ])
    for article in articles:
        articles_writer.writerow([
            article.id,
            article.source,
            article.title,
            article.summary,
            article.url,
            article.published_at.isoformat() if article.published_at else None,
            article.ingested_at.isoformat() if article.ingested_at else None,
            article.article_text,
            article.article_text_clean,
            article.article_text_processed,
            article.ner_model,
            article.normalized_at.isoformat() if article.normalized_at else None,
            json.dumps(article.embedding) if article.embedding else None,
            article.embedding_model,
        ])

    # Build entities CSV
    entities_buffer = io.StringIO()
    entities_writer = csv.writer(entities_buffer)
    entities_writer.writerow(["name", "type"])
    for name, entity_type in sorted(unique_entities):
        entities_writer.writerow([name, entity_type])

    # Build article_entity_map CSV
    map_buffer = io.StringIO()
    map_writer = csv.writer(map_buffer)
    map_writer.writerow(["article_id", "entity_name"])
    for article in articles:
        for entity in article.entities:
            map_writer.writerow([article.id, entity.name])

    return articles_buffer.getvalue(), entities_buffer.getvalue(), map_buffer.getvalue()


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

    bucket = os.environ["S3_BUCKET_NAME"]
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
        embedding_model=config.embedding_model,
        embedding_batch_size=config.embedding_batch_size,
        max_article_words=config.max_article_words,
    )

    if not normalized:
        logger.warning("No articles normalized")
        return

    # Build CSVs
    articles_csv, entities_csv, map_csv = build_csvs(normalized)
    timestamp = now.strftime('%Y_%m_%d_%H_%M')

    if config.output == "local":
        path = Path("output")
        path.mkdir(exist_ok=True)

        (path / f"processed_articles_{timestamp}.csv").write_text(articles_csv)
        (path / f"entities_{timestamp}.csv").write_text(entities_csv)
        (path / f"article_entity_map_{timestamp}.csv").write_text(map_csv)

        logger.info(f"Saved {len(normalized)} articles to {path}/")
    else:
        prefix = build_s3_key("news-normalized", period_dt, "")[:-1]

        upload_csv_to_s3(articles_csv, bucket, f"{prefix}/processed_articles_{timestamp}.csv")
        upload_csv_to_s3(entities_csv, bucket, f"{prefix}/entities_{timestamp}.csv")
        upload_csv_to_s3(map_csv, bucket, f"{prefix}/article_entity_map_{timestamp}.csv")

        logger.info(f"Uploaded 3 CSV files to s3://{bucket}/{prefix}/")


if __name__ == "__main__":
    main()
