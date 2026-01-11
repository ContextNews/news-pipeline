"""CLI entry point for news embed stage."""

import argparse
import logging
import os
from datetime import datetime, timezone

from news_pipeline.embed.embed import prepare_text, compute_embeddings_batch
from news_pipeline.embed.models import EmbeddedArticle
from news_pipeline.utils.aws import (
    build_s3_key,
    list_s3_jsonl_files,
    read_jsonl_from_s3,
    upload_jsonl_to_s3,
)
from news_pipeline.utils.serialization import serialize_dataclass

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_MODEL = "mpnet"
DEFAULT_BATCH_SIZE = 32
DEFAULT_MAX_ARTICLE_WORDS = 250


def embed_articles(
    cleaned_articles: list[dict],
    model_key: str = DEFAULT_MODEL,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_article_words: int = DEFAULT_MAX_ARTICLE_WORDS,
) -> list[EmbeddedArticle]:
    """Embed cleaned articles."""
    if not cleaned_articles:
        logger.warning("No articles to embed")
        return []

    logger.info(f"Embedding {len(cleaned_articles)} articles")

    # Prepare texts for embedding
    prepared_texts = [
        prepare_text(
            title=article.get("title"),
            summary=article.get("summary"),
            article_text=article.get("text"),
            max_article_words=max_article_words,
        )
        for article in cleaned_articles
    ]

    # Compute embeddings
    embeddings = compute_embeddings_batch(prepared_texts, model_key=model_key, batch_size=batch_size)

    # Build embedded articles
    results = []
    for i, article in enumerate(cleaned_articles):
        results.append(EmbeddedArticle(
            id=article["id"],
            source=article["source"],
            title=article.get("title", ""),
            summary=article.get("summary", ""),
            url=article["url"],
            published_at=_parse_datetime(article.get("published_at")),
            ingested_at=_parse_datetime(article.get("ingested_at")),
            text=article.get("text"),
            embedded_text=prepared_texts[i],
            embedding=embeddings[i],
            embedding_model=model_key,
        ))

    logger.info(f"Embedded {len(results)} articles")
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
    parser.add_argument("--model", default=DEFAULT_MODEL, choices=["minilm", "mpnet"])
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    args = parser.parse_args()

    period = args.period or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    period_dt = datetime.strptime(period, "%Y-%m-%d")

    bucket = os.environ["S3_BUCKET_NAME"]
    now = datetime.now(timezone.utc)

    # Read input articles from S3 (cleaned_articles)
    input_prefix = build_s3_key("cleaned_articles", period_dt, "")[:-1]
    input_files = list_s3_jsonl_files(bucket, input_prefix)

    if not input_files:
        logger.warning(f"No input files found for {period}")
        return

    logger.info(f"Found {len(input_files)} input files for {period}")

    cleaned_articles = []
    for key in input_files:
        logger.info(f"Reading {key}")
        cleaned_articles.extend(read_jsonl_from_s3(bucket, key))

    if not cleaned_articles:
        logger.warning("No articles found in input files")
        return

    logger.info(f"Loaded {len(cleaned_articles)} articles")

    # Embed articles
    embedded = embed_articles(cleaned_articles, model_key=args.model, batch_size=args.batch_size)

    if not embedded:
        logger.warning("No articles embedded")
        return

    # Upload as JSONL to S3
    timestamp = now.strftime('%Y_%m_%d_%H_%M')
    output_key = build_s3_key("embedded_articles", period_dt, f"embedded_articles_{timestamp}.jsonl")

    records = [serialize_dataclass(article) for article in embedded]
    upload_jsonl_to_s3(records, bucket, output_key)

    logger.info(f"Uploaded {len(embedded)} embedded articles to s3://{bucket}/{output_key}")


if __name__ == "__main__":
    main()
