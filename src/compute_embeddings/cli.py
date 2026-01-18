"""CLI for computing embeddings."""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from compute_embeddings.compute_embeddings import compute_embeddings
from news_pipeline.utils.aws import (
    build_s3_key,
    list_s3_jsonl_files,
    read_jsonl_from_s3,
    upload_jsonl_to_s3,
)
from news_pipeline.utils.serialization import serialize_dataclass

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_MODEL = "all-MiniLM-L6-v2"


def _load_articles_from_s3(bucket: str, prefix: str) -> list[dict]:
    """Load articles from S3 JSONL files."""
    files = list_s3_jsonl_files(bucket, prefix)
    if not files:
        logger.warning("No JSONL files found in s3://%s/%s", bucket, prefix)
        return []

    articles = []
    for key in files:
        logger.info("Loading articles from s3://%s/%s", bucket, key)
        for article in read_jsonl_from_s3(bucket, key):
            articles.append(article)

    logger.info("Loaded %d articles from S3", len(articles))
    return articles


def _load_articles_from_local(path: str) -> list[dict]:
    """Load articles from local JSONL file."""
    filepath = Path(path)
    if not filepath.exists():
        logger.error("File not found: %s", path)
        return []

    articles = []
    with filepath.open() as f:
        for line in f:
            line = line.strip()
            if line:
                articles.append(json.loads(line))

    logger.info("Loaded %d articles from %s", len(articles), path)
    return articles


def main() -> None:
    parser = argparse.ArgumentParser()

    # Input options
    parser.add_argument("--input-s3-prefix", help="S3 prefix to load articles from")
    parser.add_argument("--input-local", help="Local JSONL file to load articles from")

    # Model options
    parser.add_argument("--model", default=DEFAULT_MODEL, help=f"Sentence transformer model (default: {DEFAULT_MODEL})")
    parser.add_argument("--batch-size", type=int, default=32, help="Batch size for encoding (default: 32)")
    parser.add_argument("--word-limit", type=int, default=None, help="Max words to embed (default: no limit)")

    # Field options
    parser.add_argument("--no-title", action="store_true", help="Exclude title from embedding")
    parser.add_argument("--no-summary", action="store_true", help="Exclude summary from embedding")
    parser.add_argument("--no-text", action="store_true", help="Exclude text from embedding")

    # Output options
    parser.add_argument("--load-s3", action="store_true", help="Upload results to S3")
    parser.add_argument("--load-local", action="store_true", help="Save results to local file")

    args = parser.parse_args()

    # Load articles
    if args.input_s3_prefix:
        bucket = os.environ["S3_BUCKET_NAME"]
        articles = _load_articles_from_s3(bucket, args.input_s3_prefix)
    elif args.input_local:
        articles = _load_articles_from_local(args.input_local)
    else:
        logger.error("Must specify --input-s3-prefix or --input-local")
        return

    if not articles:
        logger.warning("No articles to process")
        return

    # Compute embeddings
    embedded_articles = compute_embeddings(
        articles=articles,
        model=args.model,
        batch_size=args.batch_size,
        embed_title=not args.no_title,
        embed_summary=not args.no_summary,
        embed_text=not args.no_text,
        word_limit=args.word_limit,
    )

    if not embedded_articles:
        logger.warning("No articles embedded")
        return

    now = datetime.now(timezone.utc)

    if args.load_s3:
        bucket = os.environ["S3_BUCKET_NAME"]
        key = build_s3_key(
            "embedded_articles",
            now,
            f"embedded_articles_{now.strftime('%Y_%m_%d_%H_%M')}.jsonl",
        )
        records = [serialize_dataclass(article) for article in embedded_articles]
        upload_jsonl_to_s3(records, bucket, key)
        logger.info("Uploaded %d embedded articles to s3://%s/%s", len(embedded_articles), bucket, key)

    if args.load_local:
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        filename = f"embedded_articles_{now.strftime('%Y_%m_%d_%H_%M')}.jsonl"
        filepath = output_dir / filename
        records = [serialize_dataclass(article) for article in embedded_articles]
        with filepath.open("w") as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        logger.info("Saved %d embedded articles to %s", len(embedded_articles), filepath)


if __name__ == "__main__":
    main()
