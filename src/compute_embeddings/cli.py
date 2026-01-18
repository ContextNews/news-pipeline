"""CLI for computing embeddings."""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

from compute_embeddings.compute_embeddings import compute_embeddings
from news_pipeline.utils.aws import (
    build_s3_key,
    upload_jsonl_to_s3,
)
from news_pipeline.utils.serialization import serialize_dataclass

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_MODEL = "all-MiniLM-L6-v2"


def _article_to_dict(article: object) -> dict[str, object]:
    return {
        "id": article.id,
        "source": article.source,
        "title": article.title,
        "summary": article.summary,
        "url": article.url,
        "published_at": article.published_at,
        "ingested_at": article.ingested_at,
        "text": article.text,
    }


def _load_articles_from_rds(ingested_date: date) -> list[dict]:
    """Load articles from RDS for a specific ingested date (UTC)."""
    from sqlalchemy import select

    from rds_postgres.connection import get_session
    from rds_postgres.models import Article

    start = datetime.combine(ingested_date, datetime.min.time(), tzinfo=timezone.utc)
    end = start + timedelta(days=1)

    logger.info("Loading articles ingested from %s to %s", start.isoformat(), end.isoformat())
    with get_session() as session:
        stmt = select(Article).where(
            Article.ingested_at >= start,
            Article.ingested_at < end,
        )
        results = session.execute(stmt).scalars().all()
        articles = [_article_to_dict(article) for article in results]

    logger.info("Loaded %d articles from RDS", len(articles))
    return articles


def _parse_ingested_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("ingested-date must be YYYY-MM-DD") from exc


def main() -> None:
    parser = argparse.ArgumentParser()

    # Input options
    parser.add_argument(
        "--ingested-date",
        type=_parse_ingested_date,
        default=date.today(),
        help="UTC date (YYYY-MM-DD)",
    )

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

    load_dotenv()
    articles = _load_articles_from_rds(args.ingested_date)

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
