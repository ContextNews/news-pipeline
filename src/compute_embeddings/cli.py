"""CLI for computing embeddings."""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date, datetime, timezone

from dotenv import load_dotenv

from compute_embeddings.compute_embeddings import compute_embeddings
from common.aws import build_s3_key, upload_jsonl_to_s3
from common.cli_helpers import date_to_range, parse_date, save_jsonl_local, setup_logging
from common.serialization import serialize_dataclass

load_dotenv()

setup_logging()
logger = logging.getLogger(__name__)

DEFAULT_MODEL = "all-MiniLM-L6-v2"


def _load_articles_from_rds(published_date: date, model: str, overwrite: bool) -> list[dict]:
    """Load articles from RDS for a specific published date (UTC)."""
    from sqlalchemy import text

    from rds_postgres.connection import get_session

    start, end = date_to_range(published_date)

    logger.info("Loading articles published from %s to %s", start.isoformat(), end.isoformat())
    with get_session() as session:
        stmt = text(
            """
            SELECT
                a.id,
                a.source,
                a.title,
                a.summary,
                a.url,
                a.published_at,
                a.ingested_at,
                a.text
            FROM articles a
            WHERE a.published_at >= :start
              AND a.published_at < :end
              AND (:overwrite OR NOT EXISTS (
                    SELECT 1
                    FROM article_embeddings e
                    WHERE e.article_id = a.id
                      AND e.embedding_model = :model
                ))
            """
        )
        results = session.execute(
            stmt,
            {"start": start, "end": end, "overwrite": overwrite, "model": model},
        ).mappings().all()
        articles = [dict(row) for row in results]

    logger.info("Loaded %d articles from RDS", len(articles))
    return articles


def main() -> None:
    parser = argparse.ArgumentParser()

    # Input options
    parser.add_argument(
        "--published-date",
        type=lambda v: parse_date(v, "published-date"),
        default=datetime.now(timezone.utc).date(),
        help="Embed articles published on this date (UTC, YYYY-MM-DD)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-embed even if the article already has an embedding for this model",
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
    parser.add_argument("--load-rds", action="store_true", help="Load embeddings into RDS")
    parser.add_argument("--load-local", action="store_true", help="Save results to local file")

    args = parser.parse_args()

    load_dotenv()
    articles = _load_articles_from_rds(args.published_date, args.model, args.overwrite)

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

    if args.load_rds:
        from sqlalchemy import text

        from rds_postgres.connection import get_session

        updated = 0
        inserted = 0
        with get_session() as session:
            for article in embedded_articles:
                params = {
                    "article_id": article.id,
                    "embedded_text": article.embedded_text,
                    "embedding": article.embedding,
                    "embedding_model": article.embedding_model,
                }
                update_stmt = text(
                    """
                    UPDATE article_embeddings
                    SET embedded_text = :embedded_text,
                        embedding = :embedding
                    WHERE article_id = :article_id
                      AND embedding_model = :embedding_model
                    """
                )
                result = session.execute(update_stmt, params)
                if result.rowcount and result.rowcount > 0:
                    updated += 1
                    continue
                insert_stmt = text(
                    """
                    INSERT INTO article_embeddings
                        (article_id, embedded_text, embedding, embedding_model)
                    VALUES
                        (:article_id, :embedded_text, :embedding, :embedding_model)
                    """
                )
                session.execute(insert_stmt, params)
                inserted += 1
            session.commit()
        logger.info("Upserted %d embeddings in RDS (%d updated, %d inserted)", updated + inserted, updated, inserted)

    if args.load_local:
        records = [serialize_dataclass(article) for article in embedded_articles]
        filepath = save_jsonl_local(records, "embedded_articles", now)
        logger.info("Saved %d embedded articles to %s", len(embedded_articles), filepath)


if __name__ == "__main__":
    main()
