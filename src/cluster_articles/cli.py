"""CLI for clustering articles."""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from dotenv import load_dotenv

from cluster_articles.cluster_articles import cluster_articles
from news_pipeline.utils.aws import build_s3_key, upload_jsonl_to_s3

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "all-MiniLM-L6-v2"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def _parse_ingested_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("ingested-date must be YYYY-MM-DD") from exc


def _load_articles_with_embeddings(
    ingested_date: date,
    embedding_model: str,
) -> list[dict[str, object]]:
    """Load articles and embeddings from RDS for a specific ingested date (UTC)."""
    from sqlalchemy import text

    from rds_postgres.connection import get_session

    start = datetime.combine(ingested_date, datetime.min.time())
    end = start + timedelta(days=1)

    logger.info("Loading articles ingested from %s to %s", start.isoformat(), end.isoformat())
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
                a.text,
                e.embedding,
                e.embedding_model
            FROM articles a
            JOIN article_embeddings e ON e.article_id = a.id
            WHERE a.ingested_at >= :start
              AND a.ingested_at < :end
              AND e.embedding_model = :model
            """
        )
        results = session.execute(
            stmt,
            {"start": start, "end": end, "model": embedding_model},
        ).mappings().all()
        articles = [dict(row) for row in results]

    logger.info("Loaded %d articles with embeddings", len(articles))
    return articles


def _build_cluster_records(records: list[dict[str, object]]) -> list[dict[str, object]]:
    output = []
    for record in records:
        published_at = record.get("published_at")
        ingested_at = record.get("ingested_at")
        if isinstance(published_at, datetime):
            published_at = published_at.isoformat()
        if isinstance(ingested_at, datetime):
            ingested_at = ingested_at.isoformat()
        output.append(
            {
                "id": record.get("id"),
                "source": record.get("source"),
                "title": record.get("title"),
                "summary": record.get("summary"),
                "url": record.get("url"),
                "published_at": published_at,
                "ingested_at": ingested_at,
                "text": record.get("text"),
                "cluster_id": record.get("cluster_id"),
                "embedding_model": record.get("embedding_model"),
            }
        )
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ingested-date",
        type=_parse_ingested_date,
        default=datetime.now(timezone.utc).date(),
        help="UTC date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--embedding-model",
        default=DEFAULT_MODEL,
        help=f"Embedding model to use (default: {DEFAULT_MODEL})",
    )
    parser.add_argument("--min-cluster-size", type=int, default=5)
    parser.add_argument("--min-samples", type=int, default=None)
    parser.add_argument("--load-s3", action="store_true", help="Upload results to S3")
    parser.add_argument("--load-rds", action="store_true", help="Save clusters to RDS")
    parser.add_argument("--load-local", action="store_true", help="Save results to local file")
    parser.add_argument(
        "--overwrite-clusters",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Overwrite existing clusters for the ingested date",
    )
    args = parser.parse_args()

    load_dotenv()

    articles = _load_articles_with_embeddings(args.ingested_date, args.embedding_model)
    if not articles:
        logger.warning("No articles to cluster")
        return

    clustered = cluster_articles(
        articles,
        min_cluster_size=args.min_cluster_size,
        min_samples=args.min_samples,
    )
    if not clustered:
        logger.warning("No clusters produced")
        return

    records = _build_cluster_records(clustered)
    now = datetime.now(timezone.utc)

    if args.load_s3:
        bucket = os.environ["S3_BUCKET_NAME"]
        key = build_s3_key(
            "clustered_articles",
            now,
            f"clustered_articles_{now.strftime('%Y_%m_%d_%H_%M')}.jsonl",
        )
        upload_jsonl_to_s3(records, bucket, key)
        logger.info("Uploaded %d clustered articles to s3://%s/%s", len(records), bucket, key)

    if args.load_rds:
        from sqlalchemy import text

        from rds_postgres.connection import get_session

        clusters = {}
        for record in records:
            label = record.get("cluster_id")
            if label is None or label == -1:
                continue
            clusters.setdefault(label, []).append(record["id"])

        if not clusters:
            logger.warning("No non-noise clusters to save")
        else:
            clustered_at = datetime.now(timezone.utc)
            with get_session() as session:
                if args.overwrite_clusters:
                    delete_stmt = text(
                        """
                        DELETE FROM article_cluster_articles aca
                        USING article_clusters ac
                        WHERE aca.article_cluster_id = ac.article_cluster_id
                          AND ac.clustered_at >= :start
                          AND ac.clustered_at < :end
                        """
                    )
                    session.execute(
                        delete_stmt,
                        {
                            "start": datetime.combine(args.ingested_date, datetime.min.time()),
                            "end": datetime.combine(args.ingested_date, datetime.min.time()) + timedelta(days=1),
                        },
                    )
                    session.execute(
                        text(
                            """
                            DELETE FROM article_clusters
                            WHERE clustered_at >= :start
                              AND clustered_at < :end
                            """
                        ),
                        {
                            "start": datetime.combine(args.ingested_date, datetime.min.time()),
                            "end": datetime.combine(args.ingested_date, datetime.min.time()) + timedelta(days=1),
                        },
                    )
                for _, article_ids in clusters.items():
                    cluster_id = uuid4().hex
                    session.execute(
                        text(
                            """
                            INSERT INTO article_clusters (article_cluster_id, clustered_at)
                            VALUES (:cluster_id, :clustered_at)
                            """
                        ),
                        {"cluster_id": cluster_id, "clustered_at": clustered_at},
                    )
                    session.execute(
                        text(
                            """
                            INSERT INTO article_cluster_articles (article_cluster_id, article_id)
                            VALUES (:cluster_id, :article_id)
                            """
                        ),
                        [
                            {"cluster_id": cluster_id, "article_id": article_id}
                            for article_id in article_ids
                        ],
                    )
                session.commit()
            logger.info("Saved %d clusters to RDS", len(clusters))

    if args.load_local:
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        filename = f"clustered_articles_{now.strftime('%Y_%m_%d_%H_%M')}.jsonl"
        filepath = output_dir / filename
        with filepath.open("w") as f:
            for record in records:
                f.write(json.dumps(record, default=str, ensure_ascii=False) + "\n")
        logger.info("Saved %d clustered articles to %s", len(records), filepath)


if __name__ == "__main__":
    main()
