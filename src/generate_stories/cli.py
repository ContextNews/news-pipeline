"""CLI for generating stories from article clusters."""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from dotenv import load_dotenv

from common.aws import build_s3_key, upload_jsonl_to_s3
from generate_stories.generate_stories import generate_story, GeneratedStoryOverview

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "gpt-4o-mini"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def _parse_clustered_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("clustered-date must be YYYY-MM-DD") from exc


def _load_clusters_from_rds(clustered_date: date) -> list[dict[str, Any]]:
    """Load article clusters and their articles from RDS for a specific clustered date (UTC)."""
    from sqlalchemy import text

    from rds_postgres.connection import get_session

    start = datetime.combine(clustered_date, datetime.min.time())
    end = start + timedelta(days=1)

    logger.info("Loading clusters from %s to %s", start.isoformat(), end.isoformat())

    with get_session() as session:
        # First, get all clusters for the date
        clusters_stmt = text(
            """
            SELECT article_cluster_id, clustered_at
            FROM article_clusters
            WHERE clustered_at >= :start
              AND clustered_at < :end
            """
        )
        cluster_results = session.execute(
            clusters_stmt,
            {"start": start, "end": end},
        ).mappings().all()

        if not cluster_results:
            return []

        cluster_ids = [row["article_cluster_id"] for row in cluster_results]

        # Then, get all articles for those clusters
        articles_stmt = text(
            """
            SELECT
                aca.article_cluster_id,
                a.id,
                a.source,
                a.title,
                a.summary,
                a.url,
                a.published_at,
                a.text
            FROM article_cluster_articles aca
            JOIN articles a ON a.id = aca.article_id
            WHERE aca.article_cluster_id = ANY(:cluster_ids)
            """
        )
        article_results = session.execute(
            articles_stmt,
            {"cluster_ids": cluster_ids},
        ).mappings().all()

    # Group articles by cluster
    clusters_map: dict[str, list[dict[str, Any]]] = {}
    for row in article_results:
        cluster_id = row["article_cluster_id"]
        article = {
            "id": row["id"],
            "source": row["source"],
            "title": row["title"],
            "summary": row["summary"],
            "url": row["url"],
            "published_at": row["published_at"],
            "text": row["text"],
        }
        clusters_map.setdefault(cluster_id, []).append(article)

    # Build cluster list with metadata
    clusters = []
    for cluster_row in cluster_results:
        cluster_id = cluster_row["article_cluster_id"]
        if cluster_id in clusters_map:
            clusters.append({
                "cluster_id": cluster_id,
                "clustered_at": cluster_row["clustered_at"],
                "articles": clusters_map[cluster_id],
            })

    logger.info("Loaded %d clusters with %d total articles", len(clusters), len(article_results))
    return clusters


def _build_story_record(
    cluster_id: str,
    article_ids: list[str],
    story: GeneratedStoryOverview,
    generated_at: datetime,
) -> dict[str, Any]:
    """Build a record for a generated story."""
    return {
        "story_id": uuid4().hex,
        "cluster_id": cluster_id,
        "article_ids": article_ids,
        "title": story.title,
        "summary": story.summary,
        "key_points": story.key_points,
        "generated_at": generated_at.isoformat(),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--clustered-date",
        type=_parse_clustered_date,
        default=datetime.now(timezone.utc).date(),
        help="UTC date (YYYY-MM-DD) of clusters to process",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"OpenAI model to use (default: {DEFAULT_MODEL})",
    )
    parser.add_argument("--load-s3", action="store_true", help="Upload results to S3")
    parser.add_argument("--load-rds", action="store_true", help="Save stories to RDS")
    parser.add_argument("--load-local", action="store_true", help="Save results to local file")
    args = parser.parse_args()

    load_dotenv()

    clusters = _load_clusters_from_rds(args.clustered_date)
    if not clusters:
        logger.warning("No clusters found for date %s", args.clustered_date)
        return

    # Generate stories for each cluster
    stories = []
    now = datetime.now(timezone.utc)

    for cluster in clusters:
        cluster_id = cluster["cluster_id"]
        articles = cluster["articles"]

        logger.info("Generating story for cluster %s with %d articles", cluster_id, len(articles))
        try:
            story = generate_story(articles, model=args.model)
            article_ids = [article["id"] for article in articles]
            record = _build_story_record(cluster_id, article_ids, story, now)
            stories.append(record)
            logger.info("Generated story: %s", story.title)
        except Exception as e:
            logger.error("Failed to generate story for cluster %s: %s", cluster_id, e)
            continue

    if not stories:
        logger.warning("No stories generated")
        return

    logger.info("Generated %d stories", len(stories))

    if args.load_s3:
        bucket = os.environ["S3_BUCKET_NAME"]
        key = build_s3_key(
            "generated_stories",
            now,
            f"generated_stories_{now.strftime('%Y_%m_%d_%H_%M')}.jsonl",
        )
        upload_jsonl_to_s3(stories, bucket, key)
        logger.info("Uploaded %d stories to s3://%s/%s", len(stories), bucket, key)

    if args.load_rds:
        from sqlalchemy import text

        from rds_postgres.connection import get_session

        with get_session() as session:
            # Insert stories
            session.execute(
                text(
                    """
                    INSERT INTO stories (id, title, summary, key_points, generated_at, updated_at)
                    VALUES (:id, :title, :summary, :key_points, :generated_at, :updated_at)
                    """
                ),
                [
                    {
                        "id": story["story_id"],
                        "title": story["title"],
                        "summary": story["summary"],
                        "key_points": story["key_points"],
                        "generated_at": now,
                        "updated_at": now,
                    }
                    for story in stories
                ],
            )

            # Insert article_stories links
            article_story_rows = []
            for story in stories:
                for article_id in story["article_ids"]:
                    article_story_rows.append({
                        "article_id": article_id,
                        "story_id": story["story_id"],
                    })

            if article_story_rows:
                session.execute(
                    text(
                        """
                        INSERT INTO article_stories (article_id, story_id)
                        VALUES (:article_id, :story_id)
                        """
                    ),
                    article_story_rows,
                )

            session.commit()

        logger.info("Saved %d stories to RDS", len(stories))

    if args.load_local:
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        filename = f"generated_stories_{now.strftime('%Y_%m_%d_%H_%M')}.jsonl"
        filepath = output_dir / filename
        with filepath.open("w") as f:
            for record in stories:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        logger.info("Saved %d stories to %s", len(stories), filepath)


if __name__ == "__main__":
    main()
