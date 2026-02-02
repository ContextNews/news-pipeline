"""CLI for classifying stories by topic."""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date, datetime, timezone
from typing import Any

from dotenv import load_dotenv

from common.aws import build_s3_key, upload_jsonl_to_s3
from common.cli_helpers import date_to_range, parse_date, save_jsonl_local, setup_logging
from classify_stories.classify_stories import classify_stories, ClassifiedStory

setup_logging()
logger = logging.getLogger(__name__)

DEFAULT_MODEL = "gpt-4o-mini"


def _load_stories_from_rds(
    story_period: date,
    overwrite: bool = False,
) -> list[dict[str, Any]]:
    """Load stories from RDS for a specific story period (UTC).

    Args:
        story_period: The date to filter stories by story_period.
        overwrite: If False, only load stories that don't have topics yet.

    Returns:
        List of story dicts with id, title, summary, key_points.
    """
    from sqlalchemy import text

    from rds_postgres.connection import get_session

    start, end = date_to_range(story_period)

    logger.info("Loading stories from %s to %s", start.isoformat(), end.isoformat())

    with get_session() as session:
        if overwrite:
            stmt = text(
                """
                SELECT id, title, summary, key_points
                FROM stories
                WHERE story_period >= :start
                  AND story_period < :end
                """
            )
        else:
            stmt = text(
                """
                SELECT id, title, summary, key_points
                FROM stories
                WHERE story_period >= :start
                  AND story_period < :end
                  AND NOT EXISTS (
                      SELECT 1 FROM story_topics
                      WHERE story_topics.story_id = stories.id
                  )
                """
            )

        results = session.execute(stmt, {"start": start, "end": end}).mappings().all()

    stories = [dict(row) for row in results]
    logger.info("Loaded %d stories", len(stories))
    return stories


def _build_classification_records(
    classified_stories: list[ClassifiedStory],
    classified_at: datetime,
) -> list[dict[str, Any]]:
    """Build records for classified stories."""
    records = []
    for cs in classified_stories:
        records.append({
            "story_id": cs.story_id,
            "topics": cs.topics,
            "classified_at": classified_at.isoformat(),
        })
    return records


def main() -> None:
    parser = argparse.ArgumentParser(description="Classify stories by topic using Cronkite")
    parser.add_argument(
        "--story-period",
        type=lambda v: parse_date(v, "story-period"),
        default=datetime.now(timezone.utc).date(),
        help="UTC date (YYYY-MM-DD) of story period to process",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"OpenAI model to use (default: {DEFAULT_MODEL})",
    )
    parser.add_argument("--load-s3", action="store_true", help="Upload results to S3")
    parser.add_argument("--load-rds", action="store_true", help="Save topics to RDS")
    parser.add_argument("--load-local", action="store_true", help="Save results to local file")
    parser.add_argument(
        "--overwrite",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Re-classify stories that already have topics",
    )
    args = parser.parse_args()

    load_dotenv()

    stories = _load_stories_from_rds(args.story_period, overwrite=args.overwrite)
    if not stories:
        logger.warning("No stories found for date %s", args.story_period)
        return

    logger.info("Classifying %d stories", len(stories))
    try:
        classified = classify_stories(stories, model=args.model)
    except Exception as e:
        logger.error("Failed to classify stories: %s", e)
        return

    if not classified:
        logger.warning("No classifications produced")
        return

    logger.info("Classified %d stories", len(classified))

    now = datetime.now(timezone.utc)
    records = _build_classification_records(classified, now)

    if args.load_s3:
        bucket = os.environ["S3_BUCKET_NAME"]
        key = build_s3_key(
            "classified_stories",
            now,
            f"classified_stories_{now.strftime('%Y_%m_%d_%H_%M')}.jsonl",
        )
        upload_jsonl_to_s3(records, bucket, key)
        logger.info("Uploaded %d classifications to s3://%s/%s", len(records), bucket, key)

    if args.load_rds:
        from sqlalchemy import text

        from rds_postgres.connection import get_session

        with get_session() as session:
            if args.overwrite:
                # Get story IDs we're about to insert
                story_ids = [cs.story_id for cs in classified]

                # Delete existing topics for these stories
                session.execute(
                    text(
                        """
                        DELETE FROM story_topics
                        WHERE story_id = ANY(:story_ids)
                        """
                    ),
                    {"story_ids": story_ids},
                )
                logger.info("Deleted existing topics for %d stories", len(story_ids))

            # Build rows for story_topics (one row per story-topic pair)
            topic_rows = []
            for cs in classified:
                for topic in cs.topics:
                    topic_rows.append({
                        "story_id": cs.story_id,
                        "topic": topic,
                    })

            if topic_rows:
                session.execute(
                    text(
                        """
                        INSERT INTO story_topics (story_id, topic)
                        VALUES (:story_id, :topic)
                        ON CONFLICT DO NOTHING
                        """
                    ),
                    topic_rows,
                )

            session.commit()

        logger.info("Saved %d topic classifications to RDS", len(topic_rows))

    if args.load_local:
        filepath = save_jsonl_local(records, "classified_stories", now)
        logger.info("Saved %d classifications to %s", len(records), filepath)


if __name__ == "__main__":
    main()
