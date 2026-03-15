"""CLI for generating stories from article clusters."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from context_db.connection import get_session

from generate_stories.generate_stories import process_clusters
from generate_stories.helpers import parse_generate_stories_args
from common.aws import (
    load_clusters,
    load_article_locations,
    load_article_persons,
    load_article_topics,
    upload_stories,
    upload_jsonl_to_s3,
    build_s3_key,
)
from common.cli_helpers import setup_logging, save_jsonl_local

load_dotenv()

setup_logging()
logger = logging.getLogger(__name__)


def main() -> None:
    args = parse_generate_stories_args()

    clusters = load_clusters(args.cluster_period)
    if not clusters:
        logger.warning("No clusters found for date %s", args.cluster_period)
        return

    all_article_ids = [
        article["id"]
        for cluster in clusters
        for article in cluster["articles"]
    ]
    article_locations = load_article_locations(all_article_ids)
    article_persons = load_article_persons(all_article_ids)
    article_topics = load_article_topics(all_article_ids)

    now = datetime.now(timezone.utc)
    stories = process_clusters(
        clusters,
        article_locations,
        article_persons,
        article_topics,
        model=args.model,
        generated_at=now,
    )

    if not stories:
        logger.warning("No stories generated")
        return

    if args.load_s3:
        bucket_key = build_s3_key(
            "generated_stories",
            now,
            f"generated_stories_{now.strftime('%Y_%m_%d_%H_%M')}.jsonl",
        )
        upload_jsonl_to_s3(stories, os.environ["S3_BUCKET_NAME"], bucket_key)

    if args.load_local:
        save_jsonl_local(stories, "generated_stories", now)

    if args.load_rds:
        with get_session() as session:
            upload_stories(stories, session, args.cluster_period, args.overwrite)


if __name__ == "__main__":
    main()
