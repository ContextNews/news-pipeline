"""CLI for generating stories from article clusters."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from dotenv import load_dotenv
from context_db.connection import get_session

from generate_stories.generate_stories import process_clusters
from generate_stories.helpers import parse_generate_stories_args, upload_stories_to_object_store
from common.db_io import load_clusters, load_story_inputs, upload_stories
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

    inputs = load_story_inputs(clusters)
    now = datetime.now(timezone.utc)
    stories = process_clusters(
        clusters,
        inputs["article_locations"],
        inputs["article_persons"],
        inputs["article_topics"],
        article_organizations=inputs["article_organizations"],
        article_states=inputs["article_states"],
        location_types=inputs["location_types"],
        location_country_codes=inputs["location_country_codes"],
        country_to_state=inputs["country_to_state"],
        model=args.model,
        generated_at=now,
    )

    if not stories:
        logger.warning("No stories generated")
        return

    if args.load_object_store:
        upload_stories_to_object_store(stories, now)

    if args.load_local:
        save_jsonl_local(stories, "generated_stories", now)

    if args.load_db:
        with get_session() as session:
            upload_stories(stories, session, args.cluster_period, args.overwrite)


if __name__ == "__main__":
    main()
