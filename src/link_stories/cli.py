"""CLI for linking stories between two dates."""

from __future__ import annotations

import logging

from dotenv import load_dotenv
from rds_postgres.connection import get_session

from common.cli_helpers import setup_logging
from link_stories.helpers import parse_link_stories_args
from link_stories.link import (
    delete_story_links,
    link_stories,
    load_stories_for_date,
    save_story_links,
)

load_dotenv()

setup_logging()
logger = logging.getLogger(__name__)


def main() -> None:
    args = parse_link_stories_args()

    if args.date_b < args.date_a:
        raise ValueError("date-b must be on or after date-a")

    if not args.load_rds:
        logger.warning("link_stories requires --load-rds to save or delete links")
        return

    date_a_stories = load_stories_for_date(args.date_a)
    if not date_a_stories:
        logger.warning("No stories found for date-a %s", args.date_a)
        return

    date_b_stories = load_stories_for_date(args.date_b)
    if not date_b_stories:
        logger.warning("No stories found for date-b %s", args.date_b)
        return

    logger.info(
        "Linking %d stories from %s against %d stories from %s",
        len(date_b_stories),
        args.date_b,
        len(date_a_stories),
        args.date_a,
    )

    with get_session() as session:
        if args.delete_existing:
            delete_story_links(args.date_a, args.date_b, session)

        links = link_stories(
            date_b_stories,
            args.date_a,
            model=args.model,
            n_candidates=args.n_candidates,
        )

        if links:
            save_story_links(links, session)

    logger.info(
        "Story linking complete: %d links between %s and %s",
        len(links),
        args.date_a,
        args.date_b,
    )


if __name__ == "__main__":
    main()
