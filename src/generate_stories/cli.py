"""CLI for generating stories from article clusters."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from rds_postgres.connection import get_session

from generate_stories.generate_stories import generate_story
from generate_stories.helpers import parse_generate_stories_args, build_story_record
from common.aws import load_clusters, load_article_locations, load_article_persons, upload_stories, upload_jsonl_to_s3, build_s3_key
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

    # Get all article IDs from clusters and load their locations
    all_article_ids = [
        article["id"]
        for cluster in clusters
        for article in cluster["articles"]
    ]
    article_locations = load_article_locations(all_article_ids)
    article_persons = load_article_persons(all_article_ids)

    # Generate stories for each cluster
    stories = []
    now = datetime.now(timezone.utc)

    for i, cluster in enumerate(clusters, 1):
        cluster_id = cluster["cluster_id"]
        articles = cluster["articles"]

        logger.info(
            "--- Cluster %d/%d [%s] — %d articles ---",
            i, len(clusters), cluster_id, len(articles),
        )
        for article in articles:
            article_id = article.get("id", "?")
            title = article.get("title", "untitled")
            source = article.get("source_name", article.get("source", "unknown"))
            published = article.get("published_at", "?")
            locations = article_locations.get(article_id, [])
            persons = article_persons.get(article_id, [])
            logger.info(
                "  Article %s | %s | source=%s | published=%s | locations=%s | persons=%s",
                article_id, title, source, published, locations, persons,
            )

        try:
            story = generate_story(articles, model=args.model, article_locations=article_locations, article_persons=article_persons)
            article_ids = story.article_ids or [article["id"] for article in articles]
            record = build_story_record(cluster_id, article_ids, story, cluster["cluster_period"], now)
            stories.append(record)

            logger.info("  Story title:    %s", story.title)
            logger.info("  Story summary:  %s", story.summary[:200] if story.summary else "")
            logger.info("  Key points:     %s", story.key_points)
            logger.info("  Article IDs:    %d kept, %d noise", len(article_ids), len(story.noise_article_ids))
            articles_by_id = {a["id"]: a for a in articles}
            for aid in article_ids:
                logger.info("    kept:  %s — %s", aid, articles_by_id.get(aid, {}).get("title", "untitled"))
            for aid in story.noise_article_ids:
                logger.info("    noise: %s — %s", aid, articles_by_id.get(aid, {}).get("title", "untitled"))
            logger.info("  Location QID:   %s", story.location_qid)
            logger.info("  Person QIDs:    %s", story.person_qids)
        except Exception as e:
            logger.error("Failed to generate story for cluster %s: %s", cluster_id, e)
            continue

    if not stories:
        logger.warning("No stories generated")
        return

    logger.info("Generated %d stories", len(stories))

    # Classify stories by topic
    if args.classify:
        from generate_stories.classify_stories import classify_stories

        cronkite_stories = [
            {"id": s["story_id"], "title": s["title"], "summary": s["summary"]}
            for s in stories
        ]
        try:
            classified = classify_stories(cronkite_stories, model=args.model)
            topics_by_id = {cs.story_id: cs.topics for cs in classified}
            for story in stories:
                story["topics"] = topics_by_id.get(story["story_id"], [])
            logger.info("Classified %d of %d stories", len(classified), len(stories))
        except Exception as e:
            logger.error("Failed to classify stories: %s", e)

    if args.load_s3:
        bucket_key = build_s3_key(
            "generated_stories",
            now,
            f"generated_stories_{now.strftime('%Y_%m_%d_%H_%M')}.jsonl",
        )
        upload_jsonl_to_s3(stories, os.environ["S3_BUCKET_NAME"], bucket_key)
        logger.info("Uploaded %d stories to S3", len(stories))

    if args.load_local:
        save_jsonl_local(stories, "generated_stories", now)

    if args.load_rds:
        with get_session() as session:
            upload_stories(stories, session, args.cluster_period, args.overwrite)

    # Link stories to related stories from the previous day
    # Must run after upload_stories so today's stories are in RDS
    # for get_similar_stories to look up their metadata
    story_links = []
    if args.link_stories:
        from generate_stories.link_stories import link_stories

        if not args.load_rds:
            logger.warning("Story linking requires --load-rds to work (stories must be in DB first)")
        else:
            previous_date = args.cluster_period - timedelta(days=1)
            logger.info(
                "Starting story linking: %d stories from %s against previous date %s",
                len(stories), args.cluster_period, previous_date,
            )
            try:
                story_links = link_stories(stories, previous_date, model=args.model)
                logger.info("Found %d story links to previous day", len(story_links))
                if story_links:
                    from generate_stories.link_stories import save_story_links

                    with get_session() as session:
                        save_story_links(story_links, session)
            except Exception as e:
                logger.error("Failed to link stories: %s", e, exc_info=True)


if __name__ == "__main__":
    main()
